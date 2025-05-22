/*
 * Copyright 2017 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Andrey Kunitsyn (andrey@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.api;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.model.BaseModel;
import org.traccar.model.User;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;

/**
 * Resource for handling simple object operations with service discovery, circuit breaking,
 * distributed tracing, and metrics collection.
 */
public class SimpleObjectResource<T extends BaseModel> extends BaseObjectResource<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleObjectResource.class);
    private final String sortField;
    
    @Inject
    private ServiceDiscovery serviceDiscovery;
    
    @Inject
    private Tracer tracer;
    
    @Inject
    private MeterRegistry meterRegistry;
    
    private Timer getTimer;
    private Counter successCounter;
    private Counter errorCounter;
    private Counter circuitBreakerCounter;

    public SimpleObjectResource(Class<T> baseClass, String sortField) {
        super(baseClass);
        this.sortField = sortField;
        
        // Initialize metrics
        String metricPrefix = "api." + baseClass.getSimpleName().toLowerCase();
        this.getTimer = Timer.builder(metricPrefix + ".get.timer")
                .description("Time taken to retrieve " + baseClass.getSimpleName() + " objects")
                .register(meterRegistry);
                
        this.successCounter = Counter.builder(metricPrefix + ".success")
                .description("Number of successful " + baseClass.getSimpleName() + " retrievals")
                .register(meterRegistry);
                
        this.errorCounter = Counter.builder(metricPrefix + ".error")
                .description("Number of failed " + baseClass.getSimpleName() + " retrievals")
                .register(meterRegistry);
                
        this.circuitBreakerCounter = Counter.builder(metricPrefix + ".circuit_breaker")
                .description("Number of circuit breaker activations for " + baseClass.getSimpleName())
                .register(meterRegistry);
    }

    /**
     * Retrieves objects based on query parameters with service discovery, circuit breaking,
     * distributed tracing, and metrics collection.
     *
     * @param all Whether to retrieve all objects (admin only) or just those accessible to the user
     * @param userId Optional user ID to filter objects by permission
     * @return Collection of objects matching the criteria
     * @throws StorageException If there's an error accessing the storage
     */
    @GET
    @Timed(value = "api.request.get", extraTags = {"resource", "simpleObject"})
    @CircuitBreaker(name = "storageService", fallbackMethod = "getFallback")
    public Collection<T> get(
            @QueryParam("all") boolean all, @QueryParam("userId") long userId) throws StorageException {
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("get_" + baseClass.getSimpleName())
                .setParent(Context.current())
                .setAttribute("resource.type", baseClass.getSimpleName())
                .setAttribute("query.all", all)
                .setAttribute("query.userId", userId)
                .startSpan();
        
        LOGGER.debug("Retrieving {} objects (all={}, userId={})", baseClass.getSimpleName(), all, userId);
        
        try {
            // Record the operation with a timer
            Collection<T> result = getTimer.record(() -> {
                var conditions = new LinkedList<Condition>();

                if (all) {
                    if (permissionsService.notAdmin(getUserId())) {
                        conditions.add(new Condition.Permission(User.class, getUserId(), baseClass));
                    }
                } else {
                    if (userId == 0) {
                        userId = getUserId();
                    } else {
                        permissionsService.checkUser(getUserId(), userId);
                    }
                    conditions.add(new Condition.Permission(User.class, userId, baseClass));
                }

                // Build the request object
                Request request = new Request(
                        new Columns.All(), 
                        Condition.merge(conditions), 
                        sortField != null ? new Order(sortField) : null);

                // Use service discovery to determine if we should use a remote service
                String serviceName = baseClass.getSimpleName() + "Service";
                if (serviceDiscovery.isServiceAvailable(serviceName)) {
                    span.addEvent("Using remote service", Attributes.of(
                            io.opentelemetry.api.common.AttributeKey.stringKey("service.name"), serviceName));
                    
                    LOGGER.debug("Using remote service {} for {} objects", serviceName, baseClass.getSimpleName());
                    return serviceDiscovery.getServiceClient(serviceName).getObjects(baseClass, request);
                } else {
                    span.addEvent("Using local storage");
                    LOGGER.debug("Using local storage for {} objects", baseClass.getSimpleName());
                    return storage.getObjects(baseClass, request);
                }
            });
            
            // Record success metrics
            successCounter.increment();
            span.setStatus(StatusCode.OK);
            span.setAttribute("result.count", result.size());
            
            return result;
        } catch (Exception e) {
            // Record error metrics
            errorCounter.increment();
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            LOGGER.error("Error retrieving {} objects: {}", baseClass.getSimpleName(), e.getMessage(), e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Fallback method for circuit breaker when the primary method fails.
     * This method is called automatically by Resilience4j when the circuit is open.
     *
     * @param all Whether to retrieve all objects (admin only) or just those accessible to the user
     * @param userId Optional user ID to filter objects by permission
     * @param e The exception that triggered the fallback
     * @return Empty collection or throws an appropriate exception
     */
    public Collection<T> getFallback(boolean all, long userId, Exception e) {
        // Record circuit breaker activation
        circuitBreakerCounter.increment();
        
        // Create a span for the fallback operation
        Span span = tracer.spanBuilder("circuit_breaker_fallback_" + baseClass.getSimpleName())
                .setAttribute("resource.type", baseClass.getSimpleName())
                .setAttribute("error.type", e.getClass().getName())
                .setAttribute("error.message", e.getMessage())
                .startSpan();
        
        try {
            LOGGER.warn("Circuit breaker activated for {} objects: {}", baseClass.getSimpleName(), e.getMessage());
            
            // For certain types of exceptions, we might want to return an empty result instead of failing
            if (e instanceof StorageException) {
                span.setStatus(StatusCode.ERROR, "Storage service unavailable");
                throw new WebApplicationException("Storage service unavailable", 
                        Response.Status.SERVICE_UNAVAILABLE);
            } else {
                span.setStatus(StatusCode.ERROR, "Service unavailable");
                throw new WebApplicationException("Service unavailable", 
                        Response.Status.SERVICE_UNAVAILABLE);
            }
        } finally {
            span.end();
        }
    }
}