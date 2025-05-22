/*
 * Copyright 2017 - 2022 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Context;
import org.traccar.api.security.ServiceAccountUser;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.model.ObjectOperation;
import org.traccar.helper.LogAction;
import org.traccar.model.BaseModel;
import org.traccar.model.Group;
import org.traccar.model.Permission;
import org.traccar.model.User;
import org.traccar.session.ConnectionManager;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Response;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Base resource class for object CRUD operations.
 * Implements service discovery, circuit breaker, distributed tracing,
 * distributed caching, and metrics collection.
 */
public abstract class BaseObjectResource<T extends BaseModel> extends BaseResource {

    @Inject
    private CacheManager cacheManager;

    @Inject
    private ConnectionManager connectionManager;

    @Inject
    private LogAction actionLogger;
    
    @Inject
    private ServiceDiscovery serviceDiscovery;
    
    @Inject
    private CircuitBreakerRegistry circuitBreakerRegistry;
    
    @Inject
    private Tracer tracer;
    
    @Inject
    private MeterRegistry meterRegistry;

    @Context
    private HttpServletRequest request;

    protected final Class<T> baseClass;
    
    private final CircuitBreaker circuitBreaker;
    
    private final Timer getSingleTimer;
    private final Timer addTimer;
    private final Timer updateTimer;
    private final Timer removeTimer;

    public BaseObjectResource(Class<T> baseClass) {
        this.baseClass = baseClass;
        
        // Initialize circuit breaker for this resource
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                baseClass.getSimpleName() + "Resource",
                CircuitBreakerConfig.custom()
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(10))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .slidingWindowSize(10)
                        .build());
        
        // Initialize metrics timers
        this.getSingleTimer = Timer.builder("api.object.get")
                .tag("class", baseClass.getSimpleName())
                .description("Time taken to get a single object")
                .register(meterRegistry);
        
        this.addTimer = Timer.builder("api.object.add")
                .tag("class", baseClass.getSimpleName())
                .description("Time taken to add an object")
                .register(meterRegistry);
        
        this.updateTimer = Timer.builder("api.object.update")
                .tag("class", baseClass.getSimpleName())
                .description("Time taken to update an object")
                .register(meterRegistry);
        
        this.removeTimer = Timer.builder("api.object.remove")
                .tag("class", baseClass.getSimpleName())
                .description("Time taken to remove an object")
                .register(meterRegistry);
    }

    /**
     * Get a single object by ID with circuit breaker and tracing.
     */
    @Path("{id}")
    @GET
    @Timed(value = "api.object.get.timed", extraTags = {"operation", "get"})
    public Response getSingle(@PathParam("id") long id) throws StorageException {
        // Create a span for this operation
        Span span = tracer.spanBuilder("get_" + baseClass.getSimpleName())
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("object.id", id)
                .setAttribute("object.class", baseClass.getSimpleName())
                .startSpan();
        
        try {
            // Use timer to measure operation duration
            return getSingleTimer.record(() -> {
                try {
                    // Execute with circuit breaker
                    return circuitBreaker.executeSupplier(() -> {
                        try {
                            permissionsService.checkPermission(baseClass, getUserId(), id);
                            T entity = storage.getObject(baseClass, new Request(
                                    new Columns.All(), new Condition.Equals("id", id)));
                            if (entity != null) {
                                span.setStatus(StatusCode.OK);
                                return Response.ok(entity).build();
                            } else {
                                span.setStatus(StatusCode.ERROR, "Entity not found");
                                return Response.status(Response.Status.NOT_FOUND).build();
                            }
                        } catch (StorageException e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw e;
                        }
                    });
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    if (e instanceof StorageException) {
                        throw (StorageException) e;
                    }
                    return Response.serverError().entity(e.getMessage()).build();
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Add a new object with circuit breaker and tracing.
     */
    @POST
    @Timed(value = "api.object.add.timed", extraTags = {"operation", "add"})
    public Response add(T entity) throws Exception {
        // Create a span for this operation
        Span span = tracer.spanBuilder("add_" + baseClass.getSimpleName())
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("object.class", baseClass.getSimpleName())
                .startSpan();
        
        try {
            // Use timer to measure operation duration
            return addTimer.record(() -> {
                try {
                    // Execute with circuit breaker
                    return circuitBreaker.executeSupplier(() -> {
                        try {
                            permissionsService.checkEdit(getUserId(), entity, true, false);

                            entity.setId(storage.addObject(entity, new Request(new Columns.Exclude("id"))));
                            actionLogger.create(request, getUserId(), entity);
                            
                            // Increment counter for object creation
                            meterRegistry.counter("api.object.created", "class", baseClass.getSimpleName()).increment();

                            if (getUserId() != ServiceAccountUser.ID) {
                                storage.addPermission(new Permission(User.class, getUserId(), baseClass, entity.getId()));
                                cacheManager.invalidatePermission(true, User.class, getUserId(), baseClass, entity.getId(), true);
                                connectionManager.invalidatePermission(true, User.class, getUserId(), baseClass, entity.getId(), true);
                                actionLogger.link(request, getUserId(), User.class, getUserId(), baseClass, entity.getId());
                            }
                            
                            span.setStatus(StatusCode.OK);
                            return Response.ok(entity).build();
                        } catch (Exception e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw e;
                        }
                    });
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Update an existing object with circuit breaker and tracing.
     */
    @Path("{id}")
    @PUT
    @Timed(value = "api.object.update.timed", extraTags = {"operation", "update"})
    public Response update(T entity) throws Exception {
        // Create a span for this operation
        Span span = tracer.spanBuilder("update_" + baseClass.getSimpleName())
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("object.id", entity.getId())
                .setAttribute("object.class", baseClass.getSimpleName())
                .startSpan();
        
        try {
            // Use timer to measure operation duration
            return updateTimer.record(() -> {
                try {
                    // Execute with circuit breaker
                    return circuitBreaker.executeSupplier(() -> {
                        try {
                            permissionsService.checkPermission(baseClass, getUserId(), entity.getId());

                            boolean skipReadonly = false;
                            if (entity instanceof User after) {
                                User before = storage.getObject(User.class, new Request(
                                        new Columns.All(), new Condition.Equals("id", entity.getId())));
                                permissionsService.checkUserUpdate(getUserId(), before, (User) entity);
                                skipReadonly = permissionsService.getUser(getUserId())
                                        .compare(after, "notificationTokens", "termsAccepted");
                            } else if (entity instanceof Group group) {
                                if (group.getId() == group.getGroupId()) {
                                    throw new IllegalArgumentException("Cycle in group hierarchy");
                                }
                            }

                            permissionsService.checkEdit(getUserId(), entity, false, skipReadonly);

                            storage.updateObject(entity, new Request(
                                    new Columns.Exclude("id"),
                                    new Condition.Equals("id", entity.getId())));
                            if (entity instanceof User user) {
                                if (user.getHashedPassword() != null) {
                                    storage.updateObject(entity, new Request(
                                            new Columns.Include("hashedPassword", "salt"),
                                            new Condition.Equals("id", entity.getId())));
                                }
                            }
                            
                            // Increment counter for object updates
                            meterRegistry.counter("api.object.updated", "class", baseClass.getSimpleName()).increment();
                            
                            // Invalidate distributed cache
                            cacheManager.invalidateObject(true, entity.getClass(), entity.getId(), ObjectOperation.UPDATE);
                            actionLogger.edit(request, getUserId(), entity);
                            
                            span.setStatus(StatusCode.OK);
                            return Response.ok(entity).build();
                        } catch (Exception e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw e;
                        }
                    });
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Remove an object with circuit breaker and tracing.
     */
    @Path("{id}")
    @DELETE
    @Timed(value = "api.object.remove.timed", extraTags = {"operation", "remove"})
    public Response remove(@PathParam("id") long id) throws Exception {
        // Create a span for this operation
        Span span = tracer.spanBuilder("remove_" + baseClass.getSimpleName())
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("object.id", id)
                .setAttribute("object.class", baseClass.getSimpleName())
                .startSpan();
        
        try {
            // Use timer to measure operation duration
            return removeTimer.record(() -> {
                try {
                    // Execute with circuit breaker
                    return circuitBreaker.executeSupplier(() -> {
                        try {
                            permissionsService.checkPermission(baseClass, getUserId(), id);
                            permissionsService.checkEdit(getUserId(), baseClass, false, false);

                            storage.removeObject(baseClass, new Request(new Condition.Equals("id", id)));
                            
                            // Increment counter for object deletions
                            meterRegistry.counter("api.object.deleted", "class", baseClass.getSimpleName()).increment();
                            
                            // Invalidate distributed cache
                            cacheManager.invalidateObject(true, baseClass, id, ObjectOperation.DELETE);
                            actionLogger.remove(request, getUserId(), baseClass, id);
                            
                            span.setStatus(StatusCode.OK);
                            return Response.noContent().build();
                        } catch (Exception e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw e;
                        }
                    });
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Execute a supplier with a fallback in case of failure.
     * This is a utility method for implementing the circuit breaker pattern with a fallback.
     *
     * @param supplier The supplier to execute
     * @param fallback The fallback supplier to execute if the main supplier fails
     * @param <R> The return type
     * @return The result of the supplier or fallback
     */
    protected <R> R executeWithFallback(Supplier<R> supplier, Supplier<R> fallback) {
        try {
            return circuitBreaker.executeSupplier(supplier);
        } catch (Exception e) {
            return fallback.get();
        }
    }

    /**
     * Execute a supplier asynchronously with a timeout.
     * This is a utility method for implementing asynchronous operations with timeouts.
     *
     * @param supplier The supplier to execute
     * @param timeout The timeout duration
     * @param <R> The return type
     * @return A CompletableFuture that will complete with the result or timeout
     */
    protected <R> CompletableFuture<R> executeAsync(Supplier<R> supplier, Duration timeout) {
        return CompletableFuture.supplyAsync(supplier)
                .orTimeout(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS)
                .exceptionally(throwable -> {
                    if (throwable instanceof TimeoutException) {
                        // Record timeout metric
                        meterRegistry.counter("api.object.timeout", 
                                "class", baseClass.getSimpleName()).increment();
                    }
                    throw new RuntimeException(throwable);
                });
    }
}