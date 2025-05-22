/*
 * Copyright 2016 - 2021 Anton Tananaev (anton@traccar.org)
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
package org.traccar.api.resource;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.BaseResource;
import org.traccar.discovery.ConsulServiceDiscovery;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.function.Supplier;

@Path("events")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EventResource extends BaseResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventResource.class);
    private static final String EVENT_SERVICE_NAME = "event-service";
    private static final String CIRCUIT_BREAKER_NAME = "eventServiceCircuitBreaker";
    
    private final ConsulServiceDiscovery serviceDiscovery;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final Timer eventRequestTimer;
    private final Client client;
    
    @Inject
    public EventResource(
            ConsulServiceDiscovery serviceDiscovery,
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.serviceDiscovery = serviceDiscovery;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        this.tracer = tracer;
        this.eventRequestTimer = Timer.builder("event.request.timer")
                .description("Timer for event service requests")
                .register(meterRegistry);
        this.client = ClientBuilder.newClient();
    }

    /**
     * Get event by ID from the Event Service
     * 
     * @param id Event ID to retrieve
     * @return Event object
     * @throws StorageException if there's an error retrieving the event
     */
    @Path("{id}")
    @GET
    @Timed(value = "event.get.timer", description = "Time taken to get an event")
    public Event get(@PathParam("id") long id) throws StorageException {
        // Create a span for this request
        Span span = tracer.spanBuilder("EventResource.get").startSpan();
        span.setAttribute("event.id", id);
        
        try {
            // Use circuit breaker to handle potential failures
            return circuitBreaker.executeSupplier(eventRequestWithTracing(id, span));
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error retrieving event from Event Service: {}", e.getMessage(), e);
            
            // Fallback to local storage if Event Service is unavailable
            if (e instanceof WebApplicationException) {
                throw e;
            }
            
            // Fallback to local storage
            LOGGER.info("Falling back to local storage for event {}", id);
            Event event = storage.getObject(Event.class, new Request(
                    new Columns.All(), new Condition.Equals("id", id)));
            if (event == null) {
                throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
            }
            permissionsService.checkPermission(Device.class, getUserId(), event.getDeviceId());
            return event;
        } finally {
            span.end();
        }
    }
    
    /**
     * Creates a supplier that retrieves an event from the Event Service with tracing context
     * 
     * @param id Event ID to retrieve
     * @param span Current span for tracing
     * @return Supplier that returns an Event
     */
    private Supplier<Event> eventRequestWithTracing(long id, Span span) {
        return () -> {
            // Measure the time taken for this request
            return eventRequestTimer.record(() -> {
                try {
                    // Use the current span as parent
                    Context context = Context.current().with(span);
                    
                    // Discover Event Service endpoint dynamically
                    String eventServiceUrl = serviceDiscovery.getServiceUrl(EVENT_SERVICE_NAME);
                    if (eventServiceUrl == null) {
                        LOGGER.error("Event Service not found in service discovery");
                        throw new WebApplicationException("Event Service not available", 
                                Response.Status.SERVICE_UNAVAILABLE);
                    }
                    
                    span.setAttribute("event.service.url", eventServiceUrl);
                    LOGGER.debug("Discovered Event Service at: {}", eventServiceUrl);
                    
                    // Make request to Event Service
                    WebTarget target = client.target(eventServiceUrl)
                            .path("api/events/" + id);
                    
                    // Execute request with tracing context
                    Event event = target.request(MediaType.APPLICATION_JSON)
                            .get(Event.class);
                    
                    // Check permissions locally after retrieving the event
                    if (event != null) {
                        permissionsService.checkPermission(Device.class, getUserId(), event.getDeviceId());
                    }
                    
                    return event;
                } catch (WebApplicationException e) {
                    // Propagate HTTP errors from the Event Service
                    LOGGER.warn("Event Service returned error: {}", e.getMessage());
                    throw e;
                } catch (Exception e) {
                    // Log and rethrow other exceptions
                    LOGGER.error("Error communicating with Event Service: {}", e.getMessage(), e);
                    throw new StorageException(e);
                }
            });
        };
    }
}