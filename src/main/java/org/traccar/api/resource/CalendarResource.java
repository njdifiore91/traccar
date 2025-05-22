/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 - 2017 Andrey Kunitsyn (andrey@traccar.org)
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

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.SimpleObjectResource;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.model.Calendar;
import org.traccar.storage.StorageException;

import java.util.Collection;

/**
 * Resource for managing calendar entities with enhanced resilience and observability.
 * Implements service discovery, circuit breaking, distributed tracing, and metrics collection.
 */
@Path("calendars")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CalendarResource extends SimpleObjectResource<Calendar> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CalendarResource.class);
    
    @Inject
    private ServiceDiscovery serviceDiscovery;
    
    @Inject
    private Tracer tracer;
    
    @Inject
    private MeterRegistry meterRegistry;
    
    private Timer calendarGetTimer;
    private Counter calendarSuccessCounter;
    private Counter calendarErrorCounter;

    /**
     * Initializes the calendar resource.
     */
    public CalendarResource() {
        super(Calendar.class, "name");
    }
    
    /**
     * Initializes metrics after dependency injection is complete.
     */
    @jakarta.annotation.PostConstruct
    public void initMetrics() {
        // Initialize calendar-specific metrics
        this.calendarGetTimer = Timer.builder("api.calendar.get.timer")
                .description("Time taken to retrieve calendar objects")
                .register(meterRegistry);
                
        this.calendarSuccessCounter = Counter.builder("api.calendar.success")
                .description("Number of successful calendar operations")
                .register(meterRegistry);
                
        this.calendarErrorCounter = Counter.builder("api.calendar.error")
                .description("Number of failed calendar operations")
                .register(meterRegistry);
    }
    
    /**
     * Retrieves calendar objects with enhanced monitoring and resilience.
     * Overrides the parent method to add calendar-specific tracing and metrics.
     *
     * @param all Whether to retrieve all calendars (admin only) or just those accessible to the user
     * @param userId Optional user ID to filter calendars by permission
     * @return Collection of calendar objects matching the criteria
     * @throws StorageException If there's an error accessing the storage
     */
    @GET
    @Override
    @Timed(value = "api.request.get", extraTags = {"resource", "calendar"})
    @CircuitBreaker(name = "calendarService", fallbackMethod = "getCalendarFallback")
    public Collection<Calendar> get(
            @QueryParam("all") boolean all, @QueryParam("userId") long userId) throws StorageException {
        
        Span span = tracer.spanBuilder("get_calendars")
                .setAttribute("resource.type", "Calendar")
                .setAttribute("query.all", all)
                .setAttribute("query.userId", userId)
                .startSpan();
        
        try {
            LOGGER.debug("Retrieving calendars (all={}, userId={})", all, userId);
            
            // Use the timer to record the operation duration
            Collection<Calendar> result = calendarGetTimer.record(() -> {
                // Call the parent implementation which already has service discovery integration
                return super.get(all, userId);
            });
            
            // Record success metrics
            calendarSuccessCounter.increment();
            span.setAttribute("result.count", result.size());
            
            return result;
        } catch (Exception e) {
            // Record error metrics
            calendarErrorCounter.increment();
            span.recordException(e);
            LOGGER.error("Error retrieving calendars: {}", e.getMessage(), e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Fallback method for calendar retrieval when the circuit breaker is triggered.
     *
     * @param all Whether to retrieve all calendars (admin only) or just those accessible to the user
     * @param userId Optional user ID to filter calendars by permission
     * @param e The exception that triggered the fallback
     * @return Empty collection or throws an appropriate exception
     */
    public Collection<Calendar> getCalendarFallback(boolean all, long userId, Exception e) {
        LOGGER.warn("Circuit breaker activated for calendar retrieval: {}", e.getMessage());
        
        // Create a span for the fallback operation
        Span span = tracer.spanBuilder("calendar_circuit_breaker_fallback")
                .setAttribute("error.type", e.getClass().getName())
                .setAttribute("error.message", e.getMessage())
                .startSpan();
        
        try {
            // For calendar retrieval, we should fail with a service unavailable response
            // rather than returning potentially stale or incomplete data
            throw new WebApplicationException("Calendar service temporarily unavailable", 
                    Response.Status.SERVICE_UNAVAILABLE);
        } finally {
            span.end();
        }
    }
}