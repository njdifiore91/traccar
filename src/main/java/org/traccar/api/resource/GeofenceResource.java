/*
 * Copyright 2016 - 2017 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.traccar.api.ExtendedObjectResource;
import org.traccar.model.Geofence;

import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

@Path("geofences")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class GeofenceResource extends ExtendedObjectResource<Geofence> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeofenceResource.class);
    private static final String BACKEND_SERVICE = "position-service";
    
    @Autowired
    private DiscoveryClient discoveryClient;
    
    @Autowired
    private Tracer tracer;
    
    /**
     * Constructor for GeofenceResource.
     * Initializes the resource with Geofence class and sorting field.
     */
    public GeofenceResource() {
        super(Geofence.class, "name");
    }
    
    /**
     * Override the get method to add circuit breaker, tracing, and metrics.
     * This demonstrates the integration of resilience patterns and observability.
     */
    @Override
    @CircuitBreaker(name = "geofenceService", fallbackMethod = "getFallback")
    @Timed(value = "geofence.get.time", description = "Time taken to retrieve geofences")
    public java.util.Collection<Geofence> get(
            boolean all, long userId,
            long groupId, long deviceId) throws org.traccar.storage.StorageException {
        
        // Create a span for this operation
        Span span = tracer.spanBuilder("GeofenceResource.get")
                .setParent(Context.current())
                .setAttribute("service.name", "api-gateway")
                .setAttribute("resource.type", "geofence")
                .setAttribute("request.all", all)
                .setAttribute("request.userId", userId)
                .setAttribute("request.groupId", groupId)
                .setAttribute("request.deviceId", deviceId)
                .startSpan();
        
        try {
            // Log service discovery information
            if (discoveryClient != null) {
                LOGGER.debug("Discovered instances of {}: {}", 
                        BACKEND_SERVICE, 
                        discoveryClient.getInstances(BACKEND_SERVICE));
            }
            
            // Call the parent method to get the actual data
            java.util.Collection<Geofence> result = super.get(all, userId, groupId, deviceId);
            
            // Add result information to the span
            span.setAttribute("result.count", result.size());
            
            return result;
        } catch (Exception e) {
            // Record the exception in the span
            span.recordException(e);
            span.setAttribute("error", true);
            span.setAttribute("error.message", e.getMessage());
            throw e;
        } finally {
            // Always close the span
            span.end();
        }
    }
    
    /**
     * Fallback method for the circuit breaker.
     * This method is called when the circuit is open or when an exception occurs.
     */
    public java.util.Collection<Geofence> getFallback(
            boolean all, long userId,
            long groupId, long deviceId, Exception e) {
        
        LOGGER.warn("Executing fallback for geofence retrieval", e);
        
        // Create a span for the fallback operation
        Span span = tracer.spanBuilder("GeofenceResource.getFallback")
                .setParent(Context.current())
                .setAttribute("service.name", "api-gateway")
                .setAttribute("resource.type", "geofence")
                .setAttribute("fallback", true)
                .setAttribute("error.message", e.getMessage())
                .startSpan();
        
        try {
            // Return an empty list as fallback
            return new java.util.ArrayList<>();
        } finally {
            span.end();
        }
    }
}