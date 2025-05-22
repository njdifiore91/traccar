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
package org.traccar.api.resource;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.annotation.Timed;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.traccar.api.ExtendedObjectResource;
import org.traccar.model.Attribute;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.handler.ComputedAttributesHandler;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;
import org.traccar.discovery.ServiceDiscovery;

@Path("attributes/computed")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class AttributeResource extends ExtendedObjectResource<Attribute> {

    @Inject
    private CacheManager cacheManager;

    @Inject
    private ComputedAttributesHandler.Late computedAttributesHandler;
    
    @Inject
    private ServiceDiscovery serviceDiscovery;
    
    @Inject
    private Tracer tracer;

    public AttributeResource() {
        super(Attribute.class, "description");
    }

    @POST
    @Path("test")
    @Timed(value = "attribute.test", description = "Time taken to test an attribute")
    @CircuitBreaker(name = "positionService", fallbackMethod = "testFallback")
    public Response test(@QueryParam("deviceId") long deviceId, Attribute entity) throws Exception {
        // Create a span for this operation
        Span span = tracer.spanBuilder("attribute.test").setParent(Context.current()).startSpan();
        
        try {
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("attributeId", entity.getId());
            
            permissionsService.checkAdmin(getUserId());
            permissionsService.checkPermission(Device.class, getUserId(), deviceId);

            // Use service discovery to locate the Position Service
            String positionServiceUrl = serviceDiscovery.getServiceUrl("position-service");
            span.setAttribute("positionServiceUrl", positionServiceUrl);
            
            Position position = storage.getObject(Position.class, new Request(
                    new Columns.All(),
                    new Condition.LatestPositions(deviceId)));

            var key = new Object();
            try {
                cacheManager.addDevice(position.getDeviceId(), key);
                Object result = computedAttributesHandler.computeAttribute(entity, position);
                if (result != null) {
                    return switch (entity.getType()) {
                        case "number", "boolean" -> Response.ok(result).build();
                        default -> Response.ok(result.toString()).build();
                    };
                } else {
                    return Response.noContent().build();
                }
            } finally {
                cacheManager.removeDevice(position.getDeviceId(), key);
            }
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Fallback method for the test endpoint when the circuit breaker is open
     */
    public Response testFallback(long deviceId, Attribute entity, Exception e) {
        // Log the exception that triggered the fallback
        Span span = tracer.spanBuilder("attribute.test.fallback").startSpan();
        try {
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("attributeId", entity.getId());
            span.setAttribute("error", e.getMessage());
            
            // Return a service unavailable response
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity("Position service is currently unavailable. Please try again later.")
                    .build();
        } finally {
            span.end();
        }
    }

    @POST
    @Timed(value = "attribute.add", description = "Time taken to add an attribute")
    public Response add(Attribute entity) throws Exception {
        permissionsService.checkAdmin(getUserId());
        return super.add(entity);
    }

    @Path("{id}")
    @PUT
    @Timed(value = "attribute.update", description = "Time taken to update an attribute")
    public Response update(Attribute entity) throws Exception {
        permissionsService.checkAdmin(getUserId());
        return super.update(entity);
    }

    @Path("{id}")
    @DELETE
    @Timed(value = "attribute.remove", description = "Time taken to remove an attribute")
    public Response remove(@PathParam("id") long id) throws Exception {
        permissionsService.checkAdmin(getUserId());
        return super.remove(id);
    }

}