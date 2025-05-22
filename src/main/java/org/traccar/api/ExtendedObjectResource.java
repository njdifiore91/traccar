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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.discovery.ServiceDiscovery;
import org.traccar.model.BaseModel;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.model.User;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.ServiceUnavailableException;
import jakarta.ws.rs.core.Response;
import java.util.Collection;
import java.util.LinkedList;

public class ExtendedObjectResource<T extends BaseModel> extends BaseObjectResource<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtendedObjectResource.class);
    private final String sortField;
    private final Tracer tracer;
    private final ServiceDiscovery serviceDiscovery;

    @Inject
    public ExtendedObjectResource(Class<T> baseClass, String sortField, Tracer tracer, ServiceDiscovery serviceDiscovery) {
        super(baseClass);
        this.sortField = sortField;
        this.tracer = tracer;
        this.serviceDiscovery = serviceDiscovery;
    }

    /**
     * Retrieves a collection of objects with filtering options.
     * This method supports service discovery, circuit breaking, and distributed tracing.
     *
     * @param all Whether to retrieve all objects (admin only)
     * @param userId Filter by user ID
     * @param groupId Filter by group ID
     * @param deviceId Filter by device ID
     * @return Collection of objects matching the criteria
     * @throws StorageException If a storage error occurs
     */
    @GET
    @Timed(value = "api.extended.get", description = "Time spent retrieving extended objects")
    @CircuitBreaker(name = "storageService", fallbackMethod = "getFallback")
    public Collection<T> get(
            @QueryParam("all") boolean all, @QueryParam("userId") long userId,
            @QueryParam("groupId") long groupId, @QueryParam("deviceId") long deviceId) throws StorageException {

        // Create span for distributed tracing
        Span span = tracer.spanBuilder("ExtendedObjectResource.get")
                .setParent(Context.current())
                .setAttribute("resource.class", baseClass.getSimpleName())
                .setAttribute("query.all", all)
                .setAttribute("query.userId", userId)
                .setAttribute("query.groupId", groupId)
                .setAttribute("query.deviceId", deviceId)
                .startSpan();

        try {
            var conditions = new LinkedList<Condition>();

            if (all) {
                if (permissionsService.notAdmin(getUserId())) {
                    conditions.add(new Condition.Permission(User.class, getUserId(), baseClass));
                }
            } else {
                if (userId == 0) {
                    conditions.add(new Condition.Permission(User.class, getUserId(), baseClass));
                } else {
                    permissionsService.checkUser(getUserId(), userId);
                    conditions.add(new Condition.Permission(User.class, userId, baseClass).excludeGroups());
                }
            }

            if (groupId > 0) {
                permissionsService.checkPermission(Group.class, getUserId(), groupId);
                conditions.add(new Condition.Permission(Group.class, groupId, baseClass).excludeGroups());
            }
            if (deviceId > 0) {
                permissionsService.checkPermission(Device.class, getUserId(), deviceId);
                conditions.add(new Condition.Permission(Device.class, deviceId, baseClass).excludeGroups());
            }

            // Verify storage service availability through service discovery
            if (!serviceDiscovery.isServiceAvailable("storage-service")) {
                LOGGER.warn("Storage service unavailable during object retrieval for {}", baseClass.getSimpleName());
                span.setAttribute("error", true);
                span.setAttribute("error.type", "service_unavailable");
                throw new ServiceUnavailableException("Storage service temporarily unavailable");
            }

            Collection<T> result = storage.getObjects(baseClass, new Request(
                    new Columns.All(), Condition.merge(conditions), sortField != null ? new Order(sortField) : null));
            
            // Add result metadata to span
            span.setAttribute("result.count", result.size());
            return result;
        } catch (Exception e) {
            span.setAttribute("error", true);
            span.setAttribute("error.type", e.getClass().getSimpleName());
            span.setAttribute("error.message", e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Fallback method for the get operation when the circuit breaker is open.
     * This provides resilience when the storage service is unavailable.
     *
     * @param all Whether to retrieve all objects
     * @param userId Filter by user ID
     * @param groupId Filter by group ID
     * @param deviceId Filter by device ID
     * @param e The exception that triggered the fallback
     * @return Empty response with appropriate status code
     */
    public Collection<T> getFallback(boolean all, long userId, long groupId, long deviceId, Exception e) {
        LOGGER.warn("Circuit breaker triggered for {} retrieval: {}", baseClass.getSimpleName(), e.getMessage());
        throw new ServiceUnavailableException(Response
                .status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("Service temporarily unavailable. Please try again later.")
                .build());
    }
}