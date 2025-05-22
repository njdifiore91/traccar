/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.warrenstrange.googleauth.GoogleAuthenticator;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.annotation.Timed;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.core.Context;
import org.traccar.api.BaseObjectResource;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.LogAction;
import org.traccar.helper.SessionHelper;
import org.traccar.helper.model.UserUtil;
import org.traccar.model.Device;
import org.traccar.model.ManagedUser;
import org.traccar.model.Permission;
import org.traccar.model.User;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Collection;
import java.util.LinkedList;

@Path("users")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class UserResource extends BaseObjectResource<User> {

    @Inject
    private Config config;

    @Inject
    private LogAction actionLogger;
    
    @Inject
    private ServiceDiscovery serviceDiscovery;
    
    @Inject
    private Tracer tracer;

    @Context
    private HttpServletRequest request;

    public UserResource() {
        super(User.class);
    }

    @GET
    @Timed(value = "user.get", description = "Time spent retrieving users")
    @CircuitBreaker(name = "userService", fallbackMethod = "getFallback")
    public Collection<User> get(
            @QueryParam("userId") long userId, @QueryParam("deviceId") long deviceId) throws StorageException {
        Span span = tracer.spanBuilder("UserResource.get")
                .setParent(Context.current())
                .setAttribute("userId", userId)
                .setAttribute("deviceId", deviceId)
                .startSpan();
        
        try {
            var conditions = new LinkedList<Condition>();
            if (userId > 0) {
                permissionsService.checkUser(getUserId(), userId);
                conditions.add(new Condition.Permission(User.class, userId, ManagedUser.class).excludeGroups());
            } else if (permissionsService.notAdmin(getUserId())) {
                conditions.add(new Condition.Permission(User.class, getUserId(), ManagedUser.class).excludeGroups());
            }
            if (deviceId > 0) {
                permissionsService.checkManager(getUserId());
                conditions.add(new Condition.Permission(User.class, Device.class, deviceId).excludeGroups());
            }
            return storage.getObjects(baseClass, new Request(
                    new Columns.All(), Condition.merge(conditions), new Order("name")));
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    public Collection<User> getFallback(long userId, long deviceId, Exception e) {
        // Fallback method for circuit breaker
        // In a real implementation, this might return cached data or a default response
        return new LinkedList<>();
    }

    @Override
    @PermitAll
    @POST
    @Timed(value = "user.add", description = "Time spent adding users")
    @CircuitBreaker(name = "userService", fallbackMethod = "addFallback")
    public Response add(User entity) throws StorageException {
        Span span = tracer.spanBuilder("UserResource.add")
                .setParent(Context.current())
                .startSpan();
        
        try {
            User currentUser = getUserId() > 0 ? permissionsService.getUser(getUserId()) : null;
            if (currentUser == null || !currentUser.getAdministrator()) {
                permissionsService.checkUserUpdate(getUserId(), new User(), entity);
                if (currentUser != null && currentUser.getUserLimit() != 0) {
                    int userLimit = currentUser.getUserLimit();
                    if (userLimit > 0) {
                        int userCount = storage.getObjects(baseClass, new Request(
                                new Columns.All(),
                                new Condition.Permission(User.class, getUserId(), ManagedUser.class).excludeGroups()))
                                .size();
                        if (userCount >= userLimit) {
                            throw new SecurityException("Manager user limit reached");
                        }
                    }
                } else {
                    if (UserUtil.isEmpty(storage)) {
                        entity.setAdministrator(true);
                    } else if (!permissionsService.getServer().getRegistration()) {
                        throw new SecurityException("Registration disabled");
                    }
                    if (permissionsService.getServer().getBoolean(Keys.WEB_TOTP_FORCE.getKey())
                            && entity.getTotpKey() == null) {
                        throw new SecurityException("One-time password key is required");
                    }
                    UserUtil.setUserDefaults(entity, config);
                }
            }

            entity.setId(storage.addObject(entity, new Request(new Columns.Exclude("id"))));
            storage.updateObject(entity, new Request(
                    new Columns.Include("hashedPassword", "salt"),
                    new Condition.Equals("id", entity.getId())));

            actionLogger.create(request, getUserId(), entity);

            if (currentUser != null && currentUser.getUserLimit() != 0) {
                storage.addPermission(new Permission(User.class, getUserId(), ManagedUser.class, entity.getId()));
                actionLogger.link(request, getUserId(), User.class, getUserId(), ManagedUser.class, entity.getId());
            }
            return Response.ok(entity).build();
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    public Response addFallback(User entity, Exception e) {
        // Fallback method for circuit breaker
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("Service temporarily unavailable").build();
    }

    @Path("{id}")
    @DELETE
    @Timed(value = "user.remove", description = "Time spent removing users")
    @CircuitBreaker(name = "userService", fallbackMethod = "removeFallback")
    public Response remove(@PathParam("id") long id) throws Exception {
        Span span = tracer.spanBuilder("UserResource.remove")
                .setParent(Context.current())
                .setAttribute("userId", id)
                .startSpan();
        
        try {
            Response response = super.remove(id);
            if (getUserId() == id) {
                request.getSession().removeAttribute(SessionHelper.USER_ID_KEY);
            }
            return response;
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    public Response removeFallback(long id, Exception e) {
        // Fallback method for circuit breaker
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("Service temporarily unavailable").build();
    }

    @Path("totp")
    @PermitAll
    @POST
    @Timed(value = "user.generateTotpKey", description = "Time spent generating TOTP keys")
    @CircuitBreaker(name = "userService", fallbackMethod = "generateTotpKeyFallback")
    public String generateTotpKey() throws StorageException {
        Span span = tracer.spanBuilder("UserResource.generateTotpKey")
                .setParent(Context.current())
                .startSpan();
        
        try {
            if (!permissionsService.getServer().getBoolean(Keys.WEB_TOTP_ENABLE.getKey())) {
                throw new SecurityException("One-time password is disabled");
            }
            return new GoogleAuthenticator().createCredentials().getKey();
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
    
    public String generateTotpKeyFallback(Exception e) {
        // Fallback method for circuit breaker
        return "Service temporarily unavailable";
    }

}