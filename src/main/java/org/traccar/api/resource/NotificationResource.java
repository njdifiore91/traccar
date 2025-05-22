/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.ExtendedObjectResource;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.model.Event;
import org.traccar.model.ManagedUser;
import org.traccar.model.Notification;
import org.traccar.model.Typed;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationMessage;
import org.traccar.notification.NotificatorManager;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Path("notifications")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class NotificationResource extends ExtendedObjectResource<Notification> {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationResource.class);
    private static final String NOTIFICATION_SERVICE_NAME = "notification-service";
    private static final String CIRCUIT_BREAKER_NAME = "notificationServiceCircuitBreaker";
    private static final int CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD = 50;
    private static final int CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE = 30;
    private static final int CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE = 10;
    private static final int CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN_STATE = 5;
    
    private final CircuitBreaker circuitBreaker;
    
    @Inject
    private NotificatorManager notificatorManager;
    
    @Inject
    private ServiceDiscovery serviceDiscovery;
    
    @Inject
    private Tracer tracer;
    
    @Inject
    private MeterRegistry meterRegistry;
    
    private final Timer notificationRequestTimer;

    public NotificationResource() {
        super(Notification.class, "description");
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD)
                .waitDurationInOpenState(Duration.ofSeconds(CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE))
                .slidingWindowSize(CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE)
                .permittedNumberOfCallsInHalfOpenState(CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN_STATE)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Initialize metrics
        this.notificationRequestTimer = Timer.builder("notification.request.duration")
                .description("Time taken to process notification requests")
                .register(meterRegistry);
    }
    
    /**
     * Discovers the Notification Service instance using service discovery.
     * 
     * @return The service instance if found, empty otherwise
     */
    private Optional<ServiceInstance> discoverNotificationService() {
        Span span = tracer.spanBuilder("DiscoverNotificationService")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Collection<ServiceInstance> instances = serviceDiscovery.findServiceInstances(NOTIFICATION_SERVICE_NAME);
            
            if (instances.isEmpty()) {
                LOGGER.warn("No instances of Notification Service found");
                span.setStatus(StatusCode.ERROR, "No instances found");
                return Optional.empty();
            }
            
            // For simplicity, just return the first healthy instance
            Optional<ServiceInstance> healthyInstance = instances.stream()
                    .filter(ServiceInstance::isHealthy)
                    .findFirst();
            
            if (healthyInstance.isEmpty()) {
                LOGGER.warn("No healthy instances of Notification Service found");
                span.setStatus(StatusCode.ERROR, "No healthy instances found");
            } else {
                span.setAttribute("service.instance.id", healthyInstance.get().getId());
                span.setAttribute("service.instance.host", healthyInstance.get().getHost());
                span.setAttribute("service.instance.port", healthyInstance.get().getPort());
            }
            
            return healthyInstance;
        } catch (Exception e) {
            LOGGER.error("Error discovering Notification Service", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            return Optional.empty();
        } finally {
            span.end();
        }
    }
    
    /**
     * Executes a function with circuit breaker, tracing, and metrics.
     * 
     * @param <T> The return type of the function
     * @param operationName The name of the operation for tracing
     * @param supplier The function to execute
     * @return The result of the function
     * @throws Exception If an error occurs during execution
     */
    private <T> T executeWithResilienceAndTracing(String operationName, Supplier<T> supplier) throws Exception {
        Span span = tracer.spanBuilder(operationName)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("circuit_breaker.name", CIRCUIT_BREAKER_NAME)
                .setAttribute("circuit_breaker.state", circuitBreaker.getState().name())
                .startSpan();
        
        Context context = Context.current().with(span);
        
        try (Scope scope = context.makeCurrent()) {
            return notificationRequestTimer.record(() -> {
                try {
                    return circuitBreaker.executeSupplier(supplier);
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    } else {
                        throw new RuntimeException("Error executing operation: " + operationName, e);
                    }
                }
            });
        } finally {
            span.end();
        }
    }

    @GET
    @Path("types")
    @Timed(value = "notification.types.get", description = "Time taken to get notification types")
    public Collection<Typed> get() {
        Span span = tracer.spanBuilder("GetNotificationTypes")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            List<Typed> types = new LinkedList<>();
            Field[] fields = Event.class.getDeclaredFields();
            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers()) && field.getName().startsWith("TYPE_")) {
                    try {
                        types.add(new Typed(field.get(null).toString()));
                    } catch (IllegalArgumentException | IllegalAccessException error) {
                        LOGGER.warn("Get event types error", error);
                        span.recordException(error);
                    }
                }
            }
            span.setAttribute("notification.types.count", types.size());
            return types;
        } finally {
            span.end();
        }
    }

    @GET
    @Path("notificators")
    @Timed(value = "notification.notificators.get", description = "Time taken to get notificators")
    public Collection<Typed> getNotificators(@QueryParam("announcement") boolean announcement) {
        Span span = tracer.spanBuilder("GetNotificators")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("announcement", announcement)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            try {
                return executeWithResilienceAndTracing("GetNotificatorTypes", () -> {
                    Set<String> announcementsUnsupported = Set.of("command", "web");
                    Collection<Typed> notificators = notificatorManager.getAllNotificatorTypes().stream()
                            .filter(typed -> !announcement || !announcementsUnsupported.contains(typed.type()))
                            .collect(Collectors.toUnmodifiableSet());
                    span.setAttribute("notification.notificators.count", notificators.size());
                    return notificators;
                });
            } catch (Exception e) {
                LOGGER.error("Error getting notificators", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                throw new RuntimeException("Failed to get notificators", e);
            }
        } finally {
            span.end();
        }
    }

    @POST
    @Path("test")
    @Timed(value = "notification.test.all", description = "Time taken to test all notification methods")
    public Response testMessage() throws MessageException, StorageException {
        Span span = tracer.spanBuilder("TestAllNotifications")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Optional<ServiceInstance> serviceInstanceOpt = discoverNotificationService();
            if (serviceInstanceOpt.isEmpty()) {
                span.setStatus(StatusCode.ERROR, "Notification service not available");
                return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity("Notification service not available")
                        .build();
            }
            
            try {
                return executeWithResilienceAndTracing("TestAllNotificators", () -> {
                    User user = permissionsService.getUser(getUserId());
                    span.setAttribute("user.id", user.getId());
                    
                    Collection<Typed> notificatorTypes = notificatorManager.getAllNotificatorTypes();
                    span.setAttribute("notification.methods.count", notificatorTypes.size());
                    
                    for (Typed method : notificatorTypes) {
                        try {
                            notificatorManager.getNotificator(method.type()).send(null, user, new Event("test", 0), null);
                            span.setAttribute("notification.method." + method.type() + ".success", true);
                        } catch (Exception e) {
                            LOGGER.warn("Failed to test notification method: {}", method.type(), e);
                            span.setAttribute("notification.method." + method.type() + ".success", false);
                            span.recordException(e);
                        }
                    }
                    
                    return Response.noContent().build();
                });
            } catch (Exception e) {
                LOGGER.error("Error testing notifications", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                
                if (e.getCause() instanceof TimeoutException) {
                    return Response.status(Response.Status.GATEWAY_TIMEOUT)
                            .entity("Notification service timeout")
                            .build();
                } else {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity("Failed to test notifications: " + e.getMessage())
                            .build();
                }
            }
        } finally {
            span.end();
        }
    }

    @POST
    @Path("test/{notificator}")
    @Timed(value = "notification.test.single", description = "Time taken to test a specific notification method")
    public Response testMessage(@PathParam("notificator") String notificator)
            throws MessageException, StorageException {
        Span span = tracer.spanBuilder("TestSingleNotification")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("notification.method", notificator)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Optional<ServiceInstance> serviceInstanceOpt = discoverNotificationService();
            if (serviceInstanceOpt.isEmpty()) {
                span.setStatus(StatusCode.ERROR, "Notification service not available");
                return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity("Notification service not available")
                        .build();
            }
            
            try {
                return executeWithResilienceAndTracing("TestNotificator", () -> {
                    User user = permissionsService.getUser(getUserId());
                    span.setAttribute("user.id", user.getId());
                    
                    notificatorManager.getNotificator(notificator).send(null, user, new Event("test", 0), null);
                    
                    return Response.noContent().build();
                });
            } catch (Exception e) {
                LOGGER.error("Error testing notification method: {}", notificator, e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                
                if (e.getCause() instanceof TimeoutException) {
                    return Response.status(Response.Status.GATEWAY_TIMEOUT)
                            .entity("Notification service timeout")
                            .build();
                } else {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity("Failed to test notification: " + e.getMessage())
                            .build();
                }
            }
        } finally {
            span.end();
        }
    }

    @POST
    @Path("send/{notificator}")
    @Timed(value = "notification.send", description = "Time taken to send notifications")
    public Response sendMessage(
            @PathParam("notificator") String notificator, @QueryParam("userId") List<Long> userIds,
            NotificationMessage message) throws MessageException, StorageException {
        Span span = tracer.spanBuilder("SendNotification")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("notification.method", notificator)
                .setAttribute("notification.userIds.specified", !userIds.isEmpty())
                .setAttribute("notification.userIds.count", userIds.size())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Optional<ServiceInstance> serviceInstanceOpt = discoverNotificationService();
            if (serviceInstanceOpt.isEmpty()) {
                span.setStatus(StatusCode.ERROR, "Notification service not available");
                return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                        .entity("Notification service not available")
                        .build();
            }
            
            try {
                return executeWithResilienceAndTracing("SendNotifications", () -> {
                    permissionsService.checkManager(getUserId());
                    span.setAttribute("user.id", getUserId());
                    
                    List<User> users;
                    if (userIds.isEmpty()) {
                        if (permissionsService.notAdmin(getUserId())) {
                            users = storage.getObjects(User.class, new Request(new Columns.All(),
                                    new Condition.Permission(User.class, getUserId(), ManagedUser.class).excludeGroups()));
                        } else {
                            users = storage.getObjects(User.class, new Request(new Columns.All()));
                        }
                    } else {
                        users = new ArrayList<>();
                        for (long userId : userIds) {
                            var conditions = new LinkedList<Condition>();
                            conditions.add(new Condition.Equals("id", userId));
                            if (permissionsService.notAdmin(getUserId())) {
                                conditions.add(new Condition.Permission(
                                        User.class, getUserId(), ManagedUser.class).excludeGroups());
                            }
                            users.add(storage.getObject(
                                    User.class, new Request(new Columns.All(), Condition.merge(conditions))));
                        }
                    }
                    
                    span.setAttribute("notification.users.total", users.size());
                    int sentCount = 0;
                    int skippedCount = 0;
                    
                    for (User user : users) {
                        if (!user.getTemporary()) {
                            try {
                                notificatorManager.getNotificator(notificator).send(user, message, null, null);
                                sentCount++;
                            } catch (Exception e) {
                                LOGGER.warn("Failed to send notification to user {}: {}", user.getId(), e.getMessage());
                                span.recordException(e);
                            }
                        } else {
                            skippedCount++;
                        }
                    }
                    
                    span.setAttribute("notification.sent.count", sentCount);
                    span.setAttribute("notification.skipped.count", skippedCount);
                    
                    return Response.noContent().build();
                });
            } catch (Exception e) {
                LOGGER.error("Error sending notifications", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                
                if (e.getCause() instanceof TimeoutException) {
                    return Response.status(Response.Status.GATEWAY_TIMEOUT)
                            .entity("Notification service timeout")
                            .build();
                } else {
                    return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                            .entity("Failed to send notifications: " + e.getMessage())
                            .build();
                }
            }
        } finally {
            span.end();
        }
    }

}