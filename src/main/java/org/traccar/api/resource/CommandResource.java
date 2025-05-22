/*
 * Copyright 2015 - 2022 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 Gabor Somogyi (gabor.g.somogyi@gmail.com)
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
package org.traccar.api.resource;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.BaseProtocol;
import org.traccar.ServerManager;
import org.traccar.api.ExtendedObjectResource;
import org.traccar.database.CommandsManager;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.LogAction;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.model.Command;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.model.Position;
import org.traccar.model.QueuedCommand;
import org.traccar.model.Typed;
import org.traccar.model.User;
import org.traccar.model.UserRestrictions;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Path("commands")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class CommandResource extends ExtendedObjectResource<Command> {

    private static final Logger LOGGER = LoggerFactory.getLogger(CommandResource.class);

    @Inject
    private CommandsManager commandsManager;

    @Inject
    private ServerManager serverManager;

    @Inject
    private LogAction actionLogger;

    @Inject
    private ServiceDiscovery serviceDiscovery;
    
    @Inject
    private Tracer tracer;
    
    @Inject
    private MeterRegistry meterRegistry;
    
    @Inject
    @Named("protocolServiceCircuitBreaker")
    private CircuitBreaker circuitBreaker;
    
    private final Timer commandExecutionTimer;
    private final Counter commandSuccessCounter;
    private final Counter commandFailureCounter;

    @Context
    private HttpServletRequest request;

    public CommandResource() {
        super(Command.class, "description");
        
        // Initialize metrics
        commandExecutionTimer = Timer.builder("command.execution.time")
                .description("Time taken to execute commands")
                .register(meterRegistry);
        
        commandSuccessCounter = Counter.builder("command.execution.success")
                .description("Number of successfully executed commands")
                .register(meterRegistry);
        
        commandFailureCounter = Counter.builder("command.execution.failure")
                .description("Number of failed command executions")
                .register(meterRegistry);
    }

    private BaseProtocol getDeviceProtocol(long deviceId) throws StorageException {
        // Create a span for protocol resolution
        Span span = tracer.spanBuilder("getDeviceProtocol")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", deviceId)
                .startSpan();
        
        try {
            Position position = storage.getObject(Position.class, new Request(
                    new Columns.All(), new Condition.LatestPositions(deviceId)));
            
            if (position != null) {
                String protocolName = position.getProtocol();
                span.setAttribute("protocol", protocolName);
                
                // Use circuit breaker to get protocol from service discovery
                return circuitBreaker.executeSupplier(() -> {
                    try {
                        // Try to get protocol from service discovery first
                        BaseProtocol protocol = serviceDiscovery.getProtocolService(protocolName);
                        if (protocol != null) {
                            return protocol;
                        }
                        // Fall back to local ServerManager if not found in service discovery
                        return serverManager.getProtocol(protocolName);
                    } catch (Exception e) {
                        LOGGER.warn("Error getting protocol from service discovery", e);
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        // Fall back to local ServerManager
                        return serverManager.getProtocol(protocolName);
                    }
                });
            } else {
                span.setAttribute("protocol", "null");
                return null;
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    @GET
    @Path("send")
    public Collection<Command> get(@QueryParam("deviceId") long deviceId) throws StorageException {
        // Create a span for getting available commands
        Span span = tracer.spanBuilder("getAvailableCommands")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("deviceId", deviceId)
                .startSpan();
        
        try {
            permissionsService.checkPermission(Device.class, getUserId(), deviceId);
            BaseProtocol protocol = getDeviceProtocol(deviceId);

            var commands = storage.getObjects(baseClass, new Request(
                    new Columns.All(),
                    Condition.merge(List.of(
                            new Condition.Permission(User.class, getUserId(), baseClass),
                            new Condition.Permission(Device.class, deviceId, baseClass)
                    ))));

            Collection<Command> filteredCommands = commands.stream().filter(command -> {
                String type = command.getType();
                if (protocol != null) {
                    return command.getTextChannel() && protocol.getSupportedTextCommands().contains(type)
                            || !command.getTextChannel() && protocol.getSupportedDataCommands().contains(type);
                } else {
                    return type.equals(Command.TYPE_CUSTOM);
                }
            }).collect(Collectors.toList());
            
            span.setAttribute("commandCount", filteredCommands.size());
            return filteredCommands;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    @POST
    @Path("send")
    public Response send(Command entity, @QueryParam("groupId") long groupId) throws Exception {
        // Create a span for sending command
        Span span = tracer.spanBuilder("sendCommand")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("commandType", entity.getType())
                .setAttribute("deviceId", entity.getDeviceId())
                .setAttribute("groupId", groupId)
                .startSpan();
        
        try {
            // Use timer to measure command execution time
            return commandExecutionTimer.record(() -> {
                try {
                    if (entity.getId() > 0) {
                        permissionsService.checkPermission(baseClass, getUserId(), entity.getId());
                        long deviceId = entity.getDeviceId();
                        entity = storage.getObject(baseClass, new Request(
                                new Columns.All(), new Condition.Equals("id", entity.getId())));
                        entity.setDeviceId(deviceId);
                    } else {
                        permissionsService.checkRestriction(getUserId(), UserRestrictions::getLimitCommands);
                    }

                    if (groupId > 0) {
                        permissionsService.checkPermission(Group.class, getUserId(), groupId);
                        var devices = DeviceUtil.getAccessibleDevices(storage, getUserId(), List.of(), List.of(groupId));
                        List<QueuedCommand> queuedCommands = new ArrayList<>();
                        for (Device device : devices) {
                            Command command = QueuedCommand.fromCommand(entity).toCommand();
                            command.setDeviceId(device.getId());
                            QueuedCommand queuedCommand = sendCommandWithCircuitBreaker(command, span);
                            if (queuedCommand != null) {
                                queuedCommands.add(queuedCommand);
                            }
                        }
                        if (!queuedCommands.isEmpty()) {
                            commandSuccessCounter.increment();
                            return Response.accepted(queuedCommands).build();
                        }
                    } else {
                        permissionsService.checkPermission(Device.class, getUserId(), entity.getDeviceId());
                        QueuedCommand queuedCommand = sendCommandWithCircuitBreaker(entity, span);
                        if (queuedCommand != null) {
                            commandSuccessCounter.increment();
                            return Response.accepted(queuedCommand).build();
                        }
                    }

                    actionLogger.command(request, getUserId(), groupId, entity.getDeviceId(), entity.getType());
                    commandSuccessCounter.increment();
                    return Response.ok(entity).build();
                } catch (Exception e) {
                    commandFailureCounter.increment();
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } finally {
            span.end();
        }
    }
    
    private QueuedCommand sendCommandWithCircuitBreaker(Command command, Span parentSpan) {
        Span span = tracer.spanBuilder("executeCommand")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("commandType", command.getType())
                .setAttribute("deviceId", command.getDeviceId())
                .startSpan();
        
        try {
            return circuitBreaker.executeSupplier(() -> {
                try {
                    return commandsManager.sendCommand(command);
                } catch (Exception e) {
                    LOGGER.warn("Error sending command", e);
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw new RuntimeException("Failed to send command: " + e.getMessage(), e);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Circuit breaker error when sending command", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Circuit breaker error: " + e.getMessage());
            return null;
        } finally {
            span.end();
        }
    }

    @GET
    @Path("types")
    public Collection<Typed> get(
            @QueryParam("deviceId") long deviceId,
            @QueryParam("textChannel") boolean textChannel) throws StorageException {
        // Create a span for getting command types
        Span span = tracer.spanBuilder("getCommandTypes")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("deviceId", deviceId)
                .setAttribute("textChannel", textChannel)
                .startSpan();
        
        try {
            if (deviceId != 0) {
                permissionsService.checkPermission(Device.class, getUserId(), deviceId);
                BaseProtocol protocol = getDeviceProtocol(deviceId);
                if (protocol != null) {
                    Collection<Typed> result;
                    if (textChannel) {
                        result = protocol.getSupportedTextCommands().stream().map(Typed::new).collect(Collectors.toList());
                    } else {
                        result = protocol.getSupportedDataCommands().stream().map(Typed::new).collect(Collectors.toList());
                    }
                    span.setAttribute("commandTypeCount", result.size());
                    return result;
                } else {
                    span.setAttribute("commandTypeCount", 1);
                    return Collections.singletonList(new Typed(Command.TYPE_CUSTOM));
                }
            } else {
                List<Typed> result = new ArrayList<>();
                Field[] fields = Command.class.getDeclaredFields();
                for (Field field : fields) {
                    if (Modifier.isStatic(field.getModifiers()) && field.getName().startsWith("TYPE_")) {
                        try {
                            result.add(new Typed(field.get(null).toString()));
                        } catch (IllegalArgumentException | IllegalAccessException error) {
                            LOGGER.warn("Get command types error", error);
                            span.recordException(error);
                        }
                    }
                }
                span.setAttribute("commandTypeCount", result.size());
                return result;
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

}