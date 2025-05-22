/*
 * Copyright 2017 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.database;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.BaseProtocol;
import org.traccar.ServerManager;
import org.traccar.broadcast.BroadcastInterface;
import org.traccar.broadcast.BroadcastService;
import org.traccar.model.Command;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.ObjectOperation;
import org.traccar.model.Position;
import org.traccar.model.QueuedCommand;
import org.traccar.push.PushCommandManager;
import org.traccar.session.ConnectionManager;
import org.traccar.session.DeviceSession;
import org.traccar.session.cache.CacheManager;
import org.traccar.sms.SmsManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;
import org.traccar.messaging.CommandProducer;
import org.traccar.messaging.CommandConsumer;
import org.traccar.discovery.ServiceDiscovery;

import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Manages device commands, including sending, queuing, and processing.
 * Implements circuit breaker pattern, distributed tracing, and metrics collection.
 * Uses message broker for asynchronous command delivery with retry policies.
 */
@Singleton
public class CommandsManager implements BroadcastInterface {

    private static final String COMMAND_TOPIC = "command-delivery";
    private static final String COMMAND_RESPONSE_TOPIC = "command-response";
    private static final String CIRCUIT_BREAKER_NAME = "commandDelivery";
    private static final int MAX_RETRY_ATTEMPTS = 5;
    private static final Duration INITIAL_BACKOFF = Duration.ofMillis(500);
    private static final double BACKOFF_MULTIPLIER = 2.0;
    
    private final Storage storage;
    private final ServerManager serverManager;
    private final SmsManager smsManager;
    private final ConnectionManager connectionManager;
    private final BroadcastService broadcastService;
    private final NotificationManager notificationManager;
    private final CacheManager cacheManager;
    private final PushCommandManager pushCommandManager;
    private final DatabaseCircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final CommandProducer commandProducer;
    private final CommandConsumer commandConsumer;
    private final ServiceDiscovery serviceDiscovery;
    private final DatabaseMetricsCollector metricsCollector;
    private final Executor retryExecutor;

    /**
     * Constructs a new CommandsManager with the necessary dependencies.
     *
     * @param storage Storage for persisting commands
     * @param serverManager Server manager for protocol access
     * @param smsManager SMS manager for text channel commands (nullable)
     * @param connectionManager Connection manager for device sessions
     * @param broadcastService Broadcast service for command updates
     * @param notificationManager Notification manager for command events
     * @param cacheManager Cache manager for device information
     * @param pushCommandManager Push command manager for push notifications (nullable)
     * @param circuitBreaker Circuit breaker for command delivery resilience
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param commandProducer Producer for sending commands to message broker
     * @param commandConsumer Consumer for receiving commands from message broker
     * @param serviceDiscovery Service discovery for locating protocol services
     * @param metricsCollector Metrics collector for performance monitoring
     */
    @Inject
    public CommandsManager(
            Storage storage, ServerManager serverManager, @Nullable SmsManager smsManager,
            ConnectionManager connectionManager, BroadcastService broadcastService,
            NotificationManager notificationManager, CacheManager cacheManager,
            @Nullable PushCommandManager pushCommandManager, DatabaseCircuitBreaker circuitBreaker,
            Tracer tracer, CommandProducer commandProducer, CommandConsumer commandConsumer,
            ServiceDiscovery serviceDiscovery, DatabaseMetricsCollector metricsCollector) {
        this.storage = storage;
        this.serverManager = serverManager;
        this.smsManager = smsManager;
        this.connectionManager = connectionManager;
        this.broadcastService = broadcastService;
        this.notificationManager = notificationManager;
        this.cacheManager = cacheManager;
        this.pushCommandManager = pushCommandManager;
        this.circuitBreaker = circuitBreaker;
        this.tracer = tracer;
        this.commandProducer = commandProducer;
        this.commandConsumer = commandConsumer;
        this.serviceDiscovery = serviceDiscovery;
        this.metricsCollector = metricsCollector;
        this.retryExecutor = Executors.newScheduledThreadPool(2);
        
        broadcastService.registerListener(this);
        initializeCommandConsumer();
    }

    /**
     * Initializes the command consumer to process commands from the message broker.
     */
    private void initializeCommandConsumer() {
        commandConsumer.subscribe(COMMAND_TOPIC, (command, metadata) -> {
            Span span = tracer.spanBuilder("process-command-from-broker")
                    .setSpanKind(SpanKind.CONSUMER)
                    .setAttribute("command.type", command.getType())
                    .setAttribute("command.deviceId", command.getDeviceId())
                    .startSpan();
            
            try (var scope = span.makeCurrent()) {
                processCommandFromBroker(command, metadata);
                span.setStatus(StatusCode.OK);
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
            } finally {
                span.end();
            }
        });
        
        commandConsumer.subscribe(COMMAND_RESPONSE_TOPIC, (response, metadata) -> {
            // Process command responses (acknowledgments, results, etc.)
            // This would be used for tracking command completion status
        });
    }

    /**
     * Processes a command received from the message broker.
     *
     * @param command The command to process
     * @param metadata Metadata associated with the command
     */
    private void processCommandFromBroker(Command command, Map<String, String> metadata) {
        int retryCount = Integer.parseInt(metadata.getOrDefault("retry-count", "0"));
        boolean isTextChannel = command.getTextChannel();
        
        try {
            if (isTextChannel) {
                sendTextCommand(command);
            } else {
                sendDataCommand(command);
            }
            
            // Send acknowledgment of successful processing
            Map<String, String> responseMetadata = new HashMap<>();
            responseMetadata.put("status", "success");
            responseMetadata.put("deviceId", String.valueOf(command.getDeviceId()));
            commandProducer.send(COMMAND_RESPONSE_TOPIC, command, responseMetadata);
            
            // Record metrics for successful command delivery
            metricsCollector.recordCommandDelivery(command.getType(), true, retryCount);
        } catch (Exception e) {
            // Handle failure with retry if appropriate
            if (retryCount < MAX_RETRY_ATTEMPTS) {
                scheduleRetry(command, retryCount);
            } else {
                // Record metrics for failed command delivery after max retries
                metricsCollector.recordCommandDelivery(command.getType(), false, retryCount);
                
                // Send failure notification
                Map<String, String> responseMetadata = new HashMap<>();
                responseMetadata.put("status", "failed");
                responseMetadata.put("error", e.getMessage());
                responseMetadata.put("deviceId", String.valueOf(command.getDeviceId()));
                commandProducer.send(COMMAND_RESPONSE_TOPIC, command, responseMetadata);
            }
        }
    }

    /**
     * Schedules a retry for a failed command with exponential backoff.
     *
     * @param command The command to retry
     * @param currentRetryCount The current retry attempt count
     */
    private void scheduleRetry(Command command, int currentRetryCount) {
        int nextRetryCount = currentRetryCount + 1;
        long delayMillis = calculateExponentialBackoff(nextRetryCount);
        
        Map<String, String> metadata = new HashMap<>();
        metadata.put("retry-count", String.valueOf(nextRetryCount));
        
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(delayMillis);
                commandProducer.send(COMMAND_TOPIC, command, metadata);
                
                // Record metrics for command retry
                metricsCollector.recordCommandRetry(command.getType(), nextRetryCount);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, retryExecutor);
    }

    /**
     * Calculates the exponential backoff delay for retries.
     *
     * @param retryCount The current retry count
     * @return The delay in milliseconds before the next retry
     */
    private long calculateExponentialBackoff(int retryCount) {
        double exponentialFactor = Math.pow(BACKOFF_MULTIPLIER, retryCount - 1);
        long delay = (long) (INITIAL_BACKOFF.toMillis() * exponentialFactor);
        
        // Add jitter to prevent retry storms (±20%)
        double jitter = 0.8 + Math.random() * 0.4; // 0.8 to 1.2
        return (long) (delay * jitter);
    }

    /**
     * Sends a command to a device. Uses circuit breaker pattern for resilience.
     * Implements distributed tracing and metrics collection.
     *
     * @param command The command to send
     * @return The queued command if the command was queued, null otherwise
     * @throws Exception If an error occurs during command sending
     */
    public QueuedCommand sendCommand(Command command) throws Exception {
        Span span = tracer.spanBuilder("send-command")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("command.type", command.getType())
                .setAttribute("command.deviceId", command.getDeviceId())
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            long startTime = System.currentTimeMillis();
            QueuedCommand result = circuitBreaker.executeSupplier(CIRCUIT_BREAKER_NAME, () -> {
                try {
                    return sendCommandInternal(command);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            long endTime = System.currentTimeMillis();
            
            // Record metrics
            metricsCollector.recordCommandExecutionTime(command.getType(), endTime - startTime);
            metricsCollector.recordCommandDelivery(command.getType(), true, 0);
            
            span.setStatus(StatusCode.OK);
            return result;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            
            // Record metrics for failed command
            metricsCollector.recordCommandDelivery(command.getType(), false, 0);
            
            // If circuit breaker is open, try asynchronous delivery via message broker
            if (circuitBreaker.isCircuitBreakerOpen(CIRCUIT_BREAKER_NAME)) {
                span.addEvent("Circuit breaker open, using message broker fallback");
                sendCommandViaBroker(command);
            }
            
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Internal implementation of command sending logic.
     *
     * @param command The command to send
     * @return The queued command if the command was queued, null otherwise
     * @throws Exception If an error occurs during command sending
     */
    private QueuedCommand sendCommandInternal(Command command) throws Exception {
        long deviceId = command.getDeviceId();
        Device device = storage.getObject(Device.class, new Request(
                new Columns.Include("positionId", "phone", "attributes"), new Condition.Equals("id", deviceId)));
        Position position = storage.getObject(Position.class, new Request(
                new Columns.All(), new Condition.Equals("id", device.getPositionId())));
        BaseProtocol protocol = position != null ? serverManager.getProtocol(position.getProtocol()) : null;

        if (command.getTextChannel()) {
            return sendTextCommand(command, device, position, protocol);
        } else {
            return sendDataCommand(command, device, protocol);
        }
    }

    /**
     * Sends a command via the message broker for asynchronous processing.
     *
     * @param command The command to send
     */
    private void sendCommandViaBroker(Command command) {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("retry-count", "0");
        metadata.put("source", "circuit-breaker-fallback");
        
        commandProducer.send(COMMAND_TOPIC, command, metadata);
    }

    /**
     * Sends a text channel command.
     *
     * @param command The command to send
     * @param device The device to send the command to
     * @param position The last known position of the device
     * @param protocol The protocol used by the device
     * @return null (text commands are not queued)
     * @throws Exception If an error occurs during command sending
     */
    private QueuedCommand sendTextCommand(Command command, Device device, Position position, BaseProtocol protocol) 
            throws Exception {
        if (smsManager == null) {
            throw new RuntimeException("SMS not configured");
        }
        if (position != null) {
            protocol.sendTextCommand(device.getPhone(), command);
        } else if (command.getType().equals(Command.TYPE_CUSTOM)) {
            smsManager.sendMessage(device.getPhone(), command.getString(Command.KEY_DATA), true);
        } else {
            throw new RuntimeException("Command " + command.getType() + " is not supported");
        }
        return null;
    }

    /**
     * Sends a text channel command with minimal information.
     * Used when processing commands from the message broker.
     *
     * @param command The command to send
     * @throws Exception If an error occurs during command sending
     */
    private void sendTextCommand(Command command) throws Exception {
        Device device = storage.getObject(Device.class, new Request(
                new Columns.Include("positionId", "phone", "attributes"), 
                new Condition.Equals("id", command.getDeviceId())));
        Position position = storage.getObject(Position.class, new Request(
                new Columns.All(), new Condition.Equals("id", device.getPositionId())));
        BaseProtocol protocol = position != null ? serverManager.getProtocol(position.getProtocol()) : null;
        
        sendTextCommand(command, device, position, protocol);
    }

    /**
     * Sends a data channel command.
     *
     * @param command The command to send
     * @param device The device to send the command to
     * @param protocol The protocol used by the device
     * @return The queued command if the command was queued, null otherwise
     * @throws Exception If an error occurs during command sending
     */
    private QueuedCommand sendDataCommand(Command command, Device device, BaseProtocol protocol) throws Exception {
        if (pushCommandManager != null && protocol != null
                && protocol.getSupportedPushCommands().contains(command.getType())) {
            pushCommandManager.sendCommand(device, command);
            return null;
        } else {
            DeviceSession deviceSession = connectionManager.getDeviceSession(device.getId());
            if (deviceSession != null && deviceSession.supportsLiveCommands()) {
                deviceSession.sendCommand(command);
                return null;
            } else if (!command.getBoolean(Command.KEY_NO_QUEUE)) {
                QueuedCommand queuedCommand = QueuedCommand.fromCommand(command);
                queuedCommand.setId(storage.addObject(queuedCommand, new Request(new Columns.Exclude("id"))));
                broadcastService.updateCommand(true, device.getId());
                return queuedCommand;
            } else {
                throw new RuntimeException("Failed to send command");
            }
        }
    }

    /**
     * Sends a data channel command with minimal information.
     * Used when processing commands from the message broker.
     *
     * @param command The command to send
     * @throws Exception If an error occurs during command sending
     */
    private void sendDataCommand(Command command) throws Exception {
        Device device = storage.getObject(Device.class, new Request(
                new Columns.Include("id", "positionId", "attributes"), 
                new Condition.Equals("id", command.getDeviceId())));
        Position position = storage.getObject(Position.class, new Request(
                new Columns.All(), new Condition.Equals("id", device.getPositionId())));
        BaseProtocol protocol = position != null ? serverManager.getProtocol(position.getProtocol()) : null;
        
        sendDataCommand(command, device, protocol);
    }

    /**
     * Reads queued commands for a device.
     *
     * @param deviceId The device ID to read commands for
     * @return A collection of commands for the device
     */
    public Collection<Command> readQueuedCommands(long deviceId) {
        return readQueuedCommands(deviceId, Integer.MAX_VALUE);
    }

    /**
     * Reads a limited number of queued commands for a device.
     * Implements distributed tracing and metrics collection.
     *
     * @param deviceId The device ID to read commands for
     * @param count The maximum number of commands to read
     * @return A collection of commands for the device
     */
    public Collection<Command> readQueuedCommands(long deviceId, int count) {
        Span span = tracer.spanBuilder("read-queued-commands")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("deviceId", deviceId)
                .setAttribute("count", count)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            long startTime = System.currentTimeMillis();
            Collection<Command> result = circuitBreaker.executeSupplier(CIRCUIT_BREAKER_NAME, () -> {
                try {
                    var commands = storage.getObjects(QueuedCommand.class, new Request(
                            new Columns.All(),
                            new Condition.Equals("deviceId", deviceId),
                            new Order("id", false, count)));
                    Map<Event, Position> events = new HashMap<>();
                    for (var command : commands) {
                        storage.removeObject(QueuedCommand.class, new Request(
                                new Condition.Equals("id", command.getId())));

                        Event event = new Event(Event.TYPE_QUEUED_COMMAND_SENT, command.getDeviceId());
                        event.set("id", command.getId());
                        events.put(event, null);
                    }
                    notificationManager.updateEvents(events);
                    return commands.stream().map(QueuedCommand::toCommand).collect(Collectors.toList());
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                }
            });
            long endTime = System.currentTimeMillis();
            
            // Record metrics
            metricsCollector.recordQueuedCommandsRead(deviceId, result.size(), endTime - startTime);
            
            span.setStatus(StatusCode.OK);
            span.setAttribute("commands.count", result.size());
            return result;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Handles command updates from the broadcast service.
     * Implements distributed tracing.
     *
     * @param local Whether the update is local
     * @param deviceId The device ID for which commands were updated
     */
    @Override
    public void updateCommand(boolean local, long deviceId) {
        Span span = tracer.spanBuilder("update-command")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("local", local)
                .setAttribute("deviceId", deviceId)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            if (!local) {
                DeviceSession deviceSession = connectionManager.getDeviceSession(deviceId);
                if (deviceSession != null && deviceSession.supportsLiveCommands()) {
                    for (Command command : readQueuedCommands(deviceId)) {
                        deviceSession.sendCommand(command);
                    }
                }
            }
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }

    /**
     * Updates the notification token for a device.
     * Implements circuit breaker pattern and distributed tracing.
     *
     * @param deviceId The device ID to update the token for
     * @param token The new notification token
     */
    public void updateNotificationToken(long deviceId, String token) {
        Span span = tracer.spanBuilder("update-notification-token")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("deviceId", deviceId)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            circuitBreaker.executeRunnable(CIRCUIT_BREAKER_NAME, () -> {
                var key = new Object();
                try {
                    cacheManager.addDevice(deviceId, key);
                    Device device = cacheManager.getObject(Device.class, deviceId);
                    device.set("notificationTokens", token);
                    storage.updateObject(Device.class, new Request(
                            new Columns.Include("attributes"),
                            new Condition.Equals("id", deviceId)));
                    cacheManager.invalidateObject(true, Device.class, deviceId, ObjectOperation.UPDATE);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    cacheManager.removeDevice(deviceId, key);
                }
            });
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}