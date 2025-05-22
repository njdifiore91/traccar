/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.config.Keys;
import org.traccar.database.CommandsManager;
import org.traccar.database.MediaManager;
import org.traccar.database.StatisticsManager;
import org.traccar.helper.UnitsConverter;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.model.Command;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.ConnectionManager;
import org.traccar.session.DeviceSession;
import org.traccar.session.cache.CacheManager;

import jakarta.inject.Inject;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

public abstract class BaseProtocolDecoder extends ExtendedObjectDecoder {

    private static final String PROTOCOL_UNKNOWN = "unknown";

    private final Protocol protocol;

    private CacheManager cacheManager;
    private ConnectionManager connectionManager;
    private StatisticsManager statisticsManager;
    private MediaManager mediaManager;
    private CommandsManager commandsManager;
    
    // New dependencies for microservices architecture
    private MessageBrokerManager messageBrokerManager;
    private OpenTelemetryManager openTelemetryManager;
    private MicrometerMetricsManager metricsManager;
    private ServiceDiscoveryManager serviceDiscoveryManager;
    private CircuitBreakerManager circuitBreakerManager;
    
    // Metrics for decoder operations
    private Counter positionsReceivedCounter;
    private Timer positionProcessingTimer;

    private String modelOverride;

    public BaseProtocolDecoder(Protocol protocol) {
        this.protocol = protocol;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Inject
    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Inject
    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Inject
    public void setStatisticsManager(StatisticsManager statisticsManager) {
        this.statisticsManager = statisticsManager;
    }

    @Inject
    public void setMediaManager(MediaManager mediaManager) {
        this.mediaManager = mediaManager;
    }

    @Inject
    public void setCommandsManager(CommandsManager commandsManager) {
        this.commandsManager = commandsManager;
    }
    
    @Inject
    public void setMessageBrokerManager(MessageBrokerManager messageBrokerManager) {
        this.messageBrokerManager = messageBrokerManager;
    }
    
    @Inject
    public void setOpenTelemetryManager(OpenTelemetryManager openTelemetryManager) {
        this.openTelemetryManager = openTelemetryManager;
    }
    
    @Inject
    public void setMetricsManager(MicrometerMetricsManager metricsManager) {
        this.metricsManager = metricsManager;
        // Initialize metrics
        if (metricsManager != null) {
            String protocolName = getProtocolName();
            positionsReceivedCounter = metricsManager.createCounter(
                    "protocol.positions.received",
                    "Number of positions received by protocol",
                    "protocol", protocolName);
            positionProcessingTimer = metricsManager.createTimer(
                    "protocol.position.processing",
                    "Time taken to process a position",
                    "protocol", protocolName);
        }
    }
    
    @Inject
    public void setServiceDiscoveryManager(ServiceDiscoveryManager serviceDiscoveryManager) {
        this.serviceDiscoveryManager = serviceDiscoveryManager;
    }
    
    @Inject
    public void setCircuitBreakerManager(CircuitBreakerManager circuitBreakerManager) {
        this.circuitBreakerManager = circuitBreakerManager;
    }

    public CommandsManager getCommandsManager() {
        return commandsManager;
    }

    public String writeMediaFile(String uniqueId, ByteBuf buf, String extension) {
        return mediaManager.writeFile(uniqueId, buf, extension);
    }

    public String getProtocolName() {
        return protocol != null ? protocol.getName() : PROTOCOL_UNKNOWN;
    }

    public String getServer(Channel channel, char delimiter) {
        String server = getConfig().getString(Keys.PROTOCOL_SERVER.withPrefix(getProtocolName()));
        if (server == null && channel != null) {
            InetSocketAddress address = (InetSocketAddress) channel.localAddress();
            server = address.getAddress().getHostAddress() + ":" + address.getPort();
        }
        return server != null ? server.replace(':', delimiter) : null;
    }

    protected double convertSpeed(double value, String defaultUnits) {
        return switch (getConfig().getString(getProtocolName() + ".speed", defaultUnits)) {
            case "kmh" -> UnitsConverter.knotsFromKph(value);
            case "mps" -> UnitsConverter.knotsFromMps(value);
            case "mph" -> UnitsConverter.knotsFromMph(value);
            default -> value;
        };
    }

    protected TimeZone getTimeZone(long deviceId) {
        return getTimeZone(deviceId, "UTC");
    }

    protected TimeZone getTimeZone(long deviceId, String defaultTimeZone) {
        String timeZoneName = AttributeUtil.lookup(cacheManager, Keys.DECODER_TIMEZONE, deviceId);
        if (timeZoneName != null) {
            return TimeZone.getTimeZone(timeZoneName);
        } else if (defaultTimeZone != null) {
            return TimeZone.getTimeZone(defaultTimeZone);
        }
        return null;
    }

    public DeviceSession getDeviceSession(Channel channel, SocketAddress remoteAddress, String... uniqueIds) {
        try {
            return connectionManager.getDeviceSession(protocol, channel, remoteAddress, uniqueIds);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void setModelOverride(String modelOverride) {
        this.modelOverride = modelOverride;
    }

    public String getDeviceModel(DeviceSession deviceSession) {
        return modelOverride != null ? modelOverride : deviceSession.getModel();
    }

    public void getLastLocation(Position position, Date deviceTime) {
        if (position.getDeviceId() != 0) {
            position.setOutdated(true);
            if (deviceTime != null) {
                position.setDeviceTime(deviceTime);
            }
        }
    }
    
    /**
     * Publish position to message broker for asynchronous processing
     * 
     * @param position Position to publish
     * @param span Current tracing span for context propagation
     * @return CompletableFuture that completes when the message is published
     */
    protected CompletableFuture<Void> publishPosition(Position position, Span span) {
        if (messageBrokerManager != null) {
            return circuitBreakerManager.executeSupplier(
                "publish-position",
                () -> messageBrokerManager.publishPosition(position, span),
                throwable -> {
                    // Log the error and return a completed future
                    if (openTelemetryManager != null) {
                        openTelemetryManager.recordException(span, throwable);
                    }
                    return CompletableFuture.completedFuture(null);
                });
        }
        return CompletableFuture.completedFuture(null);
    }
    
    /**
     * Publish multiple positions to message broker for asynchronous processing
     * 
     * @param positions Collection of positions to publish
     * @param span Current tracing span for context propagation
     * @return CompletableFuture that completes when all messages are published
     */
    protected CompletableFuture<Void> publishPositions(Collection<Position> positions, Span span) {
        if (messageBrokerManager != null && !positions.isEmpty()) {
            return circuitBreakerManager.executeSupplier(
                "publish-positions",
                () -> messageBrokerManager.publishPositions(positions, span),
                throwable -> {
                    // Log the error and return a completed future
                    if (openTelemetryManager != null) {
                        openTelemetryManager.recordException(span, throwable);
                    }
                    return CompletableFuture.completedFuture(null);
                });
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected void onMessageEvent(
            Channel channel, SocketAddress remoteAddress, Object originalMessage, Object decodedMessage) {
        
        // Create a span for this message event
        Span span = null;
        Scope scope = null;
        Timer.Sample timerSample = null;
        
        try {
            // Start distributed tracing for this message event
            if (openTelemetryManager != null) {
                span = openTelemetryManager.createSpan(
                        "protocol.message.event", 
                        SpanKind.CONSUMER, 
                        Context.current());
                span.setAttribute("protocol.name", getProtocolName());
                if (channel != null && channel.remoteAddress() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
                    span.setAttribute("net.peer.ip", address.getAddress().getHostAddress());
                    span.setAttribute("net.peer.port", address.getPort());
                }
                scope = openTelemetryManager.withSpan(span);
            }
            
            // Start timer for position processing
            if (metricsManager != null) {
                timerSample = Timer.start(metricsManager.getMeterRegistry());
            }
            
            // Track statistics
            if (statisticsManager != null) {
                statisticsManager.registerMessageReceived();
            }
            
            Set<Long> deviceIds = new HashSet<>();
            if (decodedMessage != null) {
                if (decodedMessage instanceof Position position) {
                    deviceIds.add(position.getDeviceId());
                    
                    // Record metrics for position received
                    if (positionsReceivedCounter != null) {
                        positionsReceivedCounter.increment();
                    }
                    
                    // Publish position to message broker for asynchronous processing
                    publishPosition(position, span);
                    
                } else if (decodedMessage instanceof Collection) {
                    Collection<Position> positions = (Collection) decodedMessage;
                    for (Position position : positions) {
                        deviceIds.add(position.getDeviceId());
                    }
                    
                    // Record metrics for positions received
                    if (positionsReceivedCounter != null) {
                        positionsReceivedCounter.increment(positions.size());
                    }
                    
                    // Publish positions to message broker for asynchronous processing
                    publishPositions(positions, span);
                }
            }
            
            if (deviceIds.isEmpty()) {
                DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);
                if (deviceSession != null) {
                    deviceIds.add(deviceSession.getDeviceId());
                }
            }
            
            for (long deviceId : deviceIds) {
                connectionManager.updateDevice(deviceId, Device.STATUS_ONLINE, new Date());
                sendQueuedCommands(channel, remoteAddress, deviceId);
            }
            
            // Record the number of devices in the span
            if (span != null) {
                span.setAttribute("device.count", deviceIds.size());
            }
            
        } finally {
            // Stop timer and record metrics
            if (timerSample != null && positionProcessingTimer != null) {
                timerSample.stop(positionProcessingTimer);
            }
            
            // End the tracing span and scope
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }

    protected void sendQueuedCommands(Channel channel, SocketAddress remoteAddress, long deviceId) {
        Span span = null;
        Scope scope = null;
        
        try {
            // Start distributed tracing for command sending
            if (openTelemetryManager != null) {
                span = openTelemetryManager.createSpan(
                        "protocol.send.queued.commands", 
                        SpanKind.PRODUCER, 
                        Context.current());
                span.setAttribute("protocol.name", getProtocolName());
                span.setAttribute("device.id", deviceId);
                scope = openTelemetryManager.withSpan(span);
            }
            
            for (Command command : commandsManager.readQueuedCommands(deviceId)) {
                // Add command details to span
                if (span != null) {
                    span.setAttribute("command.type", command.getType());
                    span.setAttribute("command.id", command.getId());
                }
                
                // Execute command with circuit breaker protection
                if (circuitBreakerManager != null) {
                    circuitBreakerManager.execute(
                        "send-command",
                        () -> protocol.sendDataCommand(channel, remoteAddress, command),
                        throwable -> {
                            // Log the error
                            if (openTelemetryManager != null) {
                                openTelemetryManager.recordException(span, throwable);
                            }
                        });
                } else {
                    protocol.sendDataCommand(channel, remoteAddress, command);
                }
            }
        } finally {
            // End the tracing span and scope
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }

    @Override
    protected Object handleEmptyMessage(Channel channel, SocketAddress remoteAddress, Object msg) {
        Span span = null;
        Scope scope = null;
        
        try {
            // Start distributed tracing for empty message handling
            if (openTelemetryManager != null) {
                span = openTelemetryManager.createSpan(
                        "protocol.handle.empty.message", 
                        SpanKind.CONSUMER, 
                        Context.current());
                span.setAttribute("protocol.name", getProtocolName());
                if (channel != null && channel.remoteAddress() instanceof InetSocketAddress) {
                    InetSocketAddress address = (InetSocketAddress) channel.remoteAddress();
                    span.setAttribute("net.peer.ip", address.getAddress().getHostAddress());
                    span.setAttribute("net.peer.port", address.getPort());
                }
                scope = openTelemetryManager.withSpan(span);
            }
            
            DeviceSession deviceSession = getDeviceSession(channel, remoteAddress);
            if (getConfig().getBoolean(Keys.DATABASE_SAVE_EMPTY) && deviceSession != null) {
                Position position = new Position(getProtocolName());
                position.setDeviceId(deviceSession.getDeviceId());
                getLastLocation(position, null);
                
                // Record device ID in span
                if (span != null) {
                    span.setAttribute("device.id", deviceSession.getDeviceId());
                }
                
                // Publish empty position to message broker
                publishPosition(position, span);
                
                return position;
            } else {
                return null;
            }
        } finally {
            // End the tracing span and scope
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }

}