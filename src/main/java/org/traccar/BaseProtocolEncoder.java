/*
 * Copyright 2015 - 2022 Anton Tananaev (anton@traccar.org)
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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.helper.NetworkUtil;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.model.Command;
import org.traccar.model.Device;
import org.traccar.session.cache.CacheManager;

import jakarta.inject.Inject;
import jakarta.inject.Named;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.DeviceResolver;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import org.traccar.messaging.CommandSubscriber;
import org.traccar.messaging.MessageBrokerClient;

/**
 * Base protocol encoder that provides common functionality for all protocol encoders.
 * Handles command encoding, device resolution, metrics collection, distributed tracing,
 * and circuit breaker patterns for device communication.
 */
public abstract class BaseProtocolEncoder extends ChannelOutboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseProtocolEncoder.class);

    private static final String PROTOCOL_UNKNOWN = "unknown";

    private final Protocol protocol;

    private CacheManager cacheManager;
    
    // Service discovery for device resolution
    private DeviceResolver deviceResolver;
    
    // Message broker for command subscription
    private MessageBrokerClient messageBrokerClient;
    private CommandSubscriber commandSubscriber;
    
    // Distributed tracing
    private Tracer tracer;
    
    // Metrics collection
    private MeterRegistry meterRegistry;
    private Timer commandEncodingTimer;
    private Counter commandSuccessCounter;
    private Counter commandFailureCounter;
    private Map<String, Counter> commandTypeCounter = new ConcurrentHashMap<>();
    
    // Circuit breaker for device communication
    private CircuitBreaker deviceCommunicationCircuitBreaker;

    private String modelOverride;

    /**
     * Constructs a new BaseProtocolEncoder with the specified protocol.
     *
     * @param protocol The protocol implementation
     */
    public BaseProtocolEncoder(Protocol protocol) {
        this.protocol = protocol;
    }

    /**
     * Gets the cache manager used for device information.
     *
     * @return The cache manager instance
     */
    public CacheManager getCacheManager() {
        return cacheManager;
    }

    /**
     * Sets the cache manager for device information.
     *
     * @param cacheManager The cache manager to use
     */
    @Inject
    public void setCacheManager(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }
    
    /**
     * Sets the device resolver for service discovery integration.
     *
     * @param deviceResolver The device resolver to use
     */
    @Inject
    public void setDeviceResolver(DeviceResolver deviceResolver) {
        this.deviceResolver = deviceResolver;
    }
    
    /**
     * Sets the message broker client for command subscription.
     *
     * @param messageBrokerClient The message broker client to use
     */
    @Inject
    public void setMessageBrokerClient(MessageBrokerClient messageBrokerClient) {
        this.messageBrokerClient = messageBrokerClient;
    }
    
    /**
     * Sets the command subscriber for handling command messages from the broker.
     *
     * @param commandSubscriber The command subscriber to use
     */
    @Inject
    public void setCommandSubscriber(CommandSubscriber commandSubscriber) {
        this.commandSubscriber = commandSubscriber;
        // Subscribe to command topic for this protocol
        if (protocol != null) {
            commandSubscriber.subscribeToCommands(protocol.getName(), this::handleBrokeredCommand);
        }
    }
    
    /**
     * Sets the tracer for distributed tracing.
     *
     * @param tracer The OpenTelemetry tracer to use
     */
    @Inject
    public void setTracer(@Named("commandTracer") Tracer tracer) {
        this.tracer = tracer;
    }
    
    /**
     * Sets the meter registry for metrics collection.
     *
     * @param meterRegistry The Micrometer registry to use
     */
    @Inject
    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        // Initialize metrics
        this.commandEncodingTimer = Timer.builder("traccar.command.encoding.time")
                .description("Time taken to encode commands")
                .tag("protocol", getProtocolName())
                .register(meterRegistry);
        this.commandSuccessCounter = Counter.builder("traccar.command.success")
                .description("Number of successfully encoded commands")
                .tag("protocol", getProtocolName())
                .register(meterRegistry);
        this.commandFailureCounter = Counter.builder("traccar.command.failure")
                .description("Number of failed command encodings")
                .tag("protocol", getProtocolName())
                .register(meterRegistry);
    }
    
    /**
     * Sets the circuit breaker registry for device communication resilience.
     *
     * @param circuitBreakerRegistry The Resilience4j circuit breaker registry
     */
    @Inject
    public void setCircuitBreakerRegistry(CircuitBreakerRegistry circuitBreakerRegistry) {
        this.deviceCommunicationCircuitBreaker = circuitBreakerRegistry.circuitBreaker(
                "deviceCommunication." + getProtocolName());
    }

    /**
     * Gets the protocol name.
     *
     * @return The protocol name or "unknown" if not available
     */
    public String getProtocolName() {
        return protocol != null ? protocol.getName() : PROTOCOL_UNKNOWN;
    }

    /**
     * Gets the unique ID for a device.
     * Uses service discovery if available, falls back to cache manager.
     *
     * @param deviceId The device ID
     * @return The unique ID of the device
     */
    protected String getUniqueId(long deviceId) {
        if (deviceResolver != null) {
            try {
                return deviceResolver.resolveDeviceUniqueId(deviceId);
            } catch (Exception e) {
                LOGGER.warn("Failed to resolve device through service discovery, falling back to cache", e);
            }
        }
        return cacheManager.getObject(Device.class, deviceId).getUniqueId();
    }

    /**
     * Initializes the device password for a command if not already set.
     *
     * @param command The command to initialize the password for
     * @param defaultPassword The default password to use if not found in attributes
     */
    protected void initDevicePassword(Command command, String defaultPassword) {
        if (!command.hasAttribute(Command.KEY_DEVICE_PASSWORD)) {
            String password = AttributeUtil.getDevicePassword(
                    cacheManager, command.getDeviceId(), getProtocolName(), defaultPassword);
            command.set(Command.KEY_DEVICE_PASSWORD, password);
        }
    }

    /**
     * Sets the model override for device model resolution.
     *
     * @param modelOverride The model override to use
     */
    public void setModelOverride(String modelOverride) {
        this.modelOverride = modelOverride;
    }

    /**
     * Gets the device model, using the override if set.
     * Uses service discovery if available, falls back to cache manager.
     *
     * @param deviceId The device ID
     * @return The device model
     */
    public String getDeviceModel(long deviceId) {
        if (modelOverride != null) {
            return modelOverride;
        }
        
        if (deviceResolver != null) {
            try {
                return deviceResolver.resolveDeviceModel(deviceId);
            } catch (Exception e) {
                LOGGER.warn("Failed to resolve device model through service discovery, falling back to cache", e);
            }
        }
        return getCacheManager().getObject(Device.class, deviceId).getModel();
    }
    
    /**
     * Handles a command received from the message broker.
     * Creates a trace context and processes the command.
     *
     * @param command The command to handle
     * @return A CompletableFuture that completes when the command is processed
     */
    protected CompletableFuture<Boolean> handleBrokeredCommand(Command command) {
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        
        // Create a new trace for brokered commands
        Span span = tracer.spanBuilder("command." + command.getType())
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("protocol", getProtocolName())
                .setAttribute("deviceId", command.getDeviceId())
                .setAttribute("commandType", command.getType())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Find a suitable channel for the device
            Channel channel = findChannelByDeviceId(command.getDeviceId());
            if (channel != null) {
                // Execute with circuit breaker
                deviceCommunicationCircuitBreaker.executeRunnable(() -> {
                    try {
                        Object encodedCommand = encodeCommand(channel, command);
                        if (encodedCommand != null) {
                            channel.writeAndFlush(new NetworkMessage(encodedCommand, channel.remoteAddress()));
                            span.addEvent("Command sent successfully");
                            result.complete(true);
                        } else {
                            span.addEvent("Command encoding failed");
                            result.complete(false);
                        }
                    } catch (Exception e) {
                        span.recordException(e);
                        LOGGER.warn("Failed to process brokered command", e);
                        result.completeExceptionally(e);
                    }
                });
            } else {
                span.addEvent("No channel found for device");
                result.complete(false);
            }
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.warn("Error processing brokered command", e);
            result.completeExceptionally(e);
        } finally {
            span.end();
        }
        
        return result;
    }
    
    /**
     * Finds a channel for a device by its ID.
     * This method should be implemented by protocol-specific encoders.
     *
     * @param deviceId The device ID to find a channel for
     * @return The channel if found, null otherwise
     */
    protected Channel findChannelByDeviceId(long deviceId) {
        // Default implementation returns null
        // Protocol-specific implementations should override this
        return null;
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Extract or create trace context
        Span span = null;
        Scope scope = null;
        
        try {
            if (msg instanceof NetworkMessage networkMessage) {
                if (networkMessage.getMessage() instanceof Command command) {
                    // Create a span for command encoding
                    if (tracer != null) {
                        span = tracer.spanBuilder("encode.command." + command.getType())
                                .setSpanKind(SpanKind.PRODUCER)
                                .setAttribute("protocol", getProtocolName())
                                .setAttribute("deviceId", command.getDeviceId())
                                .setAttribute("commandType", command.getType())
                                .startSpan();
                        scope = span.makeCurrent();
                    }
                    
                    // Record command type metric
                    if (meterRegistry != null) {
                        commandTypeCounter.computeIfAbsent(command.getType(), type -> 
                            Counter.builder("traccar.command.type")
                                .description("Number of commands by type")
                                .tag("protocol", getProtocolName())
                                .tag("type", type)
                                .register(meterRegistry)
                        ).increment();
                    }
                    
                    // Time the command encoding operation
                    Timer.Sample sample = null;
                    if (commandEncodingTimer != null) {
                        sample = Timer.start(meterRegistry);
                    }
                    
                    // Execute command encoding with circuit breaker
                    Object encodedCommand;
                    try {
                        if (deviceCommunicationCircuitBreaker != null) {
                            encodedCommand = deviceCommunicationCircuitBreaker.executeSupplier(
                                    () -> encodeCommand(ctx.channel(), command));
                        } else {
                            encodedCommand = encodeCommand(ctx.channel(), command);
                        }
                    } catch (Exception e) {
                        if (span != null) {
                            span.recordException(e);
                        }
                        if (commandFailureCounter != null) {
                            commandFailureCounter.increment();
                        }
                        throw e;
                    }

                    // Log command information
                    StringBuilder s = new StringBuilder();
                    s.append("[").append(NetworkUtil.session(ctx.channel())).append("] ");
                    s.append("id: ").append(getUniqueId(command.getDeviceId())).append(", ");
                    s.append("command type: ").append(command.getType()).append(" ");
                    if (encodedCommand != null) {
                        s.append("sent");
                        if (commandSuccessCounter != null) {
                            commandSuccessCounter.increment();
                        }
                    } else {
                        s.append("not sent");
                        if (commandFailureCounter != null) {
                            commandFailureCounter.increment();
                        }
                    }
                    LOGGER.info(s.toString());
                    
                    // Record timing metric
                    if (sample != null) {
                        sample.stop(commandEncodingTimer);
                    }

                    ctx.write(new NetworkMessage(encodedCommand, networkMessage.getRemoteAddress()), promise);
                    
                    // Publish command result to message broker if available
                    if (messageBrokerClient != null && encodedCommand != null) {
                        try {
                            messageBrokerClient.publishCommandResult(command, true);
                        } catch (Exception e) {
                            LOGGER.warn("Failed to publish command result to message broker", e);
                        }
                    }

                    return;
                }
            }
            super.write(ctx, msg, promise);
        } finally {
            // Close the tracing scope and end the span
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }

    /**
     * Encodes a command for a specific channel.
     * Default implementation calls the channel-agnostic version.
     *
     * @param channel The channel to encode the command for
     * @param command The command to encode
     * @return The encoded command object or null if encoding failed
     */
    protected Object encodeCommand(Channel channel, Command command) {
        return encodeCommand(command);
    }

    /**
     * Encodes a command without channel context.
     * This method should be overridden by protocol-specific implementations.
     *
     * @param command The command to encode
     * @return The encoded command object or null if encoding failed
     */
    protected Object encodeCommand(Command command) {
        return null;
    }

}