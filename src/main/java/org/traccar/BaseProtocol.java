/*
 * Copyright 2015 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.string.StringEncoder;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongCounter;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import org.traccar.helper.DataConverter;
import org.traccar.model.Command;
import org.traccar.sms.SmsManager;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageBrokerClient;

import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public abstract class BaseProtocol implements Protocol {

    private final String name;
    private final Set<String> supportedDataCommands = new HashSet<>();
    private final Set<String> supportedTextCommands = new HashSet<>();
    private final Set<String> supportedPushCommands = new HashSet<>();
    private final List<TrackerConnector> connectorList = new LinkedList<>();

    private SmsManager smsManager;
    private StringProtocolEncoder textCommandEncoder = null;
    
    // Service discovery integration
    private ServiceDiscoveryManager serviceDiscoveryManager;
    
    // Message broker integration
    private MessageBrokerClient messageBrokerClient;
    
    // Circuit breaker integration
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private CircuitBreaker commandCircuitBreaker;
    
    // OpenTelemetry integration
    private Tracer tracer;
    private Meter meter;
    private LongCounter commandSuccessCounter;
    private LongCounter commandFailureCounter;
    
    // TextMapSetter for propagating trace context
    private static final TextMapSetter<Map<String, String>> SETTER = new TextMapSetter<Map<String, String>>() {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            carrier.put(key, value);
        }
    };

    public static String nameFromClass(Class<?> clazz) {
        String className = clazz.getSimpleName();
        return className.substring(0, className.length() - 8).toLowerCase();
    }

    public BaseProtocol() {
        name = nameFromClass(getClass());
    }

    @Inject
    public void setSmsManager(@Nullable SmsManager smsManager) {
        this.smsManager = smsManager;
    }
    
    @Inject
    public void setServiceDiscoveryManager(@Nullable ServiceDiscoveryManager serviceDiscoveryManager) {
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        if (serviceDiscoveryManager != null) {
            registerWithServiceDiscovery();
        }
    }
    
    @Inject
    public void setMessageBrokerClient(@Nullable MessageBrokerClient messageBrokerClient) {
        this.messageBrokerClient = messageBrokerClient;
    }
    
    @Inject
    public void setCircuitBreakerRegistry(@Nullable CircuitBreakerRegistry circuitBreakerRegistry) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        if (circuitBreakerRegistry != null) {
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
            
            commandCircuitBreaker = circuitBreakerRegistry.circuitBreaker(name + "-command", config);
            commandCircuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    // Log state transition for monitoring
                    System.out.println("Circuit breaker " + name + "-command state changed from " 
                        + event.getStateTransition().getFromState() + " to " 
                        + event.getStateTransition().getToState());
                });
        }
    }
    
    @Inject
    public void setTracer(@Nullable Tracer tracer) {
        this.tracer = tracer;
    }
    
    @Inject
    public void setMeter(@Nullable Meter meter) {
        this.meter = meter;
        if (meter != null) {
            commandSuccessCounter = meter.counterBuilder(name + ".command.success")
                .setDescription("Number of successful commands")
                .build();
            
            commandFailureCounter = meter.counterBuilder(name + ".command.failure")
                .setDescription("Number of failed commands")
                .build();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    protected void addServer(TrackerServer server) {
        connectorList.add(server);
    }

    protected void addClient(TrackerClient client) {
        connectorList.add(client);
    }

    @Override
    public Collection<TrackerConnector> getConnectorList() {
        return connectorList;
    }

    public void setSupportedDataCommands(String... commands) {
        supportedDataCommands.addAll(Arrays.asList(commands));
    }

    public void setSupportedTextCommands(String... commands) {
        supportedTextCommands.addAll(Arrays.asList(commands));
    }

    public void setSupportedPushCommands(String... commands) {
        supportedPushCommands.addAll(Arrays.asList(commands));
    }

    @Override
    public Collection<String> getSupportedDataCommands() {
        Set<String> commands = new HashSet<>(supportedDataCommands);
        commands.add(Command.TYPE_CUSTOM);
        return commands;
    }

    @Override
    public Collection<String> getSupportedTextCommands() {
        Set<String> commands = new HashSet<>(supportedTextCommands);
        commands.add(Command.TYPE_CUSTOM);
        return commands;
    }

    @Override
    public Set<String> getSupportedPushCommands() {
        return supportedPushCommands;
    }
    
    /**
     * Register this protocol with the service discovery system
     */
    private void registerWithServiceDiscovery() {
        if (serviceDiscoveryManager != null) {
            Map<String, String> metadata = new HashMap<>();
            metadata.put("protocol", name);
            metadata.put("supportedDataCommands", String.join(",", getSupportedDataCommands()));
            metadata.put("supportedTextCommands", String.join(",", getSupportedTextCommands()));
            metadata.put("supportedPushCommands", String.join(",", getSupportedPushCommands()));
            
            serviceDiscoveryManager.registerService(name, metadata);
        }
    }

    @Override
    public void sendDataCommand(Channel channel, SocketAddress remoteAddress, Command command) {
        // Create span for distributed tracing
        Span span = null;
        if (tracer != null) {
            span = tracer.spanBuilder("sendDataCommand." + command.getType())
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("protocol", name)
                .setAttribute("command.type", command.getType())
                .setAttribute("command.deviceId", String.valueOf(command.getDeviceId()))
                .startSpan();
        }
        
        try {
            // Check if direct communication is possible
            if (channel != null && channel.isActive()) {
                // Use circuit breaker pattern for resilience
                if (commandCircuitBreaker != null) {
                    Supplier<Void> commandSupplier = () -> {
                        executeDataCommand(channel, remoteAddress, command);
                        return null;
                    };
                    
                    try {
                        commandCircuitBreaker.executeSupplier(commandSupplier);
                        if (commandSuccessCounter != null) {
                            commandSuccessCounter.add(1);
                        }
                    } catch (Exception e) {
                        if (commandFailureCounter != null) {
                            commandFailureCounter.add(1);
                        }
                        // If direct communication fails, try message broker if available
                        if (messageBrokerClient != null) {
                            sendViaMessageBroker(command, span);
                        } else {
                            throw e;
                        }
                    }
                } else {
                    // No circuit breaker available, execute directly
                    executeDataCommand(channel, remoteAddress, command);
                    if (commandSuccessCounter != null) {
                        commandSuccessCounter.add(1);
                    }
                }
            } else if (messageBrokerClient != null) {
                // No direct channel available, use message broker
                sendViaMessageBroker(command, span);
            } else {
                throw new RuntimeException("No active channel or message broker available for command: " + command.getType());
            }
        } catch (Exception e) {
            if (commandFailureCounter != null) {
                commandFailureCounter.add(1);
            }
            if (span != null) {
                span.recordException(e);
            }
            throw e;
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }
    
    /**
     * Execute a data command directly through the channel
     */
    private void executeDataCommand(Channel channel, SocketAddress remoteAddress, Command command) {
        if (supportedDataCommands.contains(command.getType())) {
            channel.writeAndFlush(new NetworkMessage(command, remoteAddress));
        } else if (command.getType().equals(Command.TYPE_CUSTOM)) {
            String data = command.getString(Command.KEY_DATA);
            if (BasePipelineFactory.getHandler(channel.pipeline(), StringEncoder.class) != null) {
                channel.writeAndFlush(new NetworkMessage(
                        data.replace("\\r", "\r").replace("\\n", "\n"), remoteAddress));
            } else {
                ByteBuf buf = Unpooled.wrappedBuffer(DataConverter.parseHex(data));
                channel.writeAndFlush(new NetworkMessage(buf, remoteAddress));
            }
        } else {
            throw new RuntimeException("Command " + command.getType() + " is not supported in protocol " + getName());
        }
    }
    
    /**
     * Send a command via the message broker
     */
    private void sendViaMessageBroker(Command command, Span parentSpan) {
        if (messageBrokerClient == null) {
            throw new RuntimeException("Message broker client not available");
        }
        
        // Create a map for propagating trace context
        Map<String, String> headers = new HashMap<>();
        
        // Inject trace context if available
        if (parentSpan != null && tracer != null) {
            tracer.getOpenTelemetry().getPropagators().getTextMapPropagator()
                .inject(Context.current().with(parentSpan), headers, SETTER);
        }
        
        // Add command metadata
        headers.put("protocol", name);
        headers.put("commandType", command.getType());
        
        // Send command to the message broker
        CompletableFuture<Void> future = messageBrokerClient.sendCommand(command, headers);
        
        // Handle completion
        future.whenComplete((result, exception) -> {
            if (exception != null) {
                if (commandFailureCounter != null) {
                    commandFailureCounter.add(1);
                }
                System.err.println("Failed to send command via message broker: " + exception.getMessage());
            } else {
                if (commandSuccessCounter != null) {
                    commandSuccessCounter.add(1);
                }
            }
        });
    }

    public void setTextCommandEncoder(StringProtocolEncoder textCommandEncoder) {
        this.textCommandEncoder = textCommandEncoder;
    }

    @Override
    public void sendTextCommand(String destAddress, Command command) throws Exception {
        // Create span for distributed tracing
        Span span = null;
        if (tracer != null) {
            span = tracer.spanBuilder("sendTextCommand." + command.getType())
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("protocol", name)
                .setAttribute("command.type", command.getType())
                .setAttribute("command.deviceId", String.valueOf(command.getDeviceId()))
                .startSpan();
        }
        
        try {
            if (smsManager != null) {
                if (command.getType().equals(Command.TYPE_CUSTOM)) {
                    smsManager.sendMessage(destAddress, command.getString(Command.KEY_DATA), true);
                } else if (supportedTextCommands.contains(command.getType()) && textCommandEncoder != null) {
                    String encodedCommand = (String) textCommandEncoder.encodeCommand(command);
                    if (encodedCommand != null) {
                        smsManager.sendMessage(destAddress, encodedCommand, true);
                    } else {
                        throw new RuntimeException("Failed to encode command");
                    }
                } else {
                    throw new RuntimeException(
                            "Command " + command.getType() + " is not supported in protocol " + getName());
                }
                
                if (commandSuccessCounter != null) {
                    commandSuccessCounter.add(1);
                }
            } else {
                if (commandFailureCounter != null) {
                    commandFailureCounter.add(1);
                }
                throw new RuntimeException("SMS is not enabled");
            }
        } catch (Exception e) {
            if (commandFailureCounter != null) {
                commandFailureCounter.add(1);
            }
            if (span != null) {
                span.recordException(e);
            }
            throw e;
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }

}