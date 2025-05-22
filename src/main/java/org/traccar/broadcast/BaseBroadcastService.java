/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.broadcast;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageSerializer;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.model.BaseModel;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.ObjectOperation;
import org.traccar.model.Permission;
import org.traccar.model.Position;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;

public abstract class BaseBroadcastService implements BroadcastService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseBroadcastService.class);
    
    private final Set<BroadcastInterface> listeners = new HashSet<>();
    
    @Inject
    private MessageBrokerManager messageBrokerManager;
    
    @Inject
    private MessageSerializer messageSerializer;
    
    @Inject
    private ServiceRegistry serviceRegistry;
    
    @Inject
    private MeterRegistry meterRegistry;
    
    @Inject
    private Tracer tracer;
    
    @Inject
    private TextMapPropagator textMapPropagator;
    
    private final Counter broadcastCounter;
    private final Timer broadcastTimer;
    private final CircuitBreaker circuitBreaker;
    
    protected BaseBroadcastService() {
        // Initialize metrics
        broadcastCounter = Counter.builder("traccar.broadcast.messages")
                .description("Number of broadcast messages sent")
                .register(meterRegistry);
        
        broadcastTimer = Timer.builder("traccar.broadcast.duration")
                .description("Time taken to process broadcast messages")
                .register(meterRegistry);
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker("broadcastService");
    }

    @Override
    public boolean singleInstance() {
        return true;
    }
    
    /**
     * Registers this service with the service discovery mechanism.
     * This method should be called during service initialization.
     */
    protected void registerWithServiceDiscovery() {
        try {
            serviceRegistry.register("broadcast-service", 
                    "http://localhost:8082/api/broadcast", // Replace with actual endpoint
                    "/health"); // Health check endpoint
            LOGGER.info("Broadcast service registered with service discovery");
        } catch (Exception e) {
            LOGGER.error("Failed to register with service discovery", e);
        }
    }
    
    /**
     * Deregisters this service from the service discovery mechanism.
     * This method should be called during service shutdown.
     */
    protected void deregisterFromServiceDiscovery() {
        try {
            serviceRegistry.deregister("broadcast-service");
            LOGGER.info("Broadcast service deregistered from service discovery");
        } catch (Exception e) {
            LOGGER.error("Failed to deregister from service discovery", e);
        }
    }

    @Override
    public void registerListener(BroadcastInterface listener) {
        listeners.add(listener);
        meterRegistry.gauge("traccar.broadcast.listeners", listeners, Set::size);
        LOGGER.debug("Registered broadcast listener: {}", listener.getClass().getName());
    }

    @Override
    public void updateDevice(boolean local, Device device) {
        BroadcastMessage message = new BroadcastMessage();
        message.setDevice(device);
        sendMessage(message);
    }

    @Override
    public void updatePosition(boolean local, Position position) {
        BroadcastMessage message = new BroadcastMessage();
        message.setPosition(position);
        sendMessage(message);
    }

    @Override
    public void updateEvent(boolean local, long userId, Event event) {
        BroadcastMessage message = new BroadcastMessage();
        message.setUserId(userId);
        message.setEvent(event);
        sendMessage(message);
    }

    @Override
    public void updateCommand(boolean local, long deviceId) {
        BroadcastMessage message = new BroadcastMessage();
        message.setCommandDeviceId(deviceId);
        sendMessage(message);
    }

    @Override
    public <T extends BaseModel> void invalidateObject(
            boolean local, Class<T> clazz, long id, ObjectOperation operation) {
        BroadcastMessage message = new BroadcastMessage();
        var invalidateObject = new BroadcastMessage.InvalidateObject();
        invalidateObject.setClazz(Permission.getKey(clazz));
        invalidateObject.setId(id);
        invalidateObject.setOperation(operation);
        message.setInvalidateObject(invalidateObject);
        sendMessage(message);
    }

    @Override
    public synchronized <T1 extends BaseModel, T2 extends BaseModel> void invalidatePermission(
            boolean local, Class<T1> clazz1, long id1, Class<T2> clazz2, long id2, boolean link) {
        BroadcastMessage message = new BroadcastMessage();
        var invalidatePermission = new BroadcastMessage.InvalidatePermission();
        invalidatePermission.setClazz1(Permission.getKey(clazz1));
        invalidatePermission.setId1(id1);
        invalidatePermission.setClazz2(Permission.getKey(clazz2));
        invalidatePermission.setId2(id2);
        invalidatePermission.setLink(link);
        message.setInvalidatePermission(invalidatePermission);
        sendMessage(message);
    }

    /**
     * Send a broadcast message using the appropriate transport mechanism.
     * This method is implemented by concrete subclasses to define the specific
     * transport mechanism (e.g., local, multicast, message broker).
     *
     * @param message the broadcast message to send
     */
    protected abstract void sendMessage(BroadcastMessage message);
    
    /**
     * Sends a message through the message broker with distributed tracing context propagation
     * and circuit breaker protection.
     *
     * @param message the broadcast message to send
     * @param topic the topic to publish the message to
     */
    protected void sendMessageToBroker(BroadcastMessage message, String topic) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("broadcast.send")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("broadcast.topic", topic)
                .startSpan();
        
        try {
            // Increment the counter for metrics
            broadcastCounter.increment();
            
            // Time the operation
            broadcastTimer.record(() -> {
                // Serialize the message
                byte[] serializedMessage = messageSerializer.serialize(message);
                
                // Create message envelope with headers
                MessageEnvelope envelope = new MessageEnvelope();
                envelope.setPayload(serializedMessage);
                
                // Add trace context to headers for distributed tracing
                MessageHeaders headers = new MessageHeaders();
                textMapPropagator.inject(Context.current(), headers, MessageHeaders::put);
                envelope.setHeaders(headers);
                
                // Send the message with circuit breaker protection
                try {
                    circuitBreaker.executeRunnable(() -> {
                        try {
                            messageBrokerManager.publish(topic, envelope);
                        } catch (Exception e) {
                            LOGGER.error("Failed to publish message to broker", e);
                            throw new RuntimeException(e);
                        }
                    });
                } catch (Exception e) {
                    LOGGER.error("Circuit breaker prevented message publishing", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Handles an incoming broadcast message and dispatches it to registered listeners.
     * This method also propagates distributed tracing context and records metrics.
     *
     * @param message the broadcast message to handle
     * @param headers message headers containing tracing context
     * @throws Exception if an error occurs during message handling
     */
    protected void handleMessage(BroadcastMessage message, MessageHeaders headers) throws Exception {
        // Extract and continue the trace context if present
        Context extractedContext = textMapPropagator.extract(Context.current(), headers, MessageHeaders::get);
        Span span = tracer.spanBuilder("broadcast.receive")
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(extractedContext)
                .startSpan();
        
        try {
            // Record metrics for message handling
            broadcastTimer.record(() -> {
                try {
                    dispatchMessageToListeners(message);
                } catch (Exception e) {
                    LOGGER.error("Error dispatching message to listeners", e);
                    span.recordException(e);
                    throw new RuntimeException(e);
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Legacy method for backward compatibility.
     * 
     * @param message the broadcast message to handle
     * @throws Exception if an error occurs during message handling
     */
    protected void handleMessage(BroadcastMessage message) throws Exception {
        handleMessage(message, new MessageHeaders());
    }
    
    /**
     * Dispatches a message to all registered listeners based on the message type.
     *
     * @param message the broadcast message to dispatch
     * @throws Exception if an error occurs during dispatch
     */
    private void dispatchMessageToListeners(BroadcastMessage message) throws Exception {
        // Record metrics for each message type
        if (message.getDevice() != null) {
            meterRegistry.counter("traccar.broadcast.messages", "type", "device").increment();
            listeners.forEach(listener -> listener.updateDevice(false, message.getDevice()));
        } else if (message.getPosition() != null) {
            meterRegistry.counter("traccar.broadcast.messages", "type", "position").increment();
            listeners.forEach(listener -> listener.updatePosition(false, message.getPosition()));
        } else if (message.getUserId() != null && message.getEvent() != null) {
            meterRegistry.counter("traccar.broadcast.messages", "type", "event").increment();
            listeners.forEach(listener -> listener.updateEvent(false, message.getUserId(), message.getEvent()));
        } else if (message.getCommandDeviceId() != null) {
            meterRegistry.counter("traccar.broadcast.messages", "type", "command").increment();
            listeners.forEach(listener -> listener.updateCommand(false, message.getCommandDeviceId()));
        } else if (message.getInvalidateObject() != null) {
            meterRegistry.counter("traccar.broadcast.messages", "type", "invalidateObject").increment();
            var invalidateObject = message.getInvalidateObject();
            for (BroadcastInterface listener : listeners) {
                listener.invalidateObject(
                        false,
                        Permission.getKeyClass(invalidateObject.getClazz()), invalidateObject.getId(),
                        invalidateObject.getOperation());
            }
        } else if (message.getInvalidatePermission() != null) {
            meterRegistry.counter("traccar.broadcast.messages", "type", "invalidatePermission").increment();
            var invalidatePermission = message.getInvalidatePermission();
            for (BroadcastInterface listener : listeners) {
                listener.invalidatePermission(
                        false,
                        Permission.getKeyClass(invalidatePermission.getClazz1()), invalidatePermission.getId1(),
                        Permission.getKeyClass(invalidatePermission.getClazz2()), invalidatePermission.getId2(),
                        invalidatePermission.getLink());
            }
        } else {
            LOGGER.warn("Received broadcast message with no recognizable content");
            meterRegistry.counter("traccar.broadcast.messages", "type", "unknown").increment();
        }
    }

}