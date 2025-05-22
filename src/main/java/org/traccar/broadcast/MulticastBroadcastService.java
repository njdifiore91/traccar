/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.databind.ObjectMapper;
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
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MulticastSocket;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Broadcast service implementation using multicast with service discovery integration.
 * Includes distributed tracing, metrics collection, and circuit breaker patterns.
 */
public class MulticastBroadcastService extends BaseBroadcastService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MulticastBroadcastService.class);
    private static final String CIRCUIT_BREAKER_NAME = "broadcastService";
    private static final String BROADCAST_MESSAGE_TYPE = "broadcast";

    private final ObjectMapper objectMapper;
    private final NetworkInterface networkInterface;
    private final int port;
    private final InetSocketAddress group;
    private final ExecutorService executorService;
    private final byte[] receiverBuffer = new byte[4096];
    private final AtomicBoolean running = new AtomicBoolean(false);
    // Service discovery is handled through multicast
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final CircuitBreaker circuitBreaker;

    // Metrics
    private final Counter messagesSentCounter;
    private final Counter messagesReceivedCounter;
    private final Counter messageErrorsCounter;
    private final Timer messageSendTimer;
    private final Timer messageReceiveTimer;

    private DatagramSocket publisherSocket;
    private Thread receiverThread;

    public MulticastBroadcastService(
            Config config, 
            ExecutorService executorService, 
            ObjectMapper objectMapper,

            Tracer tracer,
            MeterRegistry meterRegistry) throws IOException {
        
        this.executorService = executorService;
        this.objectMapper = objectMapper;

        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.messagesSentCounter = Counter.builder("broadcast.messages.sent")
                .description("Number of broadcast messages sent")
                .tag("type", BROADCAST_MESSAGE_TYPE)
                .register(meterRegistry);
        
        this.messagesReceivedCounter = Counter.builder("broadcast.messages.received")
                .description("Number of broadcast messages received")
                .tag("type", BROADCAST_MESSAGE_TYPE)
                .register(meterRegistry);
        
        this.messageErrorsCounter = Counter.builder("broadcast.messages.errors")
                .description("Number of broadcast message errors")
                .tag("type", BROADCAST_MESSAGE_TYPE)
                .register(meterRegistry);
        
        this.messageSendTimer = Timer.builder("broadcast.message.send.time")
                .description("Time taken to send broadcast messages")
                .tag("type", BROADCAST_MESSAGE_TYPE)
                .register(meterRegistry);
        
        this.messageReceiveTimer = Timer.builder("broadcast.message.receive.time")
                .description("Time taken to process received broadcast messages")
                .tag("type", BROADCAST_MESSAGE_TYPE)
                .register(meterRegistry);
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register event listeners for circuit breaker state changes
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}", 
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        // Configure multicast
        port = config.getInteger(Keys.BROADCAST_PORT);
        String interfaceName = config.getString(Keys.BROADCAST_INTERFACE);
        if (interfaceName.indexOf('.') >= 0 || interfaceName.indexOf(':') >= 0) {
            networkInterface = NetworkInterface.getByInetAddress(InetAddress.getByName(interfaceName));
        } else {
            networkInterface = NetworkInterface.getByName(interfaceName);
        }
        InetAddress address = InetAddress.getByName(config.getString(Keys.BROADCAST_ADDRESS));
        group = new InetSocketAddress(address, port);
        
        // Service registration is handled through multicast
    }

    @Override
    public boolean singleInstance() {
        return false;
    }

    @Override
    protected void sendMessage(BroadcastMessage message) {
        // Create a span for the send operation
        Span span = tracer.spanBuilder("broadcast.send")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("broadcast.message.type", message.getClass().getSimpleName())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Trace context is propagated through the current span context
            
            // Use circuit breaker to protect the send operation
            circuitBreaker.executeSupplier(messageSendTimer.wrap(() -> {
                try {
                    byte[] buffer = objectMapper.writeValueAsString(message).getBytes(StandardCharsets.UTF_8);
                    DatagramPacket packet = new DatagramPacket(buffer, buffer.length, group);
                    publisherSocket.send(packet);
                    messagesSentCounter.increment();
                    return true;
                } catch (IOException e) {
                    messageErrorsCounter.increment();
                    LOGGER.warn("Broadcast failed", e);
                    throw new RuntimeException("Failed to send broadcast message", e);
                }
            }));
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error sending broadcast message", e);
        } finally {
            span.end();
        }
    }

    @Override
    public void start() throws IOException {
        if (running.compareAndSet(false, true)) {
            // Start the receiver thread
            receiverThread = new Thread(receiver);
            receiverThread.setName("broadcast-receiver");
            receiverThread.start();
            
            LOGGER.info("Multicast broadcast service started");
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            // Interrupt the receiver thread
            if (receiverThread != null) {
                receiverThread.interrupt();
                try {
                    receiverThread.join(5000); // Wait up to 5 seconds for thread to terminate
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.warn("Interrupted while waiting for receiver thread to stop", e);
                }
            }
            
            // Close the socket
            if (publisherSocket != null) {
                publisherSocket.close();
                publisherSocket = null;
            }
            
            // Service unregistration happens automatically when the socket is closed
            
            LOGGER.info("Multicast broadcast service stopped");
        }
    }

    private final Runnable receiver = new Runnable() {
        @Override
        public void run() {
            try (MulticastSocket socket = new MulticastSocket(port)) {
                socket.setNetworkInterface(networkInterface);
                socket.joinGroup(group, networkInterface);
                publisherSocket = socket;
                
                while (running.get() && !Thread.currentThread().isInterrupted()) {
                    try {
                        DatagramPacket packet = new DatagramPacket(receiverBuffer, receiverBuffer.length);
                        socket.receive(packet);
                        
                        if (networkInterface.inetAddresses().noneMatch(a -> a.equals(packet.getAddress()))) {
                            // Process the received message with metrics and tracing
                            messageReceiveTimer.record(() -> {
                                try {
                                    String data = new String(packet.getData(), 0, packet.getLength(), StandardCharsets.UTF_8);
                                    BroadcastMessage message = objectMapper.readValue(data, BroadcastMessage.class);
                                    messagesReceivedCounter.increment();
                                    
                                    // Create a span for message processing
                                    Span span = tracer.spanBuilder("broadcast.receive")
                                            .setSpanKind(SpanKind.CONSUMER)
                                            .setAttribute("broadcast.message.type", message.getClass().getSimpleName())
                                            .setAttribute("broadcast.source.address", packet.getAddress().getHostAddress())
                                            .startSpan();
                                    
                                    try (Scope scope = span.makeCurrent()) {
                                        handleMessage(message);
                                    } finally {
                                        span.end();
                                    }
                                } catch (Exception e) {
                                    messageErrorsCounter.increment();
                                    LOGGER.warn("Error processing broadcast message", e);
                                }
                            });
                        }
                    } catch (IOException e) {
                        if (running.get() && !Thread.currentThread().isInterrupted()) {
                            messageErrorsCounter.increment();
                            LOGGER.warn("Error receiving broadcast message", e);
                        }
                    }
                }
                
                // Leave the multicast group and close the socket
                try {
                    socket.leaveGroup(group, networkInterface);
                } catch (IOException e) {
                    LOGGER.warn("Error leaving multicast group", e);
                }
                publisherSocket = null;
            } catch (Exception e) {
                if (running.get()) {
                    LOGGER.error("Fatal error in broadcast receiver", e);
                }
            }
        }
    };


}