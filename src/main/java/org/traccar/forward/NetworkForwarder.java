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
package org.traccar.forward;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Singleton
public class NetworkForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkForwarder.class);
    private static final String CIRCUIT_BREAKER_NAME = "networkForwarder";
    private static final String METRIC_PREFIX = "traccar.network.forwarder";
    private static final int CONNECTION_RETRY_INITIAL_DELAY_MS = 100;
    private static final int CONNECTION_RETRY_MAX_DELAY_MS = 5000;
    private static final int CONNECTION_RETRY_MAX_ATTEMPTS = 5;
    private static final int CONNECTION_CHECK_INTERVAL_SECONDS = 30;

    private InetAddress destination;
    private final DatagramSocket connectionUdp;
    private final Map<InetSocketAddress, Socket> connectionsTcp = new ConcurrentHashMap<>();
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final ScheduledExecutorService scheduler;
    private final ServiceDiscovery serviceDiscovery;
    private final MeterRegistry meterRegistry;
    private final Counter tcpBytesSentCounter;
    private final Counter udpBytesSentCounter;
    private final Counter tcpErrorCounter;
    private final Counter udpErrorCounter;
    private final Counter connectionCreatedCounter;
    private final Counter connectionClosedCounter;
    private final String serviceName;
    private boolean isShutdown = false;

    @Inject
    public NetworkForwarder(
            Config config,
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry,
            Optional<ServiceDiscovery> serviceDiscovery) throws IOException {
        
        this.meterRegistry = meterRegistry;
        this.serviceDiscovery = serviceDiscovery.orElse(null);
        this.serviceName = config.getString(Keys.SERVER_FORWARD);
        
        // Initialize destination address - either direct or via service discovery
        updateDestinationAddress();
        
        // Initialize UDP socket
        connectionUdp = new DatagramSocket();
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, circuitBreakerConfig);
        
        // Initialize OpenTelemetry tracer
        tracer = GlobalOpenTelemetry.getTracer("org.traccar.forward.NetworkForwarder");
        
        // Initialize metrics
        Tags commonTags = Tags.of(Tag.of("service", "network-forwarder"));
        tcpBytesSentCounter = meterRegistry.counter(METRIC_PREFIX + ".tcp.bytes.sent", commonTags);
        udpBytesSentCounter = meterRegistry.counter(METRIC_PREFIX + ".udp.bytes.sent", commonTags);
        tcpErrorCounter = meterRegistry.counter(METRIC_PREFIX + ".tcp.errors", commonTags);
        udpErrorCounter = meterRegistry.counter(METRIC_PREFIX + ".udp.errors", commonTags);
        connectionCreatedCounter = meterRegistry.counter(METRIC_PREFIX + ".connections.created", commonTags);
        connectionClosedCounter = meterRegistry.counter(METRIC_PREFIX + ".connections.closed", commonTags);
        
        // Register gauge for active connections
        meterRegistry.gauge(METRIC_PREFIX + ".connections.active", commonTags, connectionsTcp, Map::size);
        
        // Initialize scheduler for connection health checks and service discovery updates
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(this::checkConnections, CONNECTION_CHECK_INTERVAL_SECONDS, 
                CONNECTION_CHECK_INTERVAL_SECONDS, TimeUnit.SECONDS);
        
        if (serviceDiscovery != null) {
            scheduler.scheduleAtFixedRate(this::updateDestinationAddress, 60, 60, TimeUnit.SECONDS);
        }
    }
    
    private void updateDestinationAddress() {
        try {
            if (serviceDiscovery != null) {
                // Try to resolve service name using service discovery
                String resolvedAddress = serviceDiscovery.resolveService(serviceName);
                if (resolvedAddress != null) {
                    destination = InetAddress.getByName(resolvedAddress);
                    LOGGER.info("Resolved service {} to {}", serviceName, resolvedAddress);
                    return;
                }
            }
            // Fallback to direct resolution if service discovery is not available or fails
            destination = InetAddress.getByName(serviceName);
        } catch (IOException e) {
            LOGGER.warn("Failed to resolve destination address", e);
        }
    }
    
    private void checkConnections() {
        if (isShutdown) {
            return;
        }
        
        connectionsTcp.forEach((source, socket) -> {
            if (socket.isClosed() || !socket.isConnected()) {
                LOGGER.debug("Found closed connection for {}, removing", source);
                disconnect(source);
            }
        });
    }

    /**
     * Forwards data to the destination address.
     * Uses circuit breaker pattern for resilience and includes distributed tracing.
     *
     * @param source   The source address of the connection
     * @param port     The destination port
     * @param datagram True if using UDP, false for TCP
     * @param data     The data to forward
     * @return True if forwarding was successful, false otherwise
     */
    public boolean forward(InetSocketAddress source, int port, boolean datagram, byte[] data) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("network.forward")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("network.protocol", datagram ? "udp" : "tcp")
                .setAttribute("network.peer.port", port)
                .setAttribute("network.peer.address", destination.getHostAddress())
                .setAttribute("source.address", source.getAddress().getHostAddress())
                .setAttribute("source.port", source.getPort())
                .setAttribute("data.size", data.length)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Execute with circuit breaker
            return circuitBreaker.executeSupplier(() -> {
                try {
                    if (datagram) {
                        return forwardUdp(port, data, span);
                    } else {
                        return forwardTcp(source, port, data, span);
                    }
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    if (datagram) {
                        udpErrorCounter.increment();
                    } else {
                        tcpErrorCounter.increment();
                    }
                    LOGGER.warn("Network forwarding error", e);
                    return false;
                }
            });
        } catch (Exception e) {
            // This will be called if the circuit breaker is open
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Circuit breaker is open: " + e.getMessage());
            LOGGER.warn("Circuit breaker prevented network forwarding", e);
            return false;
        } finally {
            span.end();
        }
    }
    
    private boolean forwardUdp(int port, byte[] data, Span parentSpan) throws IOException {
        Span span = tracer.spanBuilder("network.forward.udp")
                .setParent(Context.current())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            DatagramPacket packet = new DatagramPacket(data, data.length, destination, port);
            connectionUdp.send(packet);
            udpBytesSentCounter.increment(data.length);
            span.setAttribute("bytes.sent", data.length);
            return true;
        } catch (IOException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    private boolean forwardTcp(InetSocketAddress source, int port, byte[] data, Span parentSpan) throws IOException {
        Span span = tracer.spanBuilder("network.forward.tcp")
                .setParent(Context.current())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Socket connectionTcp = getOrCreateTcpConnection(source, port, span);
            connectionTcp.getOutputStream().write(data);
            tcpBytesSentCounter.increment(data.length);
            span.setAttribute("bytes.sent", data.length);
            return true;
        } catch (IOException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    private Socket getOrCreateTcpConnection(InetSocketAddress source, int port, Span parentSpan) throws IOException {
        Span span = tracer.spanBuilder("network.connection.get_or_create")
                .setParent(Context.current())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Socket connectionTcp = connectionsTcp.get(source);
            if (connectionTcp == null || connectionTcp.isClosed() || !connectionTcp.isConnected()) {
                span.addEvent("Creating new TCP connection");
                connectionTcp = createTcpConnectionWithRetry(source, port, span);
                connectionsTcp.put(source, connectionTcp);
                connectionCreatedCounter.increment();
            }
            return connectionTcp;
        } finally {
            span.end();
        }
    }
    
    private Socket createTcpConnectionWithRetry(InetSocketAddress source, int port, Span parentSpan) throws IOException {
        Span span = tracer.spanBuilder("network.connection.create_with_retry")
                .setParent(Context.current())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            IOException lastException = null;
            int delay = CONNECTION_RETRY_INITIAL_DELAY_MS;
            
            for (int attempt = 0; attempt < CONNECTION_RETRY_MAX_ATTEMPTS; attempt++) {
                try {
                    span.addEvent("Connection attempt", 
                            Attributes.of(AttributeKey.longKey("attempt"), attempt + 1));
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(destination, port), 5000); // 5 second connect timeout
                    socket.setSoTimeout(30000); // 30 second read timeout
                    socket.setKeepAlive(true);
                    socket.setTcpNoDelay(true);
                    span.addEvent("Connection successful");
                    return socket;
                } catch (IOException e) {
                    lastException = e;
                    span.addEvent("Connection attempt failed", 
                            Attributes.of(AttributeKey.stringKey("error"), e.getMessage()));
                    
                    if (attempt < CONNECTION_RETRY_MAX_ATTEMPTS - 1) {
                        try {
                            Thread.sleep(delay);
                            // Exponential backoff with jitter
                            delay = Math.min(delay * 2, CONNECTION_RETRY_MAX_DELAY_MS);
                            delay += (int) (delay * 0.2 * Math.random()); // Add up to 20% jitter
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Connection retry interrupted", ie);
                        }
                    }
                }
            }
            
            span.setStatus(StatusCode.ERROR, "Failed to create TCP connection after retries");
            throw new IOException("Failed to create TCP connection after " + 
                    CONNECTION_RETRY_MAX_ATTEMPTS + " attempts", lastException);
        } finally {
            span.end();
        }
    }

    /**
     * Disconnects a TCP connection for the given source address.
     * Includes distributed tracing for observability.
     *
     * @param source The source address of the connection to disconnect
     */
    public void disconnect(InetSocketAddress source) {
        Span span = tracer.spanBuilder("network.disconnect")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("source.address", source.getAddress().getHostAddress())
                .setAttribute("source.port", source.getPort())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Socket connectionTcp = connectionsTcp.remove(source);
            if (connectionTcp != null) {
                try {
                    connectionTcp.close();
                    connectionClosedCounter.increment();
                    span.addEvent("Connection closed successfully");
                } catch (IOException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.warn("Connection close error", e);
                }
            } else {
                span.addEvent("No connection found to disconnect");
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Gracefully shuts down all connections and resources.
     * This method is called automatically when the application is shutting down.
     */
    @PreDestroy
    public void shutdown() {
        Span span = tracer.spanBuilder("network.forwarder.shutdown")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            isShutdown = true;
            LOGGER.info("Shutting down NetworkForwarder");
            
            // Shutdown scheduler
            span.addEvent("Shutting down scheduler");
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
                span.recordException(e);
            }
            
            // Close all TCP connections
            span.addEvent("Closing TCP connections", 
                    Attributes.of(AttributeKey.longKey("connection_count"), connectionsTcp.size()));
            for (Map.Entry<InetSocketAddress, Socket> entry : connectionsTcp.entrySet()) {
                try {
                    entry.getValue().close();
                    connectionClosedCounter.increment();
                } catch (IOException e) {
                    LOGGER.warn("Error closing TCP connection during shutdown", e);
                }
            }
            connectionsTcp.clear();
            
            // Close UDP socket
            span.addEvent("Closing UDP socket");
            connectionUdp.close();
            
            LOGGER.info("NetworkForwarder shutdown complete");
        } finally {
            span.end();
        }
    }
    
    /**
     * Imports missing classes to fix compilation errors.
     * These are needed for the OpenTelemetry attributes.
     */
    private static class Attributes {
        public static io.opentelemetry.api.common.Attributes of(io.opentelemetry.api.common.AttributeKey<?> key, Object value) {
            return io.opentelemetry.api.common.Attributes.builder().put(key, value).build();
        }
    }
    
    private static class AttributeKey {
        public static io.opentelemetry.api.common.AttributeKey<Long> longKey(String key) {
            return io.opentelemetry.api.common.AttributeKey.longKey(key);
        }
        
        public static io.opentelemetry.api.common.AttributeKey<String> stringKey(String key) {
            return io.opentelemetry.api.common.AttributeKey.stringKey(key);
        }
    }

}