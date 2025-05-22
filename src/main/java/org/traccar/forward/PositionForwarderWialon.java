/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.Checksum;
import org.traccar.helper.UnitsConverter;
import org.traccar.model.Position;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.zip.Deflater;

public class PositionForwarderWialon implements PositionForwarder {

    private final String version;
    private final boolean useCompression;
    private final ServiceDiscovery serviceDiscovery;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer sendTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    
    // Connection pool for UDP sockets
    private final Map<String, DatagramSocket> socketPool = new ConcurrentHashMap<>();
    private final int maxPoolSize;
    private final long socketIdleTimeout;
    private final ExecutorService executorService;

    public PositionForwarderWialon(
            Config config,
            ExecutorService executorService,
            String version,
            boolean useCompression,
            ServiceDiscovery serviceDiscovery,
            OpenTelemetry openTelemetry,
            MeterRegistry meterRegistry) {
        this.version = version;
        this.useCompression = useCompression;
        this.serviceDiscovery = serviceDiscovery;
        this.executorService = executorService;
        this.tracer = openTelemetry.getTracer("org.traccar.forward.PositionForwarderWialon");
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.sendTimer = Timer.builder("wialon.forward.send")
                .description("Time taken to send position data to Wialon")
                .register(meterRegistry);
        this.successCounter = Counter.builder("wialon.forward.success")
                .description("Number of successful position forwards to Wialon")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("wialon.forward.failure")
                .description("Number of failed position forwards to Wialon")
                .register(meterRegistry);
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("wialonForwarder");
        
        // Configure retry mechanism
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(IOException.class)
                .build();
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("wialonForwarder");
        
        // Configure connection pool
        this.maxPoolSize = config.getInteger(Keys.FORWARD_POOL_SIZE, 10);
        this.socketIdleTimeout = config.getLong(Keys.FORWARD_SOCKET_IDLE_TIMEOUT, 60000); // Default 1 minute
        
        // Start socket cleanup task
        startSocketCleanupTask();
    }
    
    private void startSocketCleanupTask() {
        executorService.submit(() -> {
            while (!executorService.isShutdown()) {
                try {
                    // Cleanup idle sockets every minute
                    TimeUnit.MINUTES.sleep(1);
                    cleanupIdleSockets();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    // Log but continue
                    System.err.println("Error in socket cleanup: " + e.getMessage());
                }
            }
        });
    }
    
    private void cleanupIdleSockets() {
        long currentTime = System.currentTimeMillis();
        socketPool.entrySet().removeIf(entry -> {
            DatagramSocket socket = entry.getValue();
            if (currentTime - socket.getLocalPort() > socketIdleTimeout) {
                socket.close();
                return true;
            }
            return false;
        });
    }
    
    private DatagramSocket getSocket(String endpoint) throws IOException {
        // Check if we have a socket in the pool
        if (socketPool.containsKey(endpoint) && !socketPool.get(endpoint).isClosed()) {
            return socketPool.get(endpoint);
        }
        
        // Create a new socket if pool isn't full
        if (socketPool.size() < maxPoolSize) {
            DatagramSocket socket = new DatagramSocket();
            socketPool.put(endpoint, socket);
            return socket;
        }
        
        // If pool is full, reuse the oldest socket
        DatagramSocket oldestSocket = socketPool.values().iterator().next();
        oldestSocket.close();
        DatagramSocket newSocket = new DatagramSocket();
        socketPool.put(endpoint, newSocket);
        return newSocket;
    }

    @Override
    public void forward(PositionData positionData, ResultHandler resultHandler) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("wialon.forward")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        // Use the span as the current context
        try (Scope scope = span.makeCurrent()) {
            // Add some attributes to the span
            span.setAttribute("device.id", positionData.getDevice().getUniqueId());
            span.setAttribute("position.id", String.valueOf(positionData.getPosition().getId()));
            
            // Use circuit breaker and retry pattern with the forwarding logic
            Supplier<Boolean> forwardingOperation = Retry.decorateSupplier(retry, 
                CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                    return doForward(positionData, span);
                }));
            
            try {
                boolean result = forwardingOperation.get();
                if (result) {
                    successCounter.increment();
                    span.setStatus(StatusCode.OK);
                    resultHandler.onResult(true, null);
                } else {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, "Forward operation failed");
                    resultHandler.onResult(false, new IOException("Forward operation failed"));
                }
            } catch (Exception e) {
                failureCounter.increment();
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                resultHandler.onResult(false, e);
            }
        } finally {
            span.end();
        }
    }
    
    private boolean doForward(PositionData positionData, Span parentSpan) {
        return sendTimer.record(() -> {
            try {
                // Create a child span for the actual sending operation
                Span sendSpan = tracer.spanBuilder("wialon.send")
                        .setParent(Context.current().with(parentSpan))
                        .setSpanKind(SpanKind.CLIENT)
                        .startSpan();
                
                try (Scope scope = sendSpan.makeCurrent()) {
                    // Resolve endpoint using service discovery
                    URI endpoint = resolveEndpoint();
                    sendSpan.setAttribute("endpoint.host", endpoint.getHost());
                    sendSpan.setAttribute("endpoint.port", endpoint.getPort());
                    
                    // Format the payload
                    String payload = formatPayload(positionData);
                    sendSpan.setAttribute("payload.size", payload.length());
                    
                    // Get a socket from the pool
                    DatagramSocket socket = getSocket(endpoint.toString());
                    
                    // Create the packet
                    DatagramPacket packet = createPacket(payload, endpoint);
                    
                    // Send the packet
                    socket.send(packet);
                    
                    return true;
                } catch (Exception e) {
                    sendSpan.recordException(e);
                    sendSpan.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                } finally {
                    sendSpan.end();
                }
            } catch (Exception e) {
                return false;
            }
        });
    }
    
    private URI resolveEndpoint() throws URISyntaxException {
        // Try to resolve endpoint using service discovery
        String endpoint = serviceDiscovery.resolveService("wialon");
        if (endpoint != null) {
            return new URI(endpoint);
        }
        
        // Fallback to configuration
        return new URI(serviceDiscovery.getConfig().getString(Keys.FORWARD_URL));
    }
    
    private String formatPayload(PositionData positionData) {
        DateFormat dateFormat = new SimpleDateFormat("ddMMyy;HHmmss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        Position position = positionData.getPosition();
        String uniqueId = positionData.getDevice().getUniqueId();

        String payload = String.format(
                "%s;%02d%.5f;%s;%03d%.5f;%s;%d;%d;%d;NA;NA;NA;NA;;%s;%s",
                dateFormat.format(position.getFixTime()),
                (int) Math.abs(position.getLatitude()),
                Math.abs(position.getLatitude()) % 1 * 60,
                position.getLatitude() >= 0 ? "N" : "S",
                (int) Math.abs(position.getLongitude()),
                Math.abs(position.getLongitude()) % 1 * 60,
                position.getLongitude() >= 0 ? "E" : "W",
                (int) UnitsConverter.kphFromKnots(position.getSpeed()),
                (int) position.getCourse(),
                (int) position.getAltitude(),
                position.getString(Position.KEY_DRIVER_UNIQUE_ID, "NA"),
                formatAttributes(position.getAttributes()));

        String message;
        if (version.startsWith("2")) {
            payload += ';';
            ByteBuffer payloadBuffer = ByteBuffer.wrap(payload.getBytes(StandardCharsets.US_ASCII));
            int checksum = Checksum.crc16(Checksum.CRC16_IBM, payloadBuffer);
            message = version + ';' + uniqueId + "#D#" + payload + String.format("%04x", checksum) + "\r\n";
        } else {
            message = uniqueId + "#D#" + payload + "\r\n";
        }
        
        return message;
    }
    
    private DatagramPacket createPacket(String message, URI endpoint) throws IOException {
        byte[] buffer = message.getBytes();
        DatagramPacket packet;
        
        if (useCompression) {
            ByteBuf container = compressData(buffer);
            packet = new DatagramPacket(container.array(), container.readableBytes(), 
                    InetAddress.getByName(endpoint.getHost()), endpoint.getPort());
        } else {
            packet = new DatagramPacket(buffer, buffer.length, 
                    InetAddress.getByName(endpoint.getHost()), endpoint.getPort());
        }
        
        return packet;
    }

    public static ByteBuf compressData(byte[] data) {
        ByteBuf container;
        Deflater deflater = new Deflater();
        deflater.setInput(data);
        deflater.finish();

        ByteBuf compressedData = Unpooled.buffer(data.length);
        byte[] tempBuffer = new byte[1024];

        try {
            while (!deflater.finished()) {
                int count = deflater.deflate(tempBuffer);
                compressedData.writeBytes(tempBuffer, 0, count);
            }
            container = Unpooled.buffer(3 + compressedData.readableBytes());
            container.writeByte(0xFF);
            container.writeShortLE(compressedData.readableBytes());
            container.writeBytes(compressedData);
        } finally {
            deflater.end();
            compressedData.release();
        }

        return container;
    }

    public static String formatAttributes(Map<String, Object> attributes) {
        if (attributes.isEmpty()) {
            return "NA";
        }
        return attributes.entrySet().stream()
                .map(entry -> {
                    Object value = entry.getValue();
                    int type;
                    if (value instanceof Double || value instanceof Float) {
                        type = 2;
                    } else if (value instanceof Number) {
                        type = 1;
                    } else {
                        type = 3;
                    }
                    return entry.getKey() + ":" + type + ":" + value;
                })
                .collect(Collectors.joining(","));
    }

}