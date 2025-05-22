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

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.decorators.Decorators;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;

/**
 * MQTT client implementation with distributed tracing, metrics collection,
 * and circuit breaker pattern for resilient connection handling.
 */
public class MqttClient {

    private static final String TRACER_NAME = "org.traccar.forward.MqttClient";
    private static final String CIRCUIT_BREAKER_NAME = "mqttClient";
    
    private final Mqtt5AsyncClient client;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    
    // Metrics
    private final Counter publishCounter;
    private final Counter publishErrorCounter;
    private final Timer publishTimer;
    private final Counter connectCounter;
    private final Counter connectErrorCounter;
    
    // TextMapSetter for propagating trace context via MQTT user properties
    private static final TextMapSetter<Mqtt5PublishBuilder> MQTT_SETTER = 
            (carrier, key, value) -> {
                if (carrier != null) {
                    carrier.userProperty(key, value);
                }
            };

    /**
     * Creates a new MQTT client with the specified URL and metrics registry.
     * 
     * @param url MQTT broker URL
     * @param meterRegistry Metrics registry for collecting operational metrics
     */
    public MqttClient(String url, MeterRegistry meterRegistry) {
        // Initialize OpenTelemetry tracer
        this.tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
        
        // Initialize metrics
        this.publishCounter = meterRegistry.counter("mqtt.publish.count", "client", "traccar");
        this.publishErrorCounter = meterRegistry.counter("mqtt.publish.error.count", "client", "traccar");
        this.publishTimer = meterRegistry.timer("mqtt.publish.time", "client", "traccar");
        this.connectCounter = meterRegistry.counter("mqtt.connect.count", "client", "traccar");
        this.connectErrorCounter = meterRegistry.counter("mqtt.connect.error.count", "client", "traccar");
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new RuntimeException("Invalid MQTT URL: " + url, e);
        }

        Mqtt5SimpleAuth simpleAuth = this.getSimpleAuth(uri);

        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : 1883; // Default to 1883 if port not specified
        
        // Create a unique client identifier with a prefix for better identification in broker logs
        String clientId = "traccar-" + UUID.randomUUID();
        
        Mqtt5ClientBuilder builder = Mqtt5Client.builder()
                .identifier(clientId)
                .serverHost(host)
                .serverPort(port)
                .simpleAuth(simpleAuth)
                .automaticReconnectWithDefaultConfig();
        
        // Configure TLS if using secure connection
        if ("ssl".equals(uri.getScheme()) || "mqtts".equals(uri.getScheme())) {
            builder.sslWithDefaultConfig();
        }

        client = builder.buildAsync();
        
        // Connect with circuit breaker and metrics
        Span span = tracer.spanBuilder("mqtt.connect")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("mqtt.broker.url", url);
            span.setAttribute("mqtt.client.id", clientId);
            
            connectCounter.increment();
            
            Supplier<CompletableFuture<Void>> connectSupplier = () -> 
                client.connectWith()
                    .cleanStart(true)
                    .keepAlive(60)
                    .send();
            
            Decorators.ofSupplier(connectSupplier)
                .withCircuitBreaker(circuitBreaker)
                .get()
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        connectErrorCounter.increment();
                        span.recordException(throwable);
                        span.setStatus(StatusCode.ERROR, "Failed to connect to MQTT broker: " + throwable.getMessage());
                        throw new RuntimeException("Failed to connect to MQTT broker", throwable);
                    } else {
                        span.setStatus(StatusCode.OK);
                    }
                });
        } finally {
            span.end();
        }
    }

    /**
     * Extracts authentication information from the URI.
     * 
     * @param uri MQTT broker URI
     * @return MQTT simple authentication object or null if no auth info provided
     */
    private Mqtt5SimpleAuth getSimpleAuth(URI uri) {
        String userInfo = uri.getUserInfo();
        Mqtt5SimpleAuth simpleAuth = null;
        if (userInfo != null) {
            int delimiter = userInfo.indexOf(':');
            if (delimiter == -1) {
                throw new IllegalArgumentException("Wrong MQTT credentials. Should be in format \"username:password\"");
            } else {
                simpleAuth = Mqtt5SimpleAuth.builder()
                        .username(userInfo.substring(0, delimiter++))
                        .password(userInfo.substring(delimiter).getBytes())
                        .build();
            }
        }
        return simpleAuth;
    }

    /**
     * Publishes a message to the specified topic with distributed tracing and circuit breaker protection.
     * 
     * @param pubTopic Topic to publish to
     * @param payload Message payload
     * @param whenComplete Callback for handling the publish result
     */
    public void publish(
            String pubTopic, String payload, BiConsumer<? super Mqtt5PublishResult, ? super Throwable> whenComplete) {
        
        // Create a span for the publish operation
        Span span = tracer.spanBuilder("mqtt.publish")
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("mqtt.topic", pubTopic);
            span.setAttribute("mqtt.payload.size", payload.getBytes().length);
            
            // Track metrics
            publishCounter.increment();
            
            // Use timer to measure publish operation duration
            publishTimer.record(() -> {
                // Create a publish builder to inject tracing headers
                Mqtt5PublishBuilder publishBuilder = new Mqtt5PublishBuilder(pubTopic, payload);
                
                // Inject the current context into the MQTT message user properties
                GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                        .inject(Context.current(), publishBuilder, MQTT_SETTER);
                
                // Use circuit breaker to protect against broker failures
                Supplier<CompletableFuture<Mqtt5PublishResult>> publishSupplier = () -> 
                    client.publishWith()
                        .topic(pubTopic)
                        .qos(MqttQos.AT_LEAST_ONCE)
                        .payload(payload.getBytes())
                        .userProperties(publishBuilder.getUserProperties())
                        .send();
                
                Decorators.ofSupplier(publishSupplier)
                    .withCircuitBreaker(circuitBreaker)
                    .get()
                    .whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            publishErrorCounter.increment();
                            span.recordException(throwable);
                            span.setStatus(StatusCode.ERROR, "Failed to publish message: " + throwable.getMessage());
                        } else {
                            span.setStatus(StatusCode.OK);
                        }
                        whenComplete.accept(result, throwable);
                    });
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Gracefully disconnects from the MQTT broker.
     */
    public void disconnect() {
        Span span = tracer.spanBuilder("mqtt.disconnect")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            client.disconnectWith()
                .sendWith()
                .whenComplete((result, throwable) -> {
                    if (throwable != null) {
                        span.recordException(throwable);
                        span.setStatus(StatusCode.ERROR, "Failed to disconnect: " + throwable.getMessage());
                    } else {
                        span.setStatus(StatusCode.OK);
                    }
                });
        } finally {
            span.end();
        }
    }
    
    /**
     * Helper class to build MQTT publish messages with trace context.
     */
    private static class Mqtt5PublishBuilder {
        private final String topic;
        private final String payload;
        private final java.util.List<com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty> userProperties;
        
        public Mqtt5PublishBuilder(String topic, String payload) {
            this.topic = topic;
            this.payload = payload;
            this.userProperties = new java.util.ArrayList<>();
        }
        
        public void userProperty(String key, String value) {
            userProperties.add(com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty.of(key, value));
        }
        
        public java.util.List<com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty> getUserProperties() {
            return userProperties;
        }
    }
}