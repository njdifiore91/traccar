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

import org.traccar.config.Config;
import org.traccar.config.Keys;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

// OpenTelemetry imports
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;

// Resilience4j imports
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

// Micrometer imports
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

// Service discovery imports
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

// MQTT QoS imports
import com.hivemq.client.mqtt.datatypes.MqttQos;

public class PositionForwarderMqtt implements PositionForwarder {

    private final MqttClient mqttClient;
    private final ObjectMapper objectMapper;
    private final String topic;
    
    // OpenTelemetry tracer
    private final Tracer tracer;
    private static final String SPAN_NAME = "mqtt.publish";
    
    // Resilience4j circuit breaker and retry
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    // Micrometer metrics
    private final Counter publishCounter;
    private final Counter errorCounter;
    private final Timer publishTimer;
    
    // QoS configuration
    private final MqttQos defaultQos;
    private final Map<String, MqttQos> messageImportanceQos;
    
    // TextMapSetter for OpenTelemetry context propagation
    private static final TextMapSetter<Map<String, String>> SETTER = new TextMapSetter<Map<String, String>>() {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            if (carrier != null) {
                carrier.put(key, value);
            }
        }
    };

    public PositionForwarderMqtt(final Config config, final ObjectMapper objectMapper, 
                               final MeterRegistry meterRegistry) {
        // Initialize base components
        this.objectMapper = objectMapper;
        this.topic = config.getString(Keys.FORWARD_TOPIC);
        
        // Initialize OpenTelemetry tracer
        this.tracer = GlobalOpenTelemetry.getTracer("org.traccar.forward.mqtt");
        
        // Initialize metrics
        this.publishCounter = meterRegistry.counter("mqtt.publish.count", "type", "position");
        this.errorCounter = meterRegistry.counter("mqtt.publish.errors", "type", "position");
        this.publishTimer = meterRegistry.timer("mqtt.publish.time", "type", "position");
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("mqttPublisher");
        
        // Configure retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(Exception.class)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("mqttPublisher");
        
        // Configure QoS levels
        String qosLevel = config.getString(Keys.FORWARD_MQTT_QOS);
        this.defaultQos = parseQosLevel(qosLevel, MqttQos.AT_LEAST_ONCE);
        
        // Initialize QoS mapping based on message importance
        this.messageImportanceQos = new HashMap<>();
        messageImportanceQos.put("high", MqttQos.EXACTLY_ONCE);  // QoS 2 for high importance
        messageImportanceQos.put("medium", MqttQos.AT_LEAST_ONCE); // QoS 1 for medium importance
        messageImportanceQos.put("low", MqttQos.AT_MOST_ONCE);   // QoS 0 for low importance
        
        // Resolve broker endpoint using service discovery
        String brokerUrl = resolveBrokerEndpoint(config.getString(Keys.FORWARD_URL));
        mqttClient = new MqttClient(brokerUrl);
    }
    
    private MqttQos parseQosLevel(String qosLevel, MqttQos defaultValue) {
        if (qosLevel == null) {
            return defaultValue;
        }
        
        try {
            int level = Integer.parseInt(qosLevel);
            switch (level) {
                case 0:
                    return MqttQos.AT_MOST_ONCE;
                case 1:
                    return MqttQos.AT_LEAST_ONCE;
                case 2:
                    return MqttQos.EXACTLY_ONCE;
                default:
                    return defaultValue;
            }
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
    
    private String resolveBrokerEndpoint(String configuredUrl) {
        // Simple implementation for service discovery
        // In a real implementation, this would use Consul or Kubernetes API
        try {
            URI uri = new URI(configuredUrl);
            String host = uri.getHost();
            
            // If the host is a service name (e.g., mqtt-broker.service), resolve it
            // This is a placeholder for actual service discovery logic
            if (host != null && host.endsWith(".service")) {
                // In a real implementation, this would query service registry
                // For now, we just return the configured URL
                return configuredUrl;
            }
            
            return configuredUrl;
        } catch (Exception e) {
            // If there's an error in resolution, fall back to the configured URL
            return configuredUrl;
        }
    }

    @Override
    public void forward(PositionData positionData, ResultHandler resultHandler) {
        // Create a span for tracing
        Span span = tracer.spanBuilder(SPAN_NAME)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("mqtt.topic", topic)
                .setAttribute("device.id", String.valueOf(positionData.getDevice().getId()))
                .startSpan();
        
        // Create context for propagation
        Context context = Context.current().with(span);
        
        // Prepare headers for context propagation
        Map<String, String> headers = new HashMap<>();
        GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                .inject(context, headers, SETTER);
        
        try (Scope scope = context.makeCurrent()) {
            // Determine QoS level based on message importance
            MqttQos qos = determineQosLevel(positionData);
            span.setAttribute("mqtt.qos", qos.getCode());
            
            // Use circuit breaker and retry pattern with metrics
            Timer.Sample sample = Timer.start();
            
            Supplier<Void> publishSupplier = Retry.decorateSupplier(retry, () -> {
                try {
                    String payload = objectMapper.writeValueAsString(positionData);
                    span.setAttribute("mqtt.payload.size", payload.length());
                    
                    mqttClient.publish(topic, payload, qos, headers, (message, e) -> {
                        if (e == null) {
                            // Success
                            span.setStatus(StatusCode.OK);
                            publishCounter.increment();
                            resultHandler.onResult(true, null);
                        } else {
                            // Error
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            span.recordException(e);
                            errorCounter.increment();
                            resultHandler.onResult(false, e);
                        }
                        span.end();
                        sample.stop(publishTimer);
                    });
                } catch (JsonProcessingException e) {
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    errorCounter.increment();
                    resultHandler.onResult(false, e);
                    span.end();
                    sample.stop(publishTimer);
                }
                return null;
            });
            
            CircuitBreaker.decorateSupplier(circuitBreaker, publishSupplier).get();
            
        } catch (Exception e) {
            // Handle any unexpected exceptions
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            errorCounter.increment();
            resultHandler.onResult(false, e);
            span.end();
        }
    }
    
    private MqttQos determineQosLevel(PositionData positionData) {
        // Determine message importance based on position data
        // This is a simple example - in a real implementation, you might have more complex logic
        String importance = Optional.ofNullable(positionData.getPosition().getAttributes())
                .map(attrs -> (String) attrs.get("importance"))
                .orElse("medium");
        
        return messageImportanceQos.getOrDefault(importance, defaultQos);
    }
}