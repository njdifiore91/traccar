/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hivemq.client.mqtt.datatypes.MqttQos;

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
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

import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * MQTT event forwarder with distributed tracing, metrics collection,
 * circuit breaker pattern, and service discovery integration.
 */
public class EventForwarderMqtt implements EventForwarder {

    private static final String TRACER_NAME = "org.traccar.forward.EventForwarderMqtt";
    private static final String RETRY_NAME = "eventForwarderMqtt";
    
    private final MqttClient mqttClient;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final Retry retry;
    private final ServiceDiscovery serviceDiscovery;
    
    // Metrics
    private final Counter forwardCounter;
    private final Counter forwardErrorCounter;
    private final Timer forwardTimer;
    
    private final String topic;
    private final Map<String, MqttQos> eventTypeQosMap;
    private final MqttQos defaultQos;

    /**
     * Creates a new MQTT event forwarder with the specified configuration.
     * 
     * @param config Configuration for the forwarder
     * @param objectMapper JSON object mapper for serializing events
     * @param meterRegistry Metrics registry for collecting operational metrics
     * @param serviceDiscovery Service discovery client for broker endpoint resolution
     */
public EventForwarderMqtt(Config config, ObjectMapper objectMapper, 
                         MeterRegistry meterRegistry, ServiceDiscovery serviceDiscovery) {
    this.tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
    this.objectMapper = objectMapper;
    this.serviceDiscovery = serviceDiscovery;
    
    // Initialize metrics
    this.forwardCounter = meterRegistry.counter("event.forward.mqtt.count", "forwarder", "mqtt");
    this.forwardErrorCounter = meterRegistry.counter("event.forward.mqtt.error.count", "forwarder", "mqtt");
    this.forwardTimer = meterRegistry.timer("event.forward.mqtt.time", "forwarder", "mqtt");
    
    // Configure retry mechanism
    RetryConfig retryConfig = RetryConfig.custom()
            .maxAttempts(config.getInteger(Keys.EVENT_FORWARD_RETRY_COUNT, 3))
            .waitDuration(Duration.ofMillis(config.getInteger(Keys.EVENT_FORWARD_RETRY_DELAY, 1000)))
            .exponentialBackoff(Duration.ofMillis(config.getInteger(Keys.EVENT_FORWARD_RETRY_DELAY, 1000)), 2.0)
            .retryExceptions(Exception.class)
            .build();
    
    RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
    this.retry = retryRegistry.retry(RETRY_NAME);
    
    // Get topic from configuration
    this.topic = config.getString(Keys.EVENT_FORWARD_TOPIC);
    
    // Initialize QoS mapping
    this.eventTypeQosMap = new HashMap<>();
    this.defaultQos = parseQosLevel(config.getString(Keys.FORWARD_MQTT_QOS, "1"));
    
    // Configure QoS levels for different event types
    // Format: alarm:2,deviceOnline:0,geofenceEnter:1
    String qosMapping = config.getString("event.forward.mqtt.qosMapping", "");
    if (!qosMapping.isEmpty()) {
        String[] mappings = qosMapping.split(",");
        for (String mapping : mappings) {
            String[] parts = mapping.split(":");
            if (parts.length == 2) {
                eventTypeQosMap.put(parts[0].trim(), parseQosLevel(parts[1].trim()));
            }
        }
    }
    
    // Resolve broker endpoint using service discovery if configured
    String brokerUrl;
    String serviceName = config.getString("event.forward.mqtt.serviceName", "");
    if (!serviceName.isEmpty() && serviceDiscovery != null) {
        brokerUrl = serviceDiscovery.resolveService(serviceName);
        if (brokerUrl == null) {
            brokerUrl = config.getString(Keys.EVENT_FORWARD_URL); // Fallback to configured URL
        }
    } else {
        brokerUrl = config.getString(Keys.EVENT_FORWARD_URL);
    }
    
    // Initialize MQTT client
    mqttClient = new MqttClient(brokerUrl, meterRegistry);
}

    /**
     * Parses a QoS level string into the corresponding MqttQos enum value.
     * 
     * @param qosString QoS level as string ("0", "1", or "2")
     * @return The corresponding MqttQos enum value
     */
    private MqttQos parseQosLevel(String qosString) {
        switch (qosString) {
            case "0":
                return MqttQos.AT_MOST_ONCE;
            case "2":
                return MqttQos.EXACTLY_ONCE;
            case "1":
            default:
                return MqttQos.AT_LEAST_ONCE;
        }
    }

    /**
     * Determines the appropriate QoS level for an event based on its type.
     * 
     * @param eventData The event data to be forwarded
     * @return The appropriate MqttQos level for the event
     */
    private MqttQos getQosForEvent(EventData eventData) {
        if (eventData.getEvent() != null && eventData.getEvent().getType() != null) {
            String eventType = eventData.getEvent().getType();
            return eventTypeQosMap.getOrDefault(eventType, defaultQos);
        }
        return defaultQos;
    }

    @Override
    public void forward(EventData eventData, ResultHandler resultHandler) {
        // Create a span for the forward operation
        Span span = tracer.spanBuilder("mqtt.event.forward")
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Add event metadata to span for tracing
            if (eventData.getEvent() != null) {
                span.setAttribute("event.type", eventData.getEvent().getType() != null ? 
                        eventData.getEvent().getType() : "unknown");
                span.setAttribute("event.id", eventData.getEvent().getId());
                
                if (eventData.getDevice() != null) {
                    span.setAttribute("device.id", eventData.getDevice().getId());
                    span.setAttribute("device.uniqueId", eventData.getDevice().getUniqueId());
                }
            }
            
            // Track metrics
            forwardCounter.increment();
            
            // Determine QoS level for this event
            MqttQos qosLevel = getQosForEvent(eventData);
            span.setAttribute("mqtt.qos", qosLevel.getCode());
            
            // Extract trace context for propagation
            Map<String, String> traceContext = new HashMap<>();
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator()
                    .inject(Context.current(), traceContext, (carrier, key, value) -> carrier.put(key, value));
            
            // Use timer to measure forward operation duration
            forwardTimer.record(() -> {
                try {
                    String payload = objectMapper.writeValueAsString(eventData);
                    span.setAttribute("mqtt.payload.size", payload.getBytes().length);
                    span.setAttribute("mqtt.topic", topic);
                    
                    // Use retry pattern for resilient publishing
                    CompletableFuture<Void> future = Retry.decorateCompletionStage(
                            retry,
                            () -> {
                                CompletableFuture<Void> result = new CompletableFuture<>();
                                mqttClient.publish(topic, payload, (message, e) -> {
                                    if (e == null) {
                                        result.complete(null);
                                    } else {
                                        result.completeExceptionally(e);
                                    }
                                });
                                return result;
                            }
                    ).get();
                    
                    future.whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            forwardErrorCounter.increment();
                            span.recordException(throwable);
                            span.setStatus(StatusCode.ERROR, "Failed to forward event: " + throwable.getMessage());
                            
                            // Create structured error info for the result handler
                            Map<String, String> metadata = new HashMap<>(traceContext);
                            metadata.put("topic", topic);
                            if (eventData.getEvent() != null && eventData.getEvent().getType() != null) {
                                metadata.put("eventType", eventData.getEvent().getType());
                            }
                            
                            ResultHandler.ErrorInfo errorInfo = new ResultHandler.ErrorInfo(
                                    "MQTT_FORWARD_ERROR",
                                    "Failed to forward event via MQTT: " + throwable.getMessage(),
                                    throwable,
                                    metadata);
                            
                            // Call result handler with error info and trace context
                            resultHandler.onResultAsync(false, errorInfo, traceContext);
                        } else {
                            span.setStatus(StatusCode.OK);
                            // Call result handler with success and trace context
                            resultHandler.onResultAsync(true, (Throwable) null, traceContext);
                        }
                    });
                } catch (JsonProcessingException e) {
                    forwardErrorCounter.increment();
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, "Failed to serialize event: " + e.getMessage());
                    
                    // Create structured error info for JSON processing errors
                    Map<String, String> metadata = new HashMap<>(traceContext);
                    metadata.put("errorType", "JSON_SERIALIZATION");
                    
                    ResultHandler.ErrorInfo errorInfo = new ResultHandler.ErrorInfo(
                            "JSON_SERIALIZATION_ERROR",
                            "Failed to serialize event to JSON: " + e.getMessage(),
                            e,
                            metadata);
                    
                    // Call result handler with error info and trace context
                    resultHandler.onResultAsync(false, errorInfo, traceContext);
                }
            });
        } finally {
            span.end();
        }
    }
}