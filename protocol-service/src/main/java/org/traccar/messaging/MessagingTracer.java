/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.messaging;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides distributed tracing for messaging operations using OpenTelemetry.
 * Creates spans for message publishing, tracks message flow across services,
 * and includes metadata for correlation.
 */
@Singleton
public class MessagingTracer {

    private static final String INSTRUMENTATION_SCOPE = "org.traccar.messaging";
    private static final String OPERATION_PUBLISH = "publish";
    
    // Common attribute keys for messaging spans
    private static final AttributeKey<String> MESSAGING_SYSTEM = AttributeKey.stringKey("messaging.system");
    private static final AttributeKey<String> MESSAGING_DESTINATION = AttributeKey.stringKey("messaging.destination");
    private static final AttributeKey<String> MESSAGING_DESTINATION_KIND = AttributeKey.stringKey("messaging.destination.kind");
    private static final AttributeKey<String> MESSAGING_MESSAGE_ID = AttributeKey.stringKey("messaging.message.id");
    private static final AttributeKey<String> MESSAGING_OPERATION = AttributeKey.stringKey("messaging.operation");
    private static final AttributeKey<Long> MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES = AttributeKey.longKey("messaging.message.payload.size_bytes");
    
    // Traccar-specific attribute keys
    private static final AttributeKey<Long> DEVICE_ID = AttributeKey.longKey("device.id");
    private static final AttributeKey<String> PROTOCOL = AttributeKey.stringKey("protocol");
    
    private final Tracer tracer;
    private final OpenTelemetry openTelemetry;
    
    /**
     * Header propagation setter for injecting trace context into message headers.
     */
    private static final TextMapSetter<Map<String, String>> SETTER = (carrier, key, value) -> {
        if (carrier != null) {
            carrier.put(key, value);
        }
    };
    
    /**
     * Header propagation getter for extracting trace context from message headers.
     */
    private static final TextMapGetter<Map<String, String>> GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    };

    /**
     * Creates a new MessagingTracer with the provided OpenTelemetry instance.
     *
     * @param openTelemetry The OpenTelemetry instance to use for tracing
     */
    @Inject
    public MessagingTracer(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    }

    /**
     * Creates a span for publishing a message to a topic.
     *
     * @param topic The destination topic
     * @param messageId The unique message identifier
     * @param messagingSystem The messaging system (e.g., "kafka", "rabbitmq")
     * @return A new span for the publish operation
     */
    public Span startPublishSpan(String topic, String messageId, String messagingSystem) {
        SpanBuilder spanBuilder = tracer.spanBuilder(topic + " publish")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(MESSAGING_SYSTEM, messagingSystem)
                .setAttribute(MESSAGING_DESTINATION, topic)
                .setAttribute(MESSAGING_DESTINATION_KIND, "topic")
                .setAttribute(MESSAGING_OPERATION, OPERATION_PUBLISH);
        
        if (messageId != null) {
            spanBuilder.setAttribute(MESSAGING_MESSAGE_ID, messageId);
        }
        
        return spanBuilder.startSpan();
    }

    /**
     * Creates a span for publishing a position message, including device-specific attributes.
     *
     * @param topic The destination topic
     * @param messageId The unique message identifier
     * @param messagingSystem The messaging system (e.g., "kafka", "rabbitmq")
     * @param deviceId The device ID associated with the position
     * @param protocol The protocol used by the device
     * @return A new span for the publish operation with device context
     */
    public Span startPositionPublishSpan(String topic, String messageId, String messagingSystem, 
                                        long deviceId, String protocol) {
        Span span = startPublishSpan(topic, messageId, messagingSystem);
        span.setAttribute(DEVICE_ID, deviceId);
        if (protocol != null) {
            span.setAttribute(PROTOCOL, protocol);
        }
        return span;
    }

    /**
     * Adds payload size information to an existing span.
     *
     * @param span The span to update
     * @param payloadSize The size of the message payload in bytes
     */
    public void setPayloadSize(Span span, long payloadSize) {
        if (span != null && payloadSize > 0) {
            span.setAttribute(MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES, payloadSize);
        }
    }

    /**
     * Records an error in the current span.
     *
     * @param span The span to update
     * @param throwable The error that occurred
     */
    public void recordError(Span span, Throwable throwable) {
        if (span != null && throwable != null) {
            span.recordException(throwable);
            span.setStatus(StatusCode.ERROR, throwable.getMessage());
        }
    }

    /**
     * Injects the current trace context into message headers for propagation.
     *
     * @param headers The message headers map to inject context into
     * @return The updated headers map with trace context
     */
    public Map<String, String> injectTraceContext(Map<String, String> headers) {
        Map<String, String> updatedHeaders = headers != null ? headers : new HashMap<>();
        openTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), updatedHeaders, SETTER);
        return updatedHeaders;
    }

    /**
     * Extracts trace context from message headers.
     *
     * @param headers The message headers containing trace context
     * @return The extracted context
     */
    public Context extractTraceContext(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Context.current();
        }
        return openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), headers, GETTER);
    }

    /**
     * Creates attributes for a messaging span.
     *
     * @param topic The destination topic
     * @param messageId The unique message identifier
     * @param messagingSystem The messaging system (e.g., "kafka", "rabbitmq")
     * @return Attributes for the span
     */
    public Attributes createMessagingAttributes(String topic, String messageId, String messagingSystem) {
        Attributes.Builder attributesBuilder = Attributes.builder()
                .put(MESSAGING_SYSTEM, messagingSystem)
                .put(MESSAGING_DESTINATION, topic)
                .put(MESSAGING_DESTINATION_KIND, "topic")
                .put(MESSAGING_OPERATION, OPERATION_PUBLISH);
        
        if (messageId != null) {
            attributesBuilder.put(MESSAGING_MESSAGE_ID, messageId);
        }
        
        return attributesBuilder.build();
    }
}