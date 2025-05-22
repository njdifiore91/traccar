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
package org.traccar.notification;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.api.baggage.propagation.BaggageUtil;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Manages distributed tracing context propagation across service boundaries for end-to-end request visibility.
 * Generates and attaches correlation IDs to notification messages, ensuring traceability throughout the
 * notification lifecycle from event generation to delivery. Integrates with OpenTelemetry for standardized tracing.
 */
@Singleton
public class DistributedTracingContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTracingContext.class);

    private static final String CORRELATION_ID_KEY = "correlation-id";
    private static final String TRACE_ID_KEY = "trace-id";
    private static final String SPAN_ID_KEY = "span-id";
    private static final String NOTIFICATION_TYPE_KEY = "notification-type";
    private static final String NOTIFICATION_CHANNEL_KEY = "notification-channel";
    private static final String NOTIFICATION_RECIPIENT_KEY = "notification-recipient";

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final TextMapPropagator propagator;

    /**
     * Constructor for the DistributedTracingContext.
     *
     * @param openTelemetry The OpenTelemetry instance for tracing operations
     */
    @Inject
    public DistributedTracingContext(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer("org.traccar.notification");
        this.propagator = openTelemetry.getPropagators().getTextMapPropagator();
    }

    /**
     * Generates a new correlation ID for a notification flow.
     *
     * @return A unique correlation ID string
     */
    public String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Creates a new span for a notification operation.
     *
     * @param spanName The name of the span
     * @param kind The kind of span (client, server, etc.)
     * @param attributes Additional attributes to add to the span
     * @return The created span
     */
    public Span createSpan(String spanName, SpanKind kind, Attributes attributes) {
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName)
                .setSpanKind(kind);
        
        if (attributes != null) {
            spanBuilder.setAllAttributes(attributes);
        }
        
        return spanBuilder.startSpan();
    }

    /**
     * Creates a new span for a notification operation with the current context as parent.
     *
     * @param spanName The name of the span
     * @param kind The kind of span (client, server, etc.)
     * @param attributes Additional attributes to add to the span
     * @return The created span
     */
    public Span createSpanWithParent(String spanName, SpanKind kind, Attributes attributes) {
        Context currentContext = Context.current();
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName)
                .setSpanKind(kind)
                .setParent(currentContext);
        
        if (attributes != null) {
            spanBuilder.setAllAttributes(attributes);
        }
        
        return spanBuilder.startSpan();
    }

    /**
     * Executes a callable within the context of a span.
     *
     * @param <T> The return type of the callable
     * @param span The span to use as context
     * @param callable The callable to execute
     * @return The result of the callable
     * @throws Exception If the callable throws an exception
     */
    public <T> T withSpan(Span span, Callable<T> callable) throws Exception {
        try (var scope = span.makeCurrent()) {
            return callable.call();
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR);
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Executes a runnable within the context of a span.
     *
     * @param span The span to use as context
     * @param runnable The runnable to execute
     */
    public void withSpan(Span span, Runnable runnable) {
        try (var scope = span.makeCurrent()) {
            runnable.run();
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR);
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Injects the current tracing context into a carrier for propagation across service boundaries.
     *
     * @param carrier The carrier to inject context into
     * @param setter The setter used to set values on the carrier
     * @param <C> The carrier type
     */
    public <C> void injectContext(C carrier, TextMapSetter<C> setter) {
        Context currentContext = Context.current();
        propagator.inject(currentContext, carrier, setter);
    }

    /**
     * Extracts tracing context from a carrier and makes it the current context.
     *
     * @param carrier The carrier containing the context
     * @param getter The getter used to get values from the carrier
     * @param <C> The carrier type
     * @return The extracted context
     */
    public <C> Context extractContext(C carrier, TextMapGetter<C> getter) {
        return propagator.extract(Context.current(), carrier, getter);
    }

    /**
     * Creates a map carrier for context propagation.
     *
     * @param correlationId The correlation ID to include
     * @param notificationType The type of notification
     * @return A map that can be used as a carrier
     */
    public Map<String, String> createContextCarrier(String correlationId, String notificationType) {
        Map<String, String> carrier = new HashMap<>();
        carrier.put(CORRELATION_ID_KEY, correlationId);
        carrier.put(NOTIFICATION_TYPE_KEY, notificationType);
        
        // Add current span context if available
        Span currentSpan = Span.current();
        if (currentSpan != null && currentSpan.getSpanContext().isValid()) {
            SpanContext spanContext = currentSpan.getSpanContext();
            carrier.put(TRACE_ID_KEY, spanContext.getTraceId());
            carrier.put(SPAN_ID_KEY, spanContext.getSpanId());
        }
        
        return carrier;
    }

    /**
     * Adds notification-specific attributes to a span.
     *
     * @param span The span to add attributes to
     * @param notificationType The type of notification
     * @param channel The notification channel (email, SMS, etc.)
     * @param recipient The recipient of the notification
     */
    public void addNotificationAttributes(Span span, String notificationType, String channel, String recipient) {
        span.setAttribute(AttributeKey.stringKey(NOTIFICATION_TYPE_KEY), notificationType);
        
        if (channel != null) {
            span.setAttribute(AttributeKey.stringKey(NOTIFICATION_CHANNEL_KEY), channel);
        }
        
        if (recipient != null) {
            span.setAttribute(AttributeKey.stringKey(NOTIFICATION_RECIPIENT_KEY), recipient);
        }
    }

    /**
     * Adds correlation ID and trace context to the logging MDC for consistent log correlation.
     *
     * @param correlationId The correlation ID to add to logs
     */
    public void setupLoggingContext(String correlationId) {
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        Span currentSpan = Span.current();
        if (currentSpan != null && currentSpan.getSpanContext().isValid()) {
            SpanContext spanContext = currentSpan.getSpanContext();
            MDC.put(TRACE_ID_KEY, spanContext.getTraceId());
            MDC.put(SPAN_ID_KEY, spanContext.getSpanId());
        }
    }

    /**
     * Clears the logging context when processing is complete.
     */
    public void clearLoggingContext() {
        MDC.remove(CORRELATION_ID_KEY);
        MDC.remove(TRACE_ID_KEY);
        MDC.remove(SPAN_ID_KEY);
    }

    /**
     * Adds baggage items to the current context for propagation.
     *
     * @param key The baggage key
     * @param value The baggage value
     * @return The updated context with baggage
     */
    public Context addBaggage(String key, String value) {
        Baggage baggage = Baggage.current();
        baggage = baggage.toBuilder().put(key, value).build();
        return Context.current().with(baggage);
    }

    /**
     * Retrieves a baggage item from the current context.
     *
     * @param key The baggage key
     * @return The baggage value, or null if not found
     */
    public String getBaggage(String key) {
        return Baggage.current().getEntryValue(key);
    }

    /**
     * TextMapSetter implementation for Map carriers.
     */
    public static class MapSetter implements TextMapSetter<Map<String, String>> {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            carrier.put(key, value);
        }
    }

    /**
     * TextMapGetter implementation for Map carriers.
     */
    public static class MapGetter implements TextMapGetter<Map<String, String>> {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    }
}