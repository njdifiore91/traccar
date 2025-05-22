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
package org.traccar.reports.common;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * Utility for integrating with OpenTelemetry for distributed tracing of report generation activities
 * across service boundaries. This component enables end-to-end visibility into report generation processes,
 * facilitating troubleshooting and performance analysis.
 */
public final class DistributedTracingUtils {

    private static final String TRACER_NAME = "org.traccar.reports";
    private static final String REPORT_TYPE_ATTRIBUTE = "report.type";
    private static final String REPORT_FORMAT_ATTRIBUTE = "report.format";
    private static final String REPORT_FROM_ATTRIBUTE = "report.from";
    private static final String REPORT_TO_ATTRIBUTE = "report.to";
    private static final String REPORT_USER_ID_ATTRIBUTE = "report.userId";
    private static final String REPORT_DEVICE_ID_ATTRIBUTE = "report.deviceId";
    private static final String REPORT_GROUP_ID_ATTRIBUTE = "report.groupId";
    private static final String REPORT_MAIL_ATTRIBUTE = "report.mail";

    private static OpenTelemetry openTelemetry;
    private static Tracer tracer;

    private DistributedTracingUtils() {
        // Utility class
    }

    /**
     * Initialize the OpenTelemetry tracer.
     * This method should be called during application startup.
     *
     * @param openTelemetry The OpenTelemetry instance to use for tracing
     */
    public static void initialize(OpenTelemetry openTelemetry) {
        DistributedTracingUtils.openTelemetry = openTelemetry;
        DistributedTracingUtils.tracer = openTelemetry.getTracer(TRACER_NAME);
    }

    /**
     * Get the OpenTelemetry instance.
     *
     * @return The OpenTelemetry instance
     */
    public static OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }

    /**
     * Get the Tracer instance.
     *
     * @return The Tracer instance
     */
    public static Tracer getTracer() {
        return tracer;
    }

    /**
     * Create a new span for a report generation operation.
     *
     * @param spanName The name of the span
     * @param parent   The parent context, or null for a root span
     * @return The created span
     */
    public static Span createReportSpan(String spanName, Context parent) {
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName)
                .setSpanKind(SpanKind.INTERNAL);

        if (parent != null) {
            spanBuilder.setParent(parent);
        }

        return spanBuilder.startSpan();
    }

    /**
     * Create a new span for a report generation operation with the current context as parent.
     *
     * @param spanName The name of the span
     * @return The created span
     */
    public static Span createReportSpan(String spanName) {
        return createReportSpan(spanName, Context.current());
    }

    /**
     * Add common report attributes to a span.
     *
     * @param span       The span to add attributes to
     * @param reportType The type of report being generated
     * @param from       The start date for the report period
     * @param to         The end date for the report period
     * @param userId     The ID of the user generating the report
     */
    public static void addReportAttributes(Span span, String reportType, Object from, Object to, long userId) {
        span.setAttribute(REPORT_TYPE_ATTRIBUTE, reportType);
        if (from != null) {
            span.setAttribute(REPORT_FROM_ATTRIBUTE, from.toString());
        }
        if (to != null) {
            span.setAttribute(REPORT_TO_ATTRIBUTE, to.toString());
        }
        span.setAttribute(REPORT_USER_ID_ATTRIBUTE, userId);
    }

    /**
     * Add device ID attribute to a span.
     *
     * @param span     The span to add the attribute to
     * @param deviceId The device ID
     */
    public static void addDeviceIdAttribute(Span span, long deviceId) {
        span.setAttribute(REPORT_DEVICE_ID_ATTRIBUTE, deviceId);
    }

    /**
     * Add group ID attribute to a span.
     *
     * @param span    The span to add the attribute to
     * @param groupId The group ID
     */
    public static void addGroupIdAttribute(Span span, long groupId) {
        span.setAttribute(REPORT_GROUP_ID_ATTRIBUTE, groupId);
    }

    /**
     * Add mail attribute to a span.
     *
     * @param span The span to add the attribute to
     * @param mail Whether the report is being sent by email
     */
    public static void addMailAttribute(Span span, boolean mail) {
        span.setAttribute(REPORT_MAIL_ATTRIBUTE, mail);
    }

    /**
     * Add report format attribute to a span.
     *
     * @param span   The span to add the attribute to
     * @param format The report format (e.g., "xlsx", "pdf")
     */
    public static void addFormatAttribute(Span span, String format) {
        span.setAttribute(REPORT_FORMAT_ATTRIBUTE, format);
    }

    /**
     * Add custom attributes to a span.
     *
     * @param span       The span to add attributes to
     * @param attributes The attributes to add
     */
    public static void addAttributes(Span span, Attributes attributes) {
        span.setAllAttributes(attributes);
    }

    /**
     * Add a custom attribute to a span.
     *
     * @param span  The span to add the attribute to
     * @param key   The attribute key
     * @param value The attribute value
     * @param <T>   The type of the attribute value
     */
    public static <T> void addAttribute(Span span, AttributeKey<T> key, T value) {
        span.setAttribute(key, value);
    }

    /**
     * Record an exception in a span.
     *
     * @param span      The span to record the exception in
     * @param throwable The exception to record
     */
    public static void recordException(Span span, Throwable throwable) {
        span.recordException(throwable);
        span.setStatus(StatusCode.ERROR, throwable.getMessage());
    }

    /**
     * Execute a function within the context of a span.
     *
     * @param spanName The name of the span
     * @param function The function to execute
     * @param <T>      The return type of the function
     * @return The result of the function
     */
    public static <T> T withSpan(String spanName, Function<Span, T> function) {
        Span span = createReportSpan(spanName);
        try (Scope scope = span.makeCurrent()) {
            return function.apply(span);
        } catch (Throwable t) {
            recordException(span, t);
            throw t;
        } finally {
            span.end();
        }
    }

    /**
     * Execute a callable within the context of a span.
     *
     * @param spanName The name of the span
     * @param callable The callable to execute
     * @param <T>      The return type of the callable
     * @return The result of the callable
     * @throws Exception If the callable throws an exception
     */
    public static <T> T withSpan(String spanName, Callable<T> callable) throws Exception {
        Span span = createReportSpan(spanName);
        try (Scope scope = span.makeCurrent()) {
            return callable.call();
        } catch (Exception e) {
            recordException(span, e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Execute a runnable within the context of a span.
     *
     * @param spanName The name of the span
     * @param runnable The runnable to execute
     */
    public static void withSpan(String spanName, Runnable runnable) {
        Span span = createReportSpan(spanName);
        try (Scope scope = span.makeCurrent()) {
            runnable.run();
        } catch (Throwable t) {
            recordException(span, t);
            throw t;
        } finally {
            span.end();
        }
    }

    /**
     * Execute a consumer within the context of a span.
     *
     * @param spanName The name of the span
     * @param consumer The consumer to execute
     */
    public static void withSpan(String spanName, BiConsumer<Span, Scope> consumer) {
        Span span = createReportSpan(spanName);
        try (Scope scope = span.makeCurrent()) {
            consumer.accept(span, scope);
        } catch (Throwable t) {
            recordException(span, t);
            throw t;
        } finally {
            span.end();
        }
    }

    /**
     * Extract context from a carrier using a getter.
     *
     * @param carrier The carrier to extract context from
     * @param getter  The getter to use for extraction
     * @param <C>     The type of the carrier
     * @return The extracted context
     */
    public static <C> Context extractContext(C carrier, TextMapGetter<C> getter) {
        return openTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), carrier, getter);
    }

    /**
     * Inject context into a carrier using a setter.
     *
     * @param context The context to inject
     * @param carrier The carrier to inject context into
     * @param setter  The setter to use for injection
     * @param <C>     The type of the carrier
     */
    public static <C> void injectContext(Context context, C carrier, TextMapSetter<C> setter) {
        openTelemetry.getPropagators().getTextMapPropagator().inject(context, carrier, setter);
    }

    /**
     * Create a TextMapGetter for extracting context from a Map.
     *
     * @param <K> The type of the map keys
     * @param <V> The type of the map values
     * @return A TextMapGetter for the map
     */
    public static <K, V> TextMapGetter<Map<K, V>> mapGetter() {
        return new TextMapGetter<>() {
            @Override
            public Iterable<String> keys(Map<K, V> carrier) {
                return (Iterable<String>) carrier.keySet();
            }

            @Override
            public String get(Map<K, V> carrier, String key) {
                return (String) carrier.get(key);
            }
        };
    }

    /**
     * Create a TextMapSetter for injecting context into a Map.
     *
     * @param <K> The type of the map keys
     * @param <V> The type of the map values
     * @return A TextMapSetter for the map
     */
    public static <K, V> TextMapSetter<Map<K, V>> mapSetter() {
        return (carrier, key, value) -> carrier.put((K) key, (V) value);
    }
}