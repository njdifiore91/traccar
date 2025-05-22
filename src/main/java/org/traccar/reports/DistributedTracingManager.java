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
package org.traccar.reports;

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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages distributed tracing for the Reporting Service using OpenTelemetry.
 * This class creates and manages trace spans for report generation processes,
 * propagates trace context across service boundaries, and exports trace data
 * to a collector for visualization and analysis.
 */
@Singleton
public class DistributedTracingManager {

    private static final String INSTRUMENTATION_SCOPE = "org.traccar.reports";
    
    // Common attribute keys for report spans
    private static final AttributeKey<String> REPORT_TYPE = AttributeKey.stringKey("report.type");
    private static final AttributeKey<String> REPORT_FORMAT = AttributeKey.stringKey("report.format");
    private static final AttributeKey<Long> USER_ID = AttributeKey.longKey("user.id");
    private static final AttributeKey<Long> DEVICE_COUNT = AttributeKey.longKey("device.count");
    private static final AttributeKey<Long> GROUP_COUNT = AttributeKey.longKey("group.count");
    private static final AttributeKey<String> TIME_PERIOD = AttributeKey.stringKey("time.period");
    
    private final Tracer tracer;
    private final OpenTelemetry openTelemetry;
    
    // Map to store active spans by operation ID
    private final Map<String, Span> activeSpans = new ConcurrentHashMap<>();

    /**
     * TextMapSetter implementation for injecting trace context into carriers like HTTP headers
     */
    private static final TextMapSetter<Map<String, String>> TEXT_MAP_SETTER = 
            (carrier, key, value) -> {
                if (carrier != null) {
                    carrier.put(key, value);
                }
            };

    /**
     * TextMapGetter implementation for extracting trace context from carriers like HTTP headers
     */
    private static final TextMapGetter<Map<String, String>> TEXT_MAP_GETTER = 
            new TextMapGetter<>() {
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
     * Constructs a new DistributedTracingManager with the provided OpenTelemetry instance.
     *
     * @param openTelemetry The OpenTelemetry instance for creating tracers and propagating context
     */
    @Inject
    public DistributedTracingManager(OpenTelemetry openTelemetry) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    }

    /**
     * Creates a new span for a report generation operation.
     *
     * @param reportType The type of report being generated (e.g., "route", "combined")
     * @param userId The ID of the user requesting the report
     * @param deviceIds Collection of device IDs included in the report
     * @param groupIds Collection of group IDs included in the report
     * @param from Start date for the report period
     * @param to End date for the report period
     * @return A unique operation ID that can be used to reference this span
     */
    public String startReportSpan(
            String reportType,
            long userId,
            Collection<Long> deviceIds,
            Collection<Long> groupIds,
            Date from,
            Date to) {
        
        String operationId = generateOperationId(reportType, userId);
        
        SpanBuilder spanBuilder = tracer.spanBuilder("generate_" + reportType + "_report")
                .setSpanKind(SpanKind.INTERNAL);
        
        Span span = spanBuilder.startSpan();
        
        // Add common attributes to the span
        span.setAttribute(REPORT_TYPE, reportType);
        span.setAttribute(USER_ID, userId);
        span.setAttribute(DEVICE_COUNT, deviceIds != null ? deviceIds.size() : 0);
        span.setAttribute(GROUP_COUNT, groupIds != null ? groupIds.size() : 0);
        span.setAttribute(TIME_PERIOD, from + " to " + to);
        
        // Store the span for later reference
        activeSpans.put(operationId, span);
        
        return operationId;
    }

    /**
     * Creates a new span for a report export operation.
     *
     * @param reportType The type of report being exported
     * @param format The format of the export (e.g., "excel", "pdf")
     * @param userId The ID of the user requesting the export
     * @param deviceIds Collection of device IDs included in the export
     * @param groupIds Collection of group IDs included in the export
     * @param from Start date for the report period
     * @param to End date for the report period
     * @return A unique operation ID that can be used to reference this span
     */
    public String startExportSpan(
            String reportType,
            String format,
            long userId,
            Collection<Long> deviceIds,
            Collection<Long> groupIds,
            Date from,
            Date to) {
        
        String operationId = generateOperationId(reportType + "_export", userId);
        
        SpanBuilder spanBuilder = tracer.spanBuilder("export_" + reportType + "_report")
                .setSpanKind(SpanKind.INTERNAL);
        
        Span span = spanBuilder.startSpan();
        
        // Add common attributes to the span
        span.setAttribute(REPORT_TYPE, reportType);
        span.setAttribute(REPORT_FORMAT, format);
        span.setAttribute(USER_ID, userId);
        span.setAttribute(DEVICE_COUNT, deviceIds != null ? deviceIds.size() : 0);
        span.setAttribute(GROUP_COUNT, groupIds != null ? groupIds.size() : 0);
        span.setAttribute(TIME_PERIOD, from + " to " + to);
        
        // Store the span for later reference
        activeSpans.put(operationId, span);
        
        return operationId;
    }

    /**
     * Adds an attribute to an existing span.
     *
     * @param operationId The operation ID of the span
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addSpanAttribute(String operationId, String key, String value) {
        Span span = activeSpans.get(operationId);
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    /**
     * Adds an attribute to an existing span.
     *
     * @param operationId The operation ID of the span
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addSpanAttribute(String operationId, String key, long value) {
        Span span = activeSpans.get(operationId);
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    /**
     * Adds an attribute to an existing span.
     *
     * @param operationId The operation ID of the span
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addSpanAttribute(String operationId, String key, double value) {
        Span span = activeSpans.get(operationId);
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    /**
     * Adds an attribute to an existing span.
     *
     * @param operationId The operation ID of the span
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addSpanAttribute(String operationId, String key, boolean value) {
        Span span = activeSpans.get(operationId);
        if (span != null) {
            span.setAttribute(key, value);
        }
    }

    /**
     * Adds an event to an existing span.
     *
     * @param operationId The operation ID of the span
     * @param name The name of the event
     * @param attributes The attributes for the event
     */
    public void addSpanEvent(String operationId, String name, Attributes attributes) {
        Span span = activeSpans.get(operationId);
        if (span != null) {
            span.addEvent(name, attributes);
        }
    }

    /**
     * Records an error in an existing span.
     *
     * @param operationId The operation ID of the span
     * @param throwable The error to record
     */
    public void recordError(String operationId, Throwable throwable) {
        Span span = activeSpans.get(operationId);
        if (span != null) {
            span.recordException(throwable);
            span.setStatus(StatusCode.ERROR, throwable.getMessage());
        }
    }

    /**
     * Ends an existing span.
     *
     * @param operationId The operation ID of the span to end
     */
    public void endSpan(String operationId) {
        Span span = activeSpans.remove(operationId);
        if (span != null) {
            span.end();
        }
    }

    /**
     * Creates a child span for a specific operation within a report generation process.
     *
     * @param parentOperationId The operation ID of the parent span
     * @param operationName The name of the operation
     * @return A new operation ID for the child span
     */
    public String startChildSpan(String parentOperationId, String operationName) {
        Span parentSpan = activeSpans.get(parentOperationId);
        if (parentSpan == null) {
            return startSpan(operationName);
        }
        
        String operationId = generateOperationId(operationName, System.currentTimeMillis());
        
        Context parentContext = Context.current().with(parentSpan);
        Span childSpan = tracer.spanBuilder(operationName)
                .setParent(parentContext)
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        activeSpans.put(operationId, childSpan);
        
        return operationId;
    }

    /**
     * Creates a new independent span for an operation.
     *
     * @param operationName The name of the operation
     * @return A new operation ID for the span
     */
    public String startSpan(String operationName) {
        String operationId = generateOperationId(operationName, System.currentTimeMillis());
        
        Span span = tracer.spanBuilder(operationName)
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        activeSpans.put(operationId, span);
        
        return operationId;
    }

    /**
     * Makes a span the active span in the current context and returns a Scope that
     * should be closed when the operation is complete.
     *
     * @param operationId The operation ID of the span to make active
     * @return A Scope that should be closed when the operation is complete
     */
    public Scope withSpan(String operationId) {
        Span span = activeSpans.get(operationId);
        if (span != null) {
            return span.makeCurrent();
        }
        return Context.current().makeCurrent();
    }

    /**
     * Injects the current trace context into a carrier for propagation across service boundaries.
     *
     * @param carrier The carrier to inject the context into (e.g., HTTP headers)
     */
    public void injectContext(Map<String, String> carrier) {
        openTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), carrier, TEXT_MAP_SETTER);
    }

    /**
     * Extracts trace context from a carrier and returns a Context object.
     *
     * @param carrier The carrier containing the trace context (e.g., HTTP headers)
     * @return A Context object containing the extracted trace context
     */
    public Context extractContext(Map<String, String> carrier) {
        return openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), carrier, TEXT_MAP_GETTER);
    }

    /**
     * Generates a unique operation ID for a span.
     *
     * @param operation The operation name
     * @param identifier A unique identifier (e.g., user ID, timestamp)
     * @return A unique operation ID
     */
    private String generateOperationId(String operation, long identifier) {
        return operation + "-" + identifier + "-" + System.nanoTime();
    }
}