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
package org.traccar.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates distributed tracing information for propagation across microservices.
 * This class stores OpenTelemetry trace identifiers and sampling flags to maintain
 * trace context when model objects are serialized and transmitted via message brokers.
 */
public class TracingContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private String traceId;
    private String spanId;
    private String parentSpanId;
    private boolean sampled;
    private Map<String, String> traceState;

    /**
     * Default constructor for serialization frameworks.
     */
    public TracingContext() {
        this.traceState = new HashMap<>();
    }

    /**
     * Creates a new tracing context with the specified trace identifiers.
     *
     * @param traceId      The trace identifier (unique across the entire distributed trace)
     * @param spanId       The span identifier (unique within a trace)
     * @param parentSpanId The parent span identifier (may be null for root spans)
     * @param sampled      Whether this trace is sampled for collection
     */
    public TracingContext(String traceId, String spanId, String parentSpanId, boolean sampled) {
        this.traceId = traceId;
        this.spanId = spanId;
        this.parentSpanId = parentSpanId;
        this.sampled = sampled;
        this.traceState = new HashMap<>();
    }

    /**
     * Gets the trace identifier.
     *
     * @return The trace identifier
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * Sets the trace identifier.
     *
     * @param traceId The trace identifier
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * Gets the span identifier.
     *
     * @return The span identifier
     */
    public String getSpanId() {
        return spanId;
    }

    /**
     * Sets the span identifier.
     *
     * @param spanId The span identifier
     */
    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    /**
     * Gets the parent span identifier.
     *
     * @return The parent span identifier
     */
    public String getParentSpanId() {
        return parentSpanId;
    }

    /**
     * Sets the parent span identifier.
     *
     * @param parentSpanId The parent span identifier
     */
    public void setParentSpanId(String parentSpanId) {
        this.parentSpanId = parentSpanId;
    }

    /**
     * Checks if this trace is sampled for collection.
     *
     * @return True if sampled, false otherwise
     */
    public boolean isSampled() {
        return sampled;
    }

    /**
     * Sets whether this trace is sampled for collection.
     *
     * @param sampled True if sampled, false otherwise
     */
    public void setSampled(boolean sampled) {
        this.sampled = sampled;
    }

    /**
     * Gets the trace state map containing vendor-specific trace information.
     *
     * @return The trace state map
     */
    public Map<String, String> getTraceState() {
        return traceState;
    }

    /**
     * Sets the trace state map containing vendor-specific trace information.
     *
     * @param traceState The trace state map
     */
    public void setTraceState(Map<String, String> traceState) {
        this.traceState = traceState;
    }

    /**
     * Adds a key-value pair to the trace state.
     *
     * @param key   The key
     * @param value The value
     */
    public void addTraceState(String key, String value) {
        if (this.traceState == null) {
            this.traceState = new HashMap<>();
        }
        this.traceState.put(key, value);
    }

    /**
     * Extracts tracing context from a carrier object (like HTTP headers or message properties).
     * This is a static factory method to create a TracingContext from external carrier data.
     *
     * @param carrier The carrier object containing trace context information
     * @return A new TracingContext instance populated with data from the carrier
     */
    public static TracingContext extract(Map<String, String> carrier) {
        if (carrier == null || !carrier.containsKey("traceparent")) {
            return null;
        }

        // Parse W3C traceparent header: 00-traceId-spanId-flags
        String traceparent = carrier.get("traceparent");
        if (traceparent == null || traceparent.isEmpty()) {
            return null;
        }

        String[] parts = traceparent.split("-");
        if (parts.length != 4) {
            return null;
        }

        String traceId = parts[1];
        String spanId = parts[2];
        boolean sampled = (parts[3].charAt(0) == '1');

        TracingContext context = new TracingContext(traceId, spanId, null, sampled);

        // Parse W3C tracestate header if present
        String tracestate = carrier.get("tracestate");
        if (tracestate != null && !tracestate.isEmpty()) {
            for (String entry : tracestate.split(",")) {
                String[] keyValue = entry.trim().split("=", 2);
                if (keyValue.length == 2) {
                    context.addTraceState(keyValue[0], keyValue[1]);
                }
            }
        }

        return context;
    }

    /**
     * Injects this tracing context into a carrier object (like HTTP headers or message properties).
     * This allows the trace context to be propagated to downstream services.
     *
     * @param carrier The carrier object to inject trace context information into
     */
    public void inject(Map<String, String> carrier) {
        if (carrier == null || traceId == null || spanId == null) {
            return;
        }

        // Format W3C traceparent header: 00-traceId-spanId-flags
        String flags = sampled ? "01" : "00";
        carrier.put("traceparent", String.format("00-%s-%s-%s", traceId, spanId, flags));

        // Format W3C tracestate header if present
        if (traceState != null && !traceState.isEmpty()) {
            StringBuilder tracestateBuilder = new StringBuilder();
            boolean first = true;
            for (Map.Entry<String, String> entry : traceState.entrySet()) {
                if (!first) {
                    tracestateBuilder.append(",");
                }
                tracestateBuilder.append(entry.getKey()).append("=").append(entry.getValue());
                first = false;
            }
            carrier.put("tracestate", tracestateBuilder.toString());
        }
    }

    /**
     * Creates a child context from this context for a new span.
     *
     * @param newSpanId The new span identifier
     * @return A new TracingContext with the same trace ID but updated span information
     */
    public TracingContext createChildContext(String newSpanId) {
        TracingContext childContext = new TracingContext(
                this.traceId,
                newSpanId,
                this.spanId,
                this.sampled);
        
        if (this.traceState != null) {
            childContext.setTraceState(new HashMap<>(this.traceState));
        }
        
        return childContext;
    }

    @Override
    public String toString() {
        return "TracingContext{" +
                "traceId='" + traceId + '\'' +
                ", spanId='" + spanId + '\'' +
                ", parentSpanId='" + parentSpanId + '\'' +
                ", sampled=" + sampled +
                ", traceState=" + traceState +
                '}';
    }
}