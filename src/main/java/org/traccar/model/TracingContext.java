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
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * Represents the tracing context for distributed tracing across microservices.
 * This class stores trace IDs, span IDs, and other metadata needed for correlating
 * requests across service boundaries.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TracingContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private String traceId;
    private String spanId;
    private String parentSpanId;
    private Map<String, String> baggage = new HashMap<>();

    /**
     * Default constructor that generates a new trace ID.
     */
    public TracingContext() {
        this.traceId = UUID.randomUUID().toString();
    }

    /**
     * Constructor with trace ID and span ID.
     * 
     * @param traceId The trace ID
     * @param spanId The span ID
     */
    public TracingContext(String traceId, String spanId) {
        this.traceId = traceId;
        this.spanId = spanId;
    }

    /**
     * Gets the trace ID.
     * 
     * @return The trace ID
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * Sets the trace ID.
     * 
     * @param traceId The trace ID to set
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * Gets the span ID.
     * 
     * @return The span ID
     */
    public String getSpanId() {
        return spanId;
    }

    /**
     * Sets the span ID.
     * 
     * @param spanId The span ID to set
     */
    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    /**
     * Gets the parent span ID.
     * 
     * @return The parent span ID
     */
    public String getParentSpanId() {
        return parentSpanId;
    }

    /**
     * Sets the parent span ID.
     * 
     * @param parentSpanId The parent span ID to set
     */
    public void setParentSpanId(String parentSpanId) {
        this.parentSpanId = parentSpanId;
    }

    /**
     * Creates a child span context from this context.
     * 
     * @return A new tracing context with this context as parent
     */
    public TracingContext createChildSpan() {
        TracingContext child = new TracingContext();
        child.setTraceId(this.traceId);
        child.setParentSpanId(this.spanId);
        child.setSpanId(UUID.randomUUID().toString());
        child.baggage.putAll(this.baggage);
        return child;
    }

    /**
     * Gets all baggage items.
     * 
     * @return Map of baggage items
     */
    @JsonAnyGetter
    public Map<String, String> getBaggage() {
        return baggage;
    }

    /**
     * Sets a baggage item.
     * 
     * @param key The baggage key
     * @param value The baggage value
     */
    @JsonAnySetter
    public void setBaggageItem(String key, String value) {
        baggage.put(key, value);
    }

    /**
     * Gets a baggage item by key.
     * 
     * @param key The baggage key
     * @return The baggage value or null if not found
     */
    public String getBaggageItem(String key) {
        return baggage.get(key);
    }
}