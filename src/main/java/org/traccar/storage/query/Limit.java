/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.storage.query;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;

/**
 * Represents a limit for database queries with support for OpenTelemetry tracing,
 * memory optimization for containerized environments, service-specific configurations,
 * and distributed database pagination patterns.
 */
public class Limit {

    private final int value;
    private final int offset;
    private final String serviceId;
    private final boolean optimizeMemory;
    private SpanContext spanContext;

    /**
     * Creates a simple limit with default settings.
     *
     * @param value The maximum number of records to return
     */
    public Limit(int value) {
        this(value, 0, null, true);
    }

    /**
     * Creates a limit with offset for pagination.
     *
     * @param value The maximum number of records to return
     * @param offset The number of records to skip
     */
    public Limit(int value, int offset) {
        this(value, offset, null, true);
    }

    /**
     * Creates a fully configured limit.
     *
     * @param value The maximum number of records to return
     * @param offset The number of records to skip
     * @param serviceId The service identifier for service-specific configurations
     * @param optimizeMemory Whether to optimize memory usage for containerized environments
     */
    public Limit(int value, int offset, String serviceId, boolean optimizeMemory) {
        this.value = value;
        this.offset = offset;
        this.serviceId = serviceId;
        this.optimizeMemory = optimizeMemory;
        this.spanContext = Span.current().getSpanContext();
    }

    /**
     * Gets the maximum number of records to return.
     *
     * @return The limit value
     */
    public int getValue() {
        return value;
    }

    /**
     * Gets the number of records to skip.
     *
     * @return The offset value
     */
    public int getOffset() {
        return offset;
    }

    /**
     * Gets the service identifier for service-specific configurations.
     *
     * @return The service identifier or null if not specified
     */
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Checks if memory optimization is enabled for containerized environments.
     *
     * @return True if memory optimization is enabled, false otherwise
     */
    public boolean isMemoryOptimized() {
        return optimizeMemory;
    }

    /**
     * Gets the OpenTelemetry span context associated with this limit operation.
     *
     * @return The span context or null if not available
     */
    public SpanContext getSpanContext() {
        return spanContext;
    }

    /**
     * Creates a new limit for the next page of results.
     *
     * @return A new limit object for the next page
     */
    public Limit nextPage() {
        return new Limit(value, offset + value, serviceId, optimizeMemory);
    }

    /**
     * Creates a new limit with a different page size but maintaining the same position.
     *
     * @param newValue The new page size
     * @return A new limit object with the updated page size
     */
    public Limit withValue(int newValue) {
        return new Limit(newValue, offset, serviceId, optimizeMemory);
    }

    /**
     * Creates a new limit with memory optimization setting.
     *
     * @param optimize Whether to optimize memory usage
     * @return A new limit object with the updated memory optimization setting
     */
    public Limit withMemoryOptimization(boolean optimize) {
        return new Limit(value, offset, serviceId, optimize);
    }

    /**
     * Creates a new limit with a specific service identifier.
     *
     * @param newServiceId The service identifier
     * @return A new limit object with the updated service identifier
     */
    public Limit withServiceId(String newServiceId) {
        return new Limit(value, offset, newServiceId, optimizeMemory);
    }

    /**
     * Creates a traced limit operation using the provided OpenTelemetry tracer.
     *
     * @param tracer The OpenTelemetry tracer
     * @param operationName The name of the operation being traced
     * @return A new limit object with tracing enabled
     */
    public Limit withTracing(Tracer tracer, String operationName) {
        Limit result = new Limit(value, offset, serviceId, optimizeMemory);
        Span span = tracer.spanBuilder(operationName)
                .setParent(Context.current().with(Span.current()))
                .setAttribute("db.limit.value", value)
                .setAttribute("db.limit.offset", offset)
                .startSpan();
        result.spanContext = span.getSpanContext();
        return result;
    }

    /**
     * Calculates the optimal limit value based on available memory and container constraints.
     * This helps prevent out-of-memory errors in containerized environments by adjusting
     * the page size dynamically based on available resources.
     *
     * @param estimatedRecordSizeBytes The estimated size of each record in bytes
     * @param availableMemoryPercent The percentage of available memory to use (0-100)
     * @return A new limit object with an optimized value
     */
    public Limit optimizeForMemory(int estimatedRecordSizeBytes, int availableMemoryPercent) {
        if (!optimizeMemory) {
            return this;
        }
        
        // Get available memory from runtime
        long maxMemory = Runtime.getRuntime().maxMemory();
        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        long availableMemory = maxMemory - usedMemory;
        
        // Calculate safe memory to use (percentage of available)
        long safeMemory = availableMemory * availableMemoryPercent / 100;
        
        // Calculate optimal record count based on estimated record size
        int optimalLimit = (int) Math.min(value, safeMemory / Math.max(1, estimatedRecordSizeBytes));
        
        // Ensure we have at least one record
        optimalLimit = Math.max(1, optimalLimit);
        
        return new Limit(optimalLimit, offset, serviceId, true);
    }

    @Override
    public String toString() {
        return "Limit{" +
                "value=" + value +
                ", offset=" + offset +
                ", serviceId='" + serviceId + '\'' +
                ", optimizeMemory=" + optimizeMemory +
                '}';
    }
}
