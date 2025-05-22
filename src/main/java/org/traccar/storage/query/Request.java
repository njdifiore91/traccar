/*
 * Copyright 2022-2025 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.context.Context;

import java.util.HashMap;
import java.util.Map;

/**
 * Database query request with OpenTelemetry integration for distributed tracing.
 * Supports correlation IDs for tracking requests across microservices.
 */
public class Request {

    private final Columns columns;
    private final Condition condition;
    private final Order order;
    private final String correlationId;
    private final Map<String, String> serviceConfig;
    private final SpanContext spanContext;
    private final ResourceConfig resourceConfig;

    /**
     * Creates a request with columns specification.
     *
     * @param columns columns to include in the query
     */
    public Request(Columns columns) {
        this(columns, null, null);
    }

    /**
     * Creates a request with condition specification.
     *
     * @param condition filtering condition
     */
    public Request(Condition condition) {
        this(null, condition, null);
    }

    /**
     * Creates a request with columns and condition specifications.
     *
     * @param columns columns to include in the query
     * @param condition filtering condition
     */
    public Request(Columns columns, Condition condition) {
        this(columns, condition, null);
    }

    /**
     * Creates a request with columns and order specifications.
     *
     * @param columns columns to include in the query
     * @param order result ordering
     */
    public Request(Columns columns, Order order) {
        this(columns, null, order);
    }

    /**
     * Creates a request with columns, condition, and order specifications.
     *
     * @param columns columns to include in the query
     * @param condition filtering condition
     * @param order result ordering
     */
    public Request(Columns columns, Condition condition, Order order) {
        this(columns, condition, order, null, null, null);
    }

    /**
     * Creates a request with columns, condition, order, and correlation ID specifications.
     * This constructor supports distributed tracing across microservices.
     *
     * @param columns columns to include in the query
     * @param condition filtering condition
     * @param order result ordering
     * @param correlationId unique identifier for tracing this request across services
     * @param serviceConfig service-specific configuration parameters
     * @param spanContext OpenTelemetry span context for distributed tracing
     */
    public Request(Columns columns, Condition condition, Order order, 
                  String correlationId, Map<String, String> serviceConfig, SpanContext spanContext) {
        this.columns = columns;
        this.condition = condition;
        this.order = order;
        this.correlationId = correlationId;
        this.serviceConfig = serviceConfig != null ? serviceConfig : new HashMap<>();
        this.spanContext = spanContext;
        this.resourceConfig = new ResourceConfig();
    }

    /**
     * Creates a new request builder for fluent API usage.
     *
     * @return new request builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Get columns specification.
     *
     * @return columns or null if not specified
     */
    public Columns getColumns() {
        return columns;
    }

    /**
     * Get condition specification.
     *
     * @return condition or null if not specified
     */
    public Condition getCondition() {
        return condition;
    }

    /**
     * Get order specification.
     *
     * @return order or null if not specified
     */
    public Order getOrder() {
        return order;
    }

    /**
     * Get correlation ID for distributed tracing.
     *
     * @return correlation ID or null if not specified
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Get service-specific configuration parameters.
     *
     * @return map of configuration parameters
     */
    public Map<String, String> getServiceConfig() {
        return serviceConfig;
    }

    /**
     * Get OpenTelemetry span context for distributed tracing.
     *
     * @return span context or null if not specified
     */
    public SpanContext getSpanContext() {
        return spanContext;
    }

    /**
     * Get resource configuration for containerized environments.
     *
     * @return resource configuration
     */
    public ResourceConfig getResourceConfig() {
        return resourceConfig;
    }

    /**
     * Creates a new request with the current OpenTelemetry context.
     *
     * @return new request with current tracing context
     */
    public Request withCurrentContext() {
        Span currentSpan = Span.current();
        return new Request(
                columns, condition, order, 
                correlationId, serviceConfig, 
                currentSpan.getSpanContext());
    }

    /**
     * Creates a new request with the specified correlation ID.
     *
     * @param correlationId correlation ID for distributed tracing
     * @return new request with the correlation ID
     */
    public Request withCorrelationId(String correlationId) {
        return new Request(
                columns, condition, order, 
                correlationId, serviceConfig, 
                spanContext);
    }

    /**
     * Creates a new request with the specified service configuration.
     *
     * @param key configuration parameter key
     * @param value configuration parameter value
     * @return new request with updated service configuration
     */
    public Request withServiceConfig(String key, String value) {
        Map<String, String> newConfig = new HashMap<>(serviceConfig);
        newConfig.put(key, value);
        return new Request(
                columns, condition, order, 
                correlationId, newConfig, 
                spanContext);
    }

    /**
     * Builder class for creating Request objects using fluent API.
     */
    public static class Builder {
        private Columns columns;
        private Condition condition;
        private Order order;
        private String correlationId;
        private Map<String, String> serviceConfig = new HashMap<>();
        private SpanContext spanContext;

        public Builder columns(Columns columns) {
            this.columns = columns;
            return this;
        }

        public Builder condition(Condition condition) {
            this.condition = condition;
            return this;
        }

        public Builder order(Order order) {
            this.order = order;
            return this;
        }

        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }

        public Builder serviceConfig(String key, String value) {
            this.serviceConfig.put(key, value);
            return this;
        }

        public Builder serviceConfig(Map<String, String> config) {
            this.serviceConfig.putAll(config);
            return this;
        }

        public Builder spanContext(SpanContext spanContext) {
            this.spanContext = spanContext;
            return this;
        }

        public Builder currentContext() {
            this.spanContext = Span.current().getSpanContext();
            return this;
        }

        public Request build() {
            return new Request(columns, condition, order, correlationId, serviceConfig, spanContext);
        }
    }

    /**
     * Configuration for resource constraints in containerized environments.
     */
    public static class ResourceConfig {
        private int maxConnections = 10;
        private int queryTimeout = 30000; // milliseconds
        private int maxResults = 1000;
        private boolean lowMemoryMode = false;

        public int getMaxConnections() {
            return maxConnections;
        }

        public void setMaxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
        }

        public int getQueryTimeout() {
            return queryTimeout;
        }

        public void setQueryTimeout(int queryTimeout) {
            this.queryTimeout = queryTimeout;
        }

        public int getMaxResults() {
            return maxResults;
        }

        public void setMaxResults(int maxResults) {
            this.maxResults = maxResults;
        }

        public boolean isLowMemoryMode() {
            return lowMemoryMode;
        }

        public void setLowMemoryMode(boolean lowMemoryMode) {
            this.lowMemoryMode = lowMemoryMode;
        }
    }
}