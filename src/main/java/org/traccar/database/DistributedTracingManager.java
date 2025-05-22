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
package org.traccar.database;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides OpenTelemetry integration for database operations, enabling distributed tracing
 * across microservices. This class creates and manages trace spans for database queries,
 * captures query parameters and execution times, and propagates trace context across
 * service boundaries.
 * 
 * This manager integrates with the OpenTelemetry observability framework to provide
 * detailed insights into database operations, helping to diagnose performance issues
 * and bottlenecks in the distributed system.
 */
@Singleton
public class DistributedTracingManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTracingManager.class);

    private static final String INSTRUMENTATION_NAME = "org.traccar.database";
    private static final String INSTRUMENTATION_VERSION = "1.0.0";

    // Database span attribute keys
    private static final AttributeKey<String> DB_SYSTEM = AttributeKey.stringKey("db.system");
    private static final AttributeKey<String> DB_OPERATION = AttributeKey.stringKey("db.operation");
    private static final AttributeKey<String> DB_STATEMENT = AttributeKey.stringKey("db.statement");
    private static final AttributeKey<String> DB_NAME = AttributeKey.stringKey("db.name");
    private static final AttributeKey<String> DB_USER = AttributeKey.stringKey("db.user");
    private static final AttributeKey<Long> DB_EXECUTION_TIME = AttributeKey.longKey("db.execution_time_ms");
    private static final AttributeKey<String> DB_QUERY_PARAMS = AttributeKey.stringKey("db.query.params");
    private static final AttributeKey<String> DB_AFFECTED_ROWS = AttributeKey.stringKey("db.affected_rows");
    private static final AttributeKey<String> CORRELATION_ID = AttributeKey.stringKey("correlation.id");
    private static final AttributeKey<String> SERVICE_NAME = AttributeKey.stringKey("service.name");

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final boolean tracingEnabled;
    private final String serviceName;
    private final Map<String, String> correlationIds = new ConcurrentHashMap<>();

    /**
     * Constructs a new DistributedTracingManager with the specified OpenTelemetry instance and configuration.
     *
     * @param openTelemetry The OpenTelemetry instance to use for tracing
     * @param config The configuration to determine if tracing is enabled
     */
    @Inject
    public DistributedTracingManager(OpenTelemetry openTelemetry, Config config) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer(INSTRUMENTATION_NAME, INSTRUMENTATION_VERSION);
        this.tracingEnabled = config.getBoolean(Keys.DATABASE_TRACING_ENABLED, false);
        this.serviceName = config.getString(Keys.SERVICE_NAME, "traccar");
        LOGGER.info("Database distributed tracing {}", tracingEnabled ? "enabled" : "disabled");
    }

    /**
     * Creates a new span for a database operation and sets it as the current span.
     *
     * @param operation The database operation being performed (e.g., "SELECT", "INSERT")
     * @param statement The SQL statement or query being executed
     * @param dbName The name of the database
     * @param dbUser The database user
     * @return A TracingContext containing the span and scope, which should be closed after the operation
     */
    public TracingContext startSpan(String operation, String statement, String dbName, String dbUser) {
        if (!tracingEnabled) {
            return new TracingContext(null, null);
        }

        Span span = tracer.spanBuilder("DB:" + operation)
                .setSpanKind(SpanKind.CLIENT)
                .setParent(Context.current())
                .setAttribute(DB_SYSTEM, "sql")
                .setAttribute(DB_OPERATION, operation)
                .setAttribute(DB_STATEMENT, statement)
                .setAttribute(DB_NAME, dbName)
                .setAttribute(DB_USER, dbUser)
                .setAttribute(SERVICE_NAME, serviceName)
                .startSpan();

        // Add correlation ID if available
        String correlationId = getCorrelationId();
        if (correlationId != null) {
            span.setAttribute(CORRELATION_ID, correlationId);
        }

        Scope scope = span.makeCurrent();
        return new TracingContext(span, scope);
    }

    /**
     * Ends a database operation span with the specified execution time and status.
     *
     * @param context The TracingContext returned by startSpan
     * @param executionTimeMs The execution time of the database operation in milliseconds
     * @param success Whether the operation was successful
     * @param errorMessage The error message if the operation failed, or null if successful
     */
    public void endSpan(TracingContext context, long executionTimeMs, boolean success, String errorMessage) {
        if (!tracingEnabled || context.span == null) {
            return;
        }

        try {
            Span span = context.span;
            span.setAttribute(DB_EXECUTION_TIME, executionTimeMs);

            if (!success) {
                span.setStatus(StatusCode.ERROR, errorMessage != null ? errorMessage : "Database operation failed");
                span.recordException(new Exception(errorMessage));
            }

            span.end();
        } finally {
            if (context.scope != null) {
                context.scope.close();
            }
        }
    }
    
    /**
     * Ends a database operation span with the specified execution time, status, and affected rows.
     *
     * @param context The TracingContext returned by startSpan
     * @param executionTimeMs The execution time of the database operation in milliseconds
     * @param affectedRows The number of rows affected by the operation
     * @param success Whether the operation was successful
     * @param errorMessage The error message if the operation failed, or null if successful
     */
    public void endSpan(TracingContext context, long executionTimeMs, int affectedRows, boolean success, String errorMessage) {
        if (!tracingEnabled || context.span == null) {
            return;
        }

        try {
            Span span = context.span;
            span.setAttribute(DB_EXECUTION_TIME, executionTimeMs);
            span.setAttribute(DB_AFFECTED_ROWS, String.valueOf(affectedRows));

            if (!success) {
                span.setStatus(StatusCode.ERROR, errorMessage != null ? errorMessage : "Database operation failed");
                span.recordException(new Exception(errorMessage));
            }

            span.end();
        } finally {
            if (context.scope != null) {
                context.scope.close();
            }
        }
    }

    /**
     * Sets a correlation ID for the current request context. This ID can be used to correlate
     * traces across service boundaries.
     *
     * @param requestId A unique identifier for the request
     * @param correlationId The correlation ID to associate with the request
     */
    public void setCorrelationId(String requestId, String correlationId) {
        if (tracingEnabled && correlationId != null && !correlationId.isEmpty()) {
            correlationIds.put(requestId, correlationId);
        }
    }

    /**
     * Clears the correlation ID for the specified request.
     *
     * @param requestId The request ID whose correlation ID should be cleared
     */
    public void clearCorrelationId(String requestId) {
        if (tracingEnabled) {
            correlationIds.remove(requestId);
        }
    }

    /**
     * Gets the correlation ID for the current thread context.
     *
     * @return The correlation ID, or null if none is set
     */
    public String getCorrelationId() {
        if (!tracingEnabled) {
            return null;
        }

        // Get the current span and extract the correlation ID if available
        Span currentSpan = Span.current();
        if (currentSpan != null && !currentSpan.equals(Span.getInvalid())) {
            // Try to get correlation ID from span context
            Context context = Context.current();
            for (Map.Entry<String, String> entry : correlationIds.entrySet()) {
                if (context.get(AttributeKey.stringKey(entry.getKey())) != null) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    /**
     * Creates attributes for a database span with the specified parameters.
     *
     * @param operation The database operation being performed
     * @param statement The SQL statement or query being executed
     * @param dbName The name of the database
     * @param dbUser The database user
     * @return The attributes for the span
     */
    public Attributes createDatabaseAttributes(String operation, String statement, String dbName, String dbUser) {
        Attributes.Builder attributesBuilder = Attributes.builder()
                .put(DB_SYSTEM, "sql")
                .put(DB_OPERATION, operation)
                .put(DB_STATEMENT, statement)
                .put(DB_NAME, dbName)
                .put(DB_USER, dbUser)
                .put(SERVICE_NAME, serviceName);

        String correlationId = getCorrelationId();
        if (correlationId != null) {
            attributesBuilder.put(CORRELATION_ID, correlationId);
        }

        return attributesBuilder.build();
    }

    /**
     * A context object that holds the span and scope for a database operation.
     */
    public static class TracingContext {
        private final Span span;
        private final Scope scope;

        /**
         * Constructs a new TracingContext with the specified span and scope.
         *
         * @param span The span for the database operation
         * @param scope The scope for the database operation
         */
        public TracingContext(Span span, Scope scope) {
            this.span = span;
            this.scope = scope;
        }

        /**
         * Gets the span for the database operation.
         *
         * @return The span
         */
        public Span getSpan() {
            return span;
        }
    }
}