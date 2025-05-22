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
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

/**
 * Represents an ordering specification for database queries.
 * Supports OpenTelemetry tracing and distributed database ordering strategies.
 */
public class Order {

    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("org.traccar.storage.query");
    
    private final String column;
    private final boolean descending;
    private final int limit;
    private final OrderStrategy strategy;
    private final SpanContext parentContext;

    /**
     * Enum defining different ordering strategies for various database environments.
     */
    public enum OrderStrategy {
        DEFAULT,           // Standard ordering strategy
        DISTRIBUTED,       // Strategy optimized for distributed databases
        CONTAINER_OPTIMIZED, // Strategy optimized for containerized environments
        SERVICE_SPECIFIC   // Strategy tailored to specific service requirements
    }

    /**
     * Creates an order specification with the default strategy.
     * 
     * @param column The column to order by
     */
    public Order(String column) {
        this(column, false, 0);
    }

    /**
     * Creates an order specification with the default strategy.
     * 
     * @param column The column to order by
     * @param descending Whether to order in descending order
     * @param limit Maximum number of results to return
     */
    public Order(String column, boolean descending, int limit) {
        this(column, descending, limit, OrderStrategy.DEFAULT, null);
    }

    /**
     * Creates an order specification with a specific strategy.
     * 
     * @param column The column to order by
     * @param descending Whether to order in descending order
     * @param limit Maximum number of results to return
     * @param strategy The ordering strategy to use
     */
    public Order(String column, boolean descending, int limit, OrderStrategy strategy) {
        this(column, descending, limit, strategy, null);
    }

    /**
     * Creates an order specification with a specific strategy and parent span context for tracing.
     * 
     * @param column The column to order by
     * @param descending Whether to order in descending order
     * @param limit Maximum number of results to return
     * @param strategy The ordering strategy to use
     * @param parentContext The parent span context for distributed tracing
     */
    public Order(String column, boolean descending, int limit, OrderStrategy strategy, SpanContext parentContext) {
        this.column = column;
        this.descending = descending;
        this.limit = limit;
        this.strategy = strategy != null ? strategy : OrderStrategy.DEFAULT;
        this.parentContext = parentContext;
    }

    /**
     * Gets the column to order by.
     * 
     * @return The column name
     */
    public String getColumn() {
        return column;
    }

    /**
     * Checks if the order is descending.
     * 
     * @return True if descending, false if ascending
     */
    public boolean getDescending() {
        return descending;
    }

    /**
     * Gets the maximum number of results to return.
     * 
     * @return The limit value
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Gets the ordering strategy.
     * 
     * @return The ordering strategy
     */
    public OrderStrategy getStrategy() {
        return strategy;
    }

    /**
     * Gets the parent span context for distributed tracing.
     * 
     * @return The parent span context
     */
    public SpanContext getParentContext() {
        return parentContext;
    }

    /**
     * Creates a new Order instance with the same properties but a different strategy.
     * 
     * @param strategy The new ordering strategy
     * @return A new Order instance with the updated strategy
     */
    public Order withStrategy(OrderStrategy strategy) {
        return new Order(this.column, this.descending, this.limit, strategy, this.parentContext);
    }

    /**
     * Creates a new Order instance with the same properties but a different limit.
     * 
     * @param limit The new limit value
     * @return A new Order instance with the updated limit
     */
    public Order withLimit(int limit) {
        return new Order(this.column, this.descending, limit, this.strategy, this.parentContext);
    }

    /**
     * Creates a new Order instance with the same properties but linked to a parent span context.
     * 
     * @param parentContext The parent span context for distributed tracing
     * @return A new Order instance with the updated parent context
     */
    public Order withParentContext(SpanContext parentContext) {
        return new Order(this.column, this.descending, this.limit, this.strategy, parentContext);
    }

    /**
     * Creates a traced span for this order operation.
     * 
     * @param operationName The name of the operation being performed
     * @return A new span for tracing the operation
     */
    public Span createSpan(String operationName) {
        Span.Builder spanBuilder = TRACER.spanBuilder(operationName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(AttributeKey.stringKey("db.order.column"), column)
                .setAttribute(AttributeKey.booleanKey("db.order.descending"), descending)
                .setAttribute(AttributeKey.longKey("db.order.limit"), limit)
                .setAttribute(AttributeKey.stringKey("db.order.strategy"), strategy.name());

        if (parentContext != null && parentContext.isValid()) {
            spanBuilder.setParent(Context.current().with(Span.wrap(parentContext)));
        }

        return spanBuilder.startSpan();
    }

    /**
     * Applies the appropriate ordering strategy based on the current configuration.
     * This method should be called when executing the query to ensure the correct
     * ordering strategy is applied for the current environment.
     * 
     * @param databaseType The type of database being used
     * @return A string representation of the order clause suitable for the database
     */
    public String applyStrategy(String databaseType) {
        Span span = createSpan("applyOrderStrategy");
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("db.type", databaseType);
            
            StringBuilder orderClause = new StringBuilder();
            orderClause.append(column);
            
            if (descending) {
                orderClause.append(" DESC");
            }
            
            // Apply strategy-specific modifications
            switch (strategy) {
                case DISTRIBUTED:
                    // For distributed databases, ensure consistent ordering with additional columns if needed
                    if (!column.contains(".") && !column.equalsIgnoreCase("id")) {
                        orderClause.append(", id"); // Add ID as secondary sort to ensure consistency
                    }
                    break;
                    
                case CONTAINER_OPTIMIZED:
                    // For containerized environments, optimize for memory usage
                    // No special handling needed in the SQL, but tracked for telemetry
                    span.setAttribute("container.optimized", true);
                    break;
                    
                case SERVICE_SPECIFIC:
                    // Service-specific ordering would be customized based on service needs
                    // This would typically be handled by the calling service
                    span.setAttribute("service.specific", true);
                    break;
                    
                default:
                    // Default strategy requires no special handling
                    break;
            }
            
            return orderClause.toString();
        } finally {
            span.end();
        }
    }
}