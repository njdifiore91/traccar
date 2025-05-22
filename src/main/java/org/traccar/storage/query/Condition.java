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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.model.GroupedModel;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * Interface for database query conditions.
 * Enhanced with OpenTelemetry integration for tracing condition building operations,
 * support for distributed database patterns across microservices,
 * service-specific filtering conditions, and optimizations for containerized environments.
 */
public interface Condition {

    /**
     * OpenTelemetry tracer for condition operations.
     * Used to trace condition building and evaluation operations across microservices.
     */
    Tracer TRACER = GlobalOpenTelemetry.getTracer("org.traccar.storage.query");
    
    /**
     * Cache for frequently used conditions to optimize resource usage in containerized environments.
     */
    Map<String, Condition> CONDITION_CACHE = new ConcurrentHashMap<>();

    /**
     * Creates a span for condition operations with appropriate attributes.
     * 
     * @param operationName Name of the operation being performed
     * @param conditionType Type of the condition
     * @param attributes Additional attributes to add to the span
     * @return The created span
     */
    static Span createSpan(String operationName, String conditionType, Attributes attributes) {
        return TRACER.spanBuilder(operationName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(AttributeKey.stringKey("condition.type"), conditionType)
                .setAllAttributes(attributes)
                .startSpan();
    }
    
    /**
     * Executes an operation with tracing.
     * 
     * @param operationName Name of the operation being performed
     * @param conditionType Type of the condition
     * @param attributes Additional attributes to add to the span
     * @param operation The operation to execute
     * @param <T> The return type of the operation
     * @return The result of the operation
     */
    static <T> T withSpan(String operationName, String conditionType, Attributes attributes, Supplier<T> operation) {
        Span span = createSpan(operationName, conditionType, attributes);
        try (Scope scope = span.makeCurrent()) {
            T result = operation.get();
            return result;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Gets a cached condition or creates a new one if not in cache.
     * 
     * @param cacheKey The cache key for the condition
     * @param conditionSupplier Supplier to create the condition if not in cache
     * @return The cached or newly created condition
     */
    static Condition getCachedOrCreate(String cacheKey, Supplier<Condition> conditionSupplier) {
        return CONDITION_CACHE.computeIfAbsent(cacheKey, k -> conditionSupplier.get());
    }

    /**
     * Merges multiple conditions into a single condition using AND operators.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     * 
     * @param conditions List of conditions to merge
     * @return A single merged condition, or null if the list is empty
     */
    static Condition merge(List<Condition> conditions) {
        return withSpan("merge_conditions", "merge", 
                Attributes.of(AttributeKey.longKey("conditions.count"), conditions.size()),
                () -> {
                    Condition result = null;
                    var iterator = conditions.iterator();
                    if (iterator.hasNext()) {
                        result = iterator.next();
                        while (iterator.hasNext()) {
                            result = new Condition.And(result, iterator.next());
                        }
                    }
                    return result;
                });
    }

    /**
     * Equality condition that compares a column to a value.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     */
    class Equals extends Compare {
        public Equals(String column, Object value) {
            super(column, "=", column, value);
        }
        
        /**
         * Creates a cached equality condition to optimize resource usage.
         * 
         * @param column Column name to compare
         * @param value Value to compare against
         * @return A cached or new Equals condition
         */
        public static Equals cached(String column, Object value) {
            String cacheKey = "equals:" + column + ":" + (value != null ? value.toString() : "null");
            return (Equals) getCachedOrCreate(cacheKey, () -> new Equals(column, value));
        }
    }

    /**
     * Comparison condition that compares a column to a value using an operator.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     */
    class Compare implements Condition {
        private final String column;
        private final String operator;
        private final String variable;
        private final Object value;

        public Compare(String column, String operator, String variable, Object value) {
            Span span = createSpan("create_compare_condition", "compare", 
                    Attributes.of(
                        AttributeKey.stringKey("condition.column"), column,
                        AttributeKey.stringKey("condition.operator"), operator));
            try (Scope scope = span.makeCurrent()) {
                this.column = column;
                this.operator = operator;
                this.variable = variable;
                this.value = value;
                span.addEvent("Compare condition created");
            } finally {
                span.end();
            }
        }

        public String getColumn() {
            return column;
        }

        public String getOperator() {
            return operator;
        }

        public String getVariable() {
            return variable;
        }

        public Object getValue() {
            return value;
        }
        
        /**
         * Creates a cached comparison condition to optimize resource usage.
         * 
         * @param column Column name to compare
         * @param operator Comparison operator
         * @param variable Variable name
         * @param value Value to compare against
         * @return A cached or new Compare condition
         */
        public static Compare cached(String column, String operator, String variable, Object value) {
            String cacheKey = "compare:" + column + ":" + operator + ":" + variable + ":" + (value != null ? value.toString() : "null");
            return (Compare) getCachedOrCreate(cacheKey, () -> new Compare(column, operator, variable, value));
        }
    }

    /**
     * Between condition that checks if a column value is between two values.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     */
    class Between implements Condition {
        private final String column;
        private final String fromVariable;
        private final Object fromValue;
        private final String toVariable;
        private final Object toValue;

        public Between(String column, String fromVariable, Object fromValue, String toVariable, Object toValue) {
            Span span = createSpan("create_between_condition", "between", 
                    Attributes.of(AttributeKey.stringKey("condition.column"), column));
            try (Scope scope = span.makeCurrent()) {
                this.column = column;
                this.fromVariable = fromVariable;
                this.fromValue = fromValue;
                this.toVariable = toVariable;
                this.toValue = toValue;
                span.addEvent("Between condition created");
            } finally {
                span.end();
            }
        }

        public String getColumn() {
            return column;
        }

        public String getFromVariable() {
            return fromVariable;
        }

        public Object getFromValue() {
            return fromValue;
        }

        public String getToVariable() {
            return toVariable;
        }

        public Object getToValue() {
            return toValue;
        }
        
        /**
         * Creates a cached between condition to optimize resource usage.
         * 
         * @param column Column name to check
         * @param fromVariable From variable name
         * @param fromValue From value
         * @param toVariable To variable name
         * @param toValue To value
         * @return A cached or new Between condition
         */
        public static Between cached(String column, String fromVariable, Object fromValue, String toVariable, Object toValue) {
            String cacheKey = "between:" + column + ":" + fromVariable + ":" + 
                    (fromValue != null ? fromValue.toString() : "null") + ":" + 
                    toVariable + ":" + (toValue != null ? toValue.toString() : "null");
            return (Between) getCachedOrCreate(cacheKey, () -> new Between(column, fromVariable, fromValue, toVariable, toValue));
        }
    }

    /**
     * OR condition that combines two conditions with OR operator.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     */
    class Or extends Binary {
        public Or(Condition first, Condition second) {
            super(first, second, "OR");
        }
        
        /**
         * Creates a cached OR condition to optimize resource usage.
         * 
         * @param first First condition
         * @param second Second condition
         * @return A cached or new Or condition
         */
        public static Or cached(Condition first, Condition second) {
            // Use hash codes for cache key since conditions may not have good toString implementations
            String cacheKey = "or:" + first.hashCode() + ":" + second.hashCode();
            return (Or) getCachedOrCreate(cacheKey, () -> new Or(first, second));
        }
    }

    /**
     * AND condition that combines two conditions with AND operator.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     */
    class And extends Binary {
        public And(Condition first, Condition second) {
            super(first, second, "AND");
        }
        
        /**
         * Creates a cached AND condition to optimize resource usage.
         * 
         * @param first First condition
         * @param second Second condition
         * @return A cached or new And condition
         */
        public static And cached(Condition first, Condition second) {
            // Use hash codes for cache key since conditions may not have good toString implementations
            String cacheKey = "and:" + first.hashCode() + ":" + second.hashCode();
            return (And) getCachedOrCreate(cacheKey, () -> new And(first, second));
        }
    }

    /**
     * Binary condition that combines two conditions with an operator.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     */
    class Binary implements Condition {
        private final Condition first;
        private final Condition second;
        private final String operator;

        public Binary(Condition first, Condition second, String operator) {
            Span span = createSpan("create_binary_condition", "binary", 
                    Attributes.of(AttributeKey.stringKey("condition.operator"), operator));
            try (Scope scope = span.makeCurrent()) {
                this.first = first;
                this.second = second;
                this.operator = operator;
                span.addEvent("Binary condition created");
            } finally {
                span.end();
            }
        }

        public Condition getFirst() {
            return first;
        }

        public Condition getSecond() {
            return second;
        }

        public String getOperator() {
            return operator;
        }
    }

    /**
     * Permission condition for checking access permissions.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     */
    class Permission implements Condition {
        private final Class<?> ownerClass;
        private final long ownerId;
        private final Class<?> propertyClass;
        private final long propertyId;
        private final boolean excludeGroups;

        private Permission(
                Class<?> ownerClass, long ownerId, Class<?> propertyClass, long propertyId, boolean excludeGroups) {
            Span span = createSpan("create_permission_condition", "permission", 
                    Attributes.of(
                        AttributeKey.stringKey("condition.ownerClass"), ownerClass.getSimpleName(),
                        AttributeKey.stringKey("condition.propertyClass"), propertyClass.getSimpleName(),
                        AttributeKey.longKey("condition.ownerId"), ownerId,
                        AttributeKey.longKey("condition.propertyId"), propertyId,
                        AttributeKey.booleanKey("condition.excludeGroups"), excludeGroups));
            try (Scope scope = span.makeCurrent()) {
                this.ownerClass = ownerClass;
                this.ownerId = ownerId;
                this.propertyClass = propertyClass;
                this.propertyId = propertyId;
                this.excludeGroups = excludeGroups;
                span.addEvent("Permission condition created");
            } finally {
                span.end();
            }
        }

        public Permission(Class<?> ownerClass, long ownerId, Class<?> propertyClass) {
            this(ownerClass, ownerId, propertyClass, 0, false);
        }

        public Permission(Class<?> ownerClass, Class<?> propertyClass, long propertyId) {
            this(ownerClass, 0, propertyClass, propertyId, false);
        }

        public Permission excludeGroups() {
            return new Permission(this.ownerClass, this.ownerId, this.propertyClass, this.propertyId, true);
        }

        public Class<?> getOwnerClass() {
            return ownerClass;
        }

        public long getOwnerId() {
            return ownerId;
        }

        public Class<?> getPropertyClass() {
            return propertyClass;
        }

        public long getPropertyId() {
            return propertyId;
        }

        public boolean getIncludeGroups() {
            Span span = createSpan("evaluate_permission_groups", "permission", 
                    Attributes.of(
                        AttributeKey.stringKey("condition.ownerClass"), ownerClass.getSimpleName(),
                        AttributeKey.stringKey("condition.propertyClass"), propertyClass.getSimpleName()));
            try (Scope scope = span.makeCurrent()) {
                boolean ownerGroupModel = GroupedModel.class.isAssignableFrom(ownerClass);
                boolean propertyGroupModel = GroupedModel.class.isAssignableFrom(propertyClass);
                boolean result = (ownerGroupModel || propertyGroupModel) && !excludeGroups;
                span.setAttribute("condition.includeGroups", result);
                return result;
            } finally {
                span.end();
            }
        }
        
        /**
         * Creates a cached permission condition to optimize resource usage.
         * 
         * @param ownerClass Owner class
         * @param ownerId Owner ID
         * @param propertyClass Property class
         * @param propertyId Property ID
         * @param excludeGroups Whether to exclude groups
         * @return A cached or new Permission condition
         */
        public static Permission cached(
                Class<?> ownerClass, long ownerId, Class<?> propertyClass, long propertyId, boolean excludeGroups) {
            String cacheKey = "permission:" + ownerClass.getName() + ":" + ownerId + ":" + 
                    propertyClass.getName() + ":" + propertyId + ":" + excludeGroups;
            return (Permission) getCachedOrCreate(cacheKey, 
                    () -> new Permission(ownerClass, ownerId, propertyClass, propertyId, excludeGroups));
        }
    }

    /**
     * LatestPositions condition for retrieving latest positions.
     * Enhanced with OpenTelemetry tracing for performance monitoring.
     */
    class LatestPositions implements Condition {
        private final long deviceId;

        public LatestPositions(long deviceId) {
            Span span = createSpan("create_latest_positions_condition", "latest_positions", 
                    Attributes.of(AttributeKey.longKey("condition.deviceId"), deviceId));
            try (Scope scope = span.makeCurrent()) {
                this.deviceId = deviceId;
                span.addEvent("LatestPositions condition created");
            } finally {
                span.end();
            }
        }

        public LatestPositions() {
            this(0);
        }

        public long getDeviceId() {
            return deviceId;
        }
        
        /**
         * Creates a cached latest positions condition to optimize resource usage.
         * 
         * @param deviceId Device ID
         * @return A cached or new LatestPositions condition
         */
        public static LatestPositions cached(long deviceId) {
            String cacheKey = "latestPositions:" + deviceId;
            return (LatestPositions) getCachedOrCreate(cacheKey, () -> new LatestPositions(deviceId));
        }
    }
    
    /**
     * ServiceSpecific condition for service-specific filtering.
     * Supports distributed database patterns across microservices.
     */
    class ServiceSpecific implements Condition {
        private final String serviceName;
        private final Map<String, Object> parameters;
        
        public ServiceSpecific(String serviceName, Map<String, Object> parameters) {
            Span span = createSpan("create_service_specific_condition", "service_specific", 
                    Attributes.of(AttributeKey.stringKey("condition.serviceName"), serviceName));
            try (Scope scope = span.makeCurrent()) {
                this.serviceName = serviceName;
                this.parameters = parameters;
                span.addEvent("ServiceSpecific condition created");
            } finally {
                span.end();
            }
        }
        
        public String getServiceName() {
            return serviceName;
        }
        
        public Map<String, Object> getParameters() {
            return parameters;
        }
        
        /**
         * Creates a cached service-specific condition to optimize resource usage.
         * 
         * @param serviceName Service name
         * @param parameters Service-specific parameters
         * @return A cached or new ServiceSpecific condition
         */
        public static ServiceSpecific cached(String serviceName, Map<String, Object> parameters) {
            // Create a deterministic string representation of parameters for cache key
            StringBuilder paramString = new StringBuilder();
            parameters.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(e -> paramString.append(e.getKey())
                            .append(":")
                            .append(e.getValue() != null ? e.getValue().toString() : "null")
                            .append(";"));
            
            String cacheKey = "serviceSpecific:" + serviceName + ":" + paramString;
            return (ServiceSpecific) getCachedOrCreate(cacheKey, () -> new ServiceSpecific(serviceName, parameters));
        }
    }

}