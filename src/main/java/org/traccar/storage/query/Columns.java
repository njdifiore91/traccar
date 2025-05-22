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
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;

import org.traccar.storage.QueryIgnore;

import java.beans.Introspector;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Provides column discovery operations for database queries.
 * Supports OpenTelemetry tracing for performance monitoring and
 * optimized memory usage for containerized environments.
 */
public abstract class Columns {

    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("org.traccar.storage.query");
    private static final Map<String, List<String>> COLUMN_CACHE = new ConcurrentHashMap<>();
    
    /**
     * Gets the list of columns for a class and operation type.
     * 
     * @param clazz The class to get columns for
     * @param type The operation type ("get" or "set")
     * @return List of column names
     */
    public abstract List<String> getColumns(Class<?> clazz, String type);

    /**
     * Gets all columns for a class and operation type with OpenTelemetry tracing.
     * Uses caching to optimize memory usage in containerized environments.
     * 
     * @param clazz The class to get columns for
     * @param type The operation type ("get" or "set")
     * @return List of column names
     */
    protected List<String> getAllColumns(Class<?> clazz, String type) {
        String cacheKey = clazz.getName() + "-" + type;
        
        // Check cache first to optimize memory usage
        List<String> cachedColumns = COLUMN_CACHE.get(cacheKey);
        if (cachedColumns != null) {
            return new ArrayList<>(cachedColumns);
        }
        
        // Create a span for column discovery operation
        Span span = TRACER.spanBuilder("columns.discover")
                .setAttribute("class.name", clazz.getName())
                .setAttribute("operation.type", type)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            List<String> columns = new ArrayList<>();
            Method[] methods = clazz.getMethods();
            
            span.setAttribute("methods.count", methods.length);
            
            for (Method method : methods) {
                int parameterCount = type.equals("set") ? 1 : 0;
                if (method.getName().startsWith(type) && method.getParameterTypes().length == parameterCount
                        && !method.isAnnotationPresent(QueryIgnore.class)
                        && !method.getName().equals("getClass")) {
                    columns.add(Introspector.decapitalize(method.getName().substring(3)));
                }
            }
            
            span.setAttribute("columns.count", columns.size());
            
            // Cache the result to optimize future calls
            COLUMN_CACHE.put(cacheKey, new ArrayList<>(columns));
            
            span.setStatus(StatusCode.OK);
            return columns;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Returns all columns for a class.
     */
    public static class All extends Columns {
        @Override
        public List<String> getColumns(Class<?> clazz, String type) {
            return getAllColumns(clazz, type);
        }
    }

    /**
     * Returns only the specified columns.
     * Optimized for microservice-specific column selection patterns.
     */
    public static class Include extends Columns {
        private final List<String> columns;

        public Include(String... columns) {
            this.columns = Arrays.stream(columns).collect(Collectors.toList());
        }

        @Override
        public List<String> getColumns(Class<?> clazz, String type) {
            Span span = TRACER.spanBuilder("columns.include")
                    .setAttribute("class.name", clazz.getName())
                    .setAttribute("operation.type", type)
                    .setAttribute("columns.count", columns.size())
                    .startSpan();
            try {
                return columns;
            } finally {
                span.end();
            }
        }
    }

    /**
     * Returns all columns except the specified ones.
     * Supports distributed database access patterns with consistent column naming.
     */
    public static class Exclude extends Columns {
        private final Set<String> columns;

        public Exclude(String... columns) {
            this.columns = Arrays.stream(columns).collect(Collectors.toSet());
        }

        @Override
        public List<String> getColumns(Class<?> clazz, String type) {
            Span span = TRACER.spanBuilder("columns.exclude")
                    .setAttribute("class.name", clazz.getName())
                    .setAttribute("operation.type", type)
                    .setAttribute("excluded.count", columns.size())
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                List<String> result = getAllColumns(clazz, type).stream()
                        .filter(column -> !columns.contains(column))
                        .collect(Collectors.toList());
                
                span.setAttribute("result.count", result.size());
                return result;
            } finally {
                span.end();
            }
        }
    }

    /**
     * Creates a service-specific column selector for microservices.
     * Supports consistent column naming across distributed services.
     * 
     * @param serviceName The name of the service
     * @param columns The columns to include
     * @return A columns instance for the service
     */
    public static Columns forService(String serviceName, String... columns) {
        return new ServiceSpecific(serviceName, columns);
    }
    
    /**
     * Service-specific column selection for microservices architecture.
     */
    public static class ServiceSpecific extends Columns {
        private final String serviceName;
        private final List<String> columns;
        
        public ServiceSpecific(String serviceName, String... columns) {
            this.serviceName = serviceName;
            this.columns = Arrays.stream(columns).collect(Collectors.toList());
        }
        
        @Override
        public List<String> getColumns(Class<?> clazz, String type) {
            Span span = TRACER.spanBuilder("columns.service")
                    .setAttribute("service.name", serviceName)
                    .setAttribute("class.name", clazz.getName())
                    .setAttribute("operation.type", type)
                    .setAttribute("columns.count", columns.size())
                    .startSpan();
            try {
                return columns;
            } finally {
                span.end();
            }
        }
    }
}