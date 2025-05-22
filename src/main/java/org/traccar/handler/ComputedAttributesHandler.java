/*
 * Copyright 2017 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.handler;

import jakarta.inject.Inject;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlException;
import org.apache.commons.jexl3.JexlFeatures;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.introspection.JexlSandbox;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Attribute;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

// Resilience4j imports for circuit breaker
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

// OpenTelemetry imports for distributed tracing
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

// Micrometer imports for metrics
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

// Messaging imports for async processing
import org.traccar.messaging.MessagePublisher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class ComputedAttributesHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ComputedAttributesHandler.class);

    private final CacheManager cacheManager;
    private final boolean early;

    private final JexlEngine engine;

    private final JexlFeatures features;

    private final boolean includeDeviceAttributes;
    private final boolean includeLastAttributes;
    
    // Circuit breaker for expression evaluation
    private final CircuitBreaker circuitBreaker;
    
    // Tracer for distributed tracing
    private final Tracer tracer;
    
    // Metrics registry for performance monitoring
    private final MeterRegistry meterRegistry;
    
    // Message publisher for async processing
    private final MessagePublisher messagePublisher;
    
    // Executor for async operations
    private final Executor executor;
    
    // Metrics
    private final Timer computeTimer;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Counter circuitBreakerOpenCounter;

    public static class Early extends ComputedAttributesHandler {
        @Inject
        public Early(Config config, CacheManager cacheManager, 
                    CircuitBreakerRegistry circuitBreakerRegistry,
                    Tracer tracer, MeterRegistry meterRegistry,
                    MessagePublisher messagePublisher, Executor executor) {
            super(config, cacheManager, true, circuitBreakerRegistry, tracer, meterRegistry, messagePublisher, executor);
        }
    }

    public static class Late extends ComputedAttributesHandler {
        @Inject
        public Late(Config config, CacheManager cacheManager,
                   CircuitBreakerRegistry circuitBreakerRegistry,
                   Tracer tracer, MeterRegistry meterRegistry,
                   MessagePublisher messagePublisher, Executor executor) {
            super(config, cacheManager, false, circuitBreakerRegistry, tracer, meterRegistry, messagePublisher, executor);
        }
    }

    public ComputedAttributesHandler(Config config, CacheManager cacheManager, boolean early) {
        this(config, cacheManager, early, null, null, null, null, null);
    }

    public ComputedAttributesHandler(Config config, CacheManager cacheManager, boolean early,
                                    CircuitBreakerRegistry circuitBreakerRegistry,
                                    Tracer tracer, MeterRegistry meterRegistry,
                                    MessagePublisher messagePublisher, Executor executor) {
        this.cacheManager = cacheManager;
        this.early = early;
        JexlSandbox sandbox = new JexlSandbox(false);
        sandbox.allow("com.safe.Functions");
        sandbox.allow(Math.class.getName());
        List.of(
            Double.class, Float.class, Integer.class, Long.class, Short.class,
            Character.class, Boolean.class, String.class, Byte.class, Date.class,
            HashMap.class, LinkedHashMap.class, double[].class, int[].class, boolean[].class, String[].class)
                .forEach((type) -> sandbox.allow(type.getName()));
        features = new JexlFeatures()
                .localVar(config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_LOCAL_VARIABLES))
                .loops(config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_LOOPS))
                .newInstance(config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_NEW_INSTANCE_CREATION))
                .structuredLiteral(true);
        engine = new JexlBuilder()
                .strict(true)
                .namespaces(Collections.singletonMap("math", Math.class))
                .sandbox(sandbox)
                .create();
        includeDeviceAttributes = config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_DEVICE_ATTRIBUTES);
        includeLastAttributes = config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_LAST_ATTRIBUTES);
        
        // Initialize circuit breaker if registry is provided
        if (circuitBreakerRegistry != null) {
            CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to trip the circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Consider the last 10 calls
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .build();
            this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                "computedAttributes" + (early ? "Early" : "Late"), circuitBreakerConfig);
        } else {
            this.circuitBreaker = null;
        }
        
        // Initialize tracer
        this.tracer = tracer;
        
        // Initialize metrics registry and create metrics
        this.meterRegistry = meterRegistry;
        if (meterRegistry != null) {
            String handlerType = early ? "early" : "late";
            this.computeTimer = Timer.builder("computed.attributes.compute.time")
                .tag("type", handlerType)
                .description("Time taken to compute attributes")
                .register(meterRegistry);
            this.successCounter = Counter.builder("computed.attributes.success")
                .tag("type", handlerType)
                .description("Number of successful attribute computations")
                .register(meterRegistry);
            this.failureCounter = Counter.builder("computed.attributes.failure")
                .tag("type", handlerType)
                .description("Number of failed attribute computations")
                .register(meterRegistry);
            this.circuitBreakerOpenCounter = Counter.builder("computed.attributes.circuit.breaker.open")
                .tag("type", handlerType)
                .description("Number of times the circuit breaker was open")
                .register(meterRegistry);
        } else {
            this.computeTimer = null;
            this.successCounter = null;
            this.failureCounter = null;
            this.circuitBreakerOpenCounter = null;
        }
        
        // Initialize message publisher for async processing
        this.messagePublisher = messagePublisher;
        
        // Initialize executor for async operations
        this.executor = executor;
    }

    private MapContext prepareContext(Position position) {
        MapContext result = new MapContext();
        if (includeDeviceAttributes) {
            Device device = cacheManager.getObject(Device.class, position.getDeviceId());
            if (device != null) {
                for (String key : device.getAttributes().keySet()) {
                    result.set(key, device.getAttributes().get(key));
                }
            }
        }
        Position last = null;
        if (includeLastAttributes) {
            last = cacheManager.getPosition(position.getDeviceId());
        }
        Set<Method> methods = new HashSet<>(Arrays.asList(position.getClass().getMethods()));
        Arrays.asList(Object.class.getMethods()).forEach(methods::remove);
        for (Method method : methods) {
            if (method.getName().startsWith("get") && method.getParameterTypes().length == 0) {
                String name = Character.toLowerCase(method.getName().charAt(3)) + method.getName().substring(4);

                try {
                    if (!method.getReturnType().equals(Map.class)) {
                        result.set(name, method.invoke(position));
                        if (last != null) {
                            result.set(prefixAttribute("last", name), method.invoke(last));
                        }
                    } else {
                        for (Map.Entry<?, ?> entry : ((Map<?, ?>) method.invoke(position)).entrySet()) {
                            result.set((String) entry.getKey(), entry.getValue());
                        }
                        if (last != null) {
                            for (Map.Entry<?, ?> entry : ((Map<?, ?>) method.invoke(last)).entrySet()) {
                                result.set(prefixAttribute("last", (String) entry.getKey()), entry.getValue());
                            }
                        }
                    }
                } catch (IllegalAccessException | InvocationTargetException error) {
                    LOGGER.warn("Attribute reflection error", error);
                }
            }
        }
        return result;
    }

    private String prefixAttribute(String prefix, String key) {
        return prefix + Character.toUpperCase(key.charAt(0)) + key.substring(1);
    }

    /**
     * @deprecated logic needs to be extracted to be used in API resource
     */
    @Deprecated
    public Object computeAttribute(Attribute attribute, Position position) throws JexlException {
        return computeAttributeWithResilience(attribute, position);
    }
    
    /**
     * Compute attribute with resilience patterns (circuit breaker, tracing, metrics)
     */
    private Object computeAttributeWithResilience(Attribute attribute, Position position) {
        // Create a span for this computation if tracing is enabled
        Span span = null;
        Scope scope = null;
        
        if (tracer != null) {
            span = tracer.spanBuilder("computeAttribute")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("attribute.name", attribute.getAttribute())
                .setAttribute("attribute.expression", attribute.getExpression())
                .setAttribute("device.id", String.valueOf(position.getDeviceId()))
                .startSpan();
            scope = span.makeCurrent();
        }
        
        try {
            // Use circuit breaker if available, otherwise execute directly
            if (circuitBreaker != null) {
                try {
                    return circuitBreaker.executeSupplier(() -> {
                        // Record metrics if available
                        if (computeTimer != null) {
                            return computeTimer.record(() -> {
                                try {
                                    Object result = evaluateExpression(attribute, position);
                                    if (successCounter != null) {
                                        successCounter.increment();
                                    }
                                    return result;
                                } catch (Exception e) {
                                    if (failureCounter != null) {
                                        failureCounter.increment();
                                    }
                                    throw e;
                                }
                            });
                        } else {
                            return evaluateExpression(attribute, position);
                        }
                    });
                } catch (Exception e) {
                    if (circuitBreakerOpenCounter != null && 
                        circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                        circuitBreakerOpenCounter.increment();
                    }
                    
                    // Record error in span if tracing is enabled
                    if (span != null) {
                        span.recordException(e)
                            .setStatus(StatusCode.ERROR, e.getMessage());
                    }
                    
                    // Graceful degradation - return fallback value
                    return getFallbackValue(attribute, e);
                }
            } else {
                // No circuit breaker, execute directly with metrics if available
                if (computeTimer != null) {
                    return computeTimer.record(() -> {
                        try {
                            Object result = evaluateExpression(attribute, position);
                            if (successCounter != null) {
                                successCounter.increment();
                            }
                            return result;
                        } catch (Exception e) {
                            if (failureCounter != null) {
                                failureCounter.increment();
                            }
                            
                            // Record error in span if tracing is enabled
                            if (span != null) {
                                span.recordException(e)
                                    .setStatus(StatusCode.ERROR, e.getMessage());
                            }
                            
                            // Graceful degradation - return fallback value
                            return getFallbackValue(attribute, e);
                        }
                    });
                } else {
                    try {
                        return evaluateExpression(attribute, position);
                    } catch (Exception e) {
                        // Record error in span if tracing is enabled
                        if (span != null) {
                            span.recordException(e)
                                .setStatus(StatusCode.ERROR, e.getMessage());
                        }
                        
                        // Graceful degradation - return fallback value
                        return getFallbackValue(attribute, e);
                    }
                }
            }
        } finally {
            // Close the span and scope if tracing is enabled
            if (scope != null) {
                scope.close();
            }
            if (span != null) {
                span.end();
            }
        }
    }
    
    /**
     * Evaluate the expression using the JEXL engine
     */
    private Object evaluateExpression(Attribute attribute, Position position) {
        return engine
                .createScript(features, engine.createInfo(), attribute.getExpression())
                .execute(prepareContext(position));
    }
    
    /**
     * Get a fallback value for an attribute when computation fails
     */
    private Object getFallbackValue(Attribute attribute, Exception e) {
        LOGGER.warn("Attribute computation error with graceful degradation", e);
        
        // Return null by default, which will cause the attribute to be removed
        // This could be enhanced to return a default value based on attribute type
        return null;
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a parent span for the entire operation if tracing is enabled
        Span parentSpan = null;
        Scope parentScope = null;
        
        if (tracer != null) {
            parentSpan = tracer.spanBuilder("computedAttributesHandler.onPosition")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("handler.type", early ? "early" : "late")
                .setAttribute("device.id", String.valueOf(position.getDeviceId()))
                .startSpan();
            parentScope = parentSpan.makeCurrent();
        }
        
        try {
            // Check if we should process asynchronously
            boolean processAsync = messagePublisher != null && executor != null;
            
            // Get attributes to process
            var attributes = cacheManager.getDeviceObjects(position.getDeviceId(), Attribute.class).stream()
                    .filter(attribute -> attribute.getPriority() < 0 == early)
                    .sorted(Comparator.comparing(Attribute::getPriority).reversed())
                    .toList();
            
            if (processAsync) {
                // Process asynchronously via message broker
                CompletableFuture.runAsync(() -> {
                    processAttributes(attributes, position);
                }, executor).thenRun(() -> {
                    callback.processed(false);
                });
            } else {
                // Process synchronously
                processAttributes(attributes, position);
                callback.processed(false);
            }
        } finally {
            // Close the parent span and scope if tracing is enabled
            if (parentScope != null) {
                parentScope.close();
            }
            if (parentSpan != null) {
                parentSpan.end();
            }
        }
    }
    
    /**
     * Process attributes for a position
     */
    private void processAttributes(List<Attribute> attributes, Position position) {
        for (Attribute attribute : attributes) {
            if (attribute.getAttribute() != null) {
                try {
                    Object result = computeAttributeWithResilience(attribute, position);
                    if (result != null) {
                        switch (attribute.getAttribute()) {
                            case "valid" -> position.setValid((Boolean) result);
                            case "latitude" -> position.setLatitude(((Number) result).doubleValue());
                            case "longitude" -> position.setLongitude(((Number) result).doubleValue());
                            case "altitude" -> position.setAltitude(((Number) result).doubleValue());
                            case "speed" -> position.setSpeed(((Number) result).doubleValue());
                            case "course" -> position.setCourse(((Number) result).doubleValue());
                            case "address" -> position.setAddress((String) result);
                            case "accuracy" -> position.setAccuracy(((Number) result).doubleValue());
                            default -> {
                                switch (attribute.getType()) {
                                    case "number" -> {
                                        Number numberValue = (Number) result;
                                        position.getAttributes().put(attribute.getAttribute(), numberValue);
                                    }
                                    case "boolean" -> {
                                        Boolean booleanValue = (Boolean) result;
                                        position.getAttributes().put(attribute.getAttribute(), booleanValue);
                                    }
                                    default -> {
                                        position.getAttributes().put(attribute.getAttribute(), result.toString());
                                    }
                                }
                            }
                        }
                    } else {
                        position.removeAttribute(attribute.getAttribute());
                    }
                } catch (Exception error) {
                    LOGGER.warn("Attribute computation error", error);
                }
            }
        }
    }
}