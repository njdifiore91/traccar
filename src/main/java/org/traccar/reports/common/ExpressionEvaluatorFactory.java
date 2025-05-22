package org.traccar.reports.common;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.introspection.JexlPermissions;
import org.jxls.expression.ExpressionEvaluator;
import org.jxls.expression.JexlExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Factory for creating expression evaluators with enhanced features:
 * - OpenTelemetry instrumentation for tracing
 * - Performance metrics collection
 * - Expression caching
 * - Improved error handling for distributed environments
 */
public class ExpressionEvaluatorFactory implements org.jxls.expression.ExpressionEvaluatorFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionEvaluatorFactory.class);
    
    private final JexlPermissions permissions = new JexlPermissions() {
        @Override
        public boolean allow(Package pack) {
            return true;
        }

        @Override
        public boolean allow(Class<?> clazz) {
            return true;
        }

        @Override
        public boolean allow(Constructor<?> ctor) {
            return true;
        }

        @Override
        public boolean allow(Method method) {
            return true;
        }

        @Override
        public boolean allow(Field field) {
            return true;
        }

        @Override
        public JexlPermissions compose(String... src) {
            return this;
        }
    };
    
    // Cache for compiled expressions to improve performance
    private final Map<String, ExpressionEvaluator> expressionCache = new ConcurrentHashMap<>();
    
    // OpenTelemetry tracer for distributed tracing
    private final Tracer tracer;
    
    // Micrometer registry for metrics collection
    private final MeterRegistry meterRegistry;
    
    // Circuit breaker for fault tolerance
    private final CircuitBreaker circuitBreaker;
    
    // Maximum cache size, configurable via environment variable
    private final int maxCacheSize;
    
    // Cache enabled flag, configurable via environment variable
    private final boolean cacheEnabled;
    
    /**
     * Constructor with default configuration.
     */
    public ExpressionEvaluatorFactory() {
        this(null, null);
    }
    
    /**
     * Constructor with OpenTelemetry and MeterRegistry for instrumentation.
     * 
     * @param openTelemetry OpenTelemetry instance for tracing
     * @param meterRegistry MeterRegistry instance for metrics
     */
    public ExpressionEvaluatorFactory(OpenTelemetry openTelemetry, MeterRegistry meterRegistry) {
        // Initialize OpenTelemetry tracer if provided
        this.tracer = openTelemetry != null ? 
                openTelemetry.getTracer("org.traccar.reports.common.ExpressionEvaluatorFactory") : null;
        
        // Initialize MeterRegistry if provided
        this.meterRegistry = meterRegistry;
        
        // Configure cache size from environment variable or use default
        String maxCacheSizeEnv = System.getenv("EXPRESSION_EVALUATOR_MAX_CACHE_SIZE");
        this.maxCacheSize = maxCacheSizeEnv != null ? Integer.parseInt(maxCacheSizeEnv) : 1000;
        
        // Configure cache enabled flag from environment variable or use default
        String cacheEnabledEnv = System.getenv("EXPRESSION_EVALUATOR_CACHE_ENABLED");
        this.cacheEnabled = cacheEnabledEnv == null || Boolean.parseBoolean(cacheEnabledEnv);
        
        // Initialize circuit breaker for fault tolerance
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(TimeoutException.class, RuntimeException.class)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("expressionEvaluator");
        
        LOGGER.info("ExpressionEvaluatorFactory initialized with cacheEnabled={}, maxCacheSize={}", 
                cacheEnabled, maxCacheSize);
    }

    @Override
    public ExpressionEvaluator createExpressionEvaluator(String expression) {
        // If expression is null, create a new evaluator without caching
        if (expression == null) {
            return createNewEvaluator(null);
        }
        
        // Use circuit breaker to handle failures gracefully
        return circuitBreaker.executeSupplier(() -> {
            Span span = null;
            Scope scope = null;
            Timer.Sample timerSample = null;
            
            try {
                // Start OpenTelemetry span if tracer is available
                if (tracer != null) {
                    span = tracer.spanBuilder("createExpressionEvaluator")
                            .setSpanKind(SpanKind.INTERNAL)
                            .setAttribute(AttributeKey.stringKey("expression"), expression)
                            .startSpan();
                    scope = span.makeCurrent();
                }
                
                // Start timer for metrics if registry is available
                if (meterRegistry != null) {
                    timerSample = Timer.start(meterRegistry);
                }
                
                // Check cache if enabled
                if (cacheEnabled) {
                    // Clean cache if it exceeds maximum size
                    if (expressionCache.size() > maxCacheSize) {
                        LOGGER.debug("Expression cache size exceeded maximum ({}), clearing cache", maxCacheSize);
                        expressionCache.clear();
                    }
                    
                    // Return cached evaluator if available
                    ExpressionEvaluator cachedEvaluator = expressionCache.get(expression);
                    if (cachedEvaluator != null) {
                        if (span != null) {
                            span.setAttribute("cache.hit", true);
                        }
                        return cachedEvaluator;
                    }
                    
                    if (span != null) {
                        span.setAttribute("cache.hit", false);
                    }
                }
                
                // Create new evaluator
                ExpressionEvaluator evaluator = createNewEvaluator(expression);
                
                // Cache the evaluator if caching is enabled
                if (cacheEnabled) {
                    expressionCache.put(expression, evaluator);
                }
                
                return evaluator;
            } catch (Exception e) {
                // Record error in span
                if (span != null) {
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                }
                
                LOGGER.error("Error creating expression evaluator for expression: {}", expression, e);
                throw e;
            } finally {
                // Record metrics
                if (timerSample != null && meterRegistry != null) {
                    timerSample.stop(meterRegistry.timer("expression.evaluator.creation", 
                            "cached", String.valueOf(cacheEnabled)));
                }
                
                // Close OpenTelemetry span
                if (scope != null) {
                    scope.close();
                }
                if (span != null) {
                    span.end();
                }
            }
        });
    }
    
    /**
     * Creates a new JexlExpressionEvaluator with the given expression.
     * 
     * @param expression The expression to evaluate, can be null
     * @return A new ExpressionEvaluator instance
     */
    private ExpressionEvaluator createNewEvaluator(String expression) {
        JexlExpressionEvaluator expressionEvaluator = expression == null
                ? new JexlExpressionEvaluator()
                : new JexlExpressionEvaluator(expression);
        
        expressionEvaluator.setJexlEngine(new JexlBuilder()
                .silent(true)
                .strict(false)
                .permissions(permissions)
                .create());
        
        return expressionEvaluator;
    }
    
    /**
     * Clears the expression cache.
     */
    public void clearCache() {
        expressionCache.clear();
        LOGGER.debug("Expression cache cleared");
    }
    
    /**
     * Gets the current size of the expression cache.
     * 
     * @return The number of cached expressions
     */
    public int getCacheSize() {
        return expressionCache.size();
    }
}