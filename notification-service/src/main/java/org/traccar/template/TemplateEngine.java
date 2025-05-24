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
package org.traccar.template;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.DateTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.signature.TokenManager;
import org.traccar.helper.model.UserUtil;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.notification.NotificationMessage;
import org.traccar.storage.StorageException;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * TemplateEngine is the main entry point for template processing in the Notification Service.
 * It provides functionality for loading templates from various sources, building template contexts,
 * and rendering templates with proper monitoring, resilience, and tracing.
 */
@Singleton
public class TemplateEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateEngine.class);
    
    /**
     * Enum representing different template sources
     */
    public enum TemplateSource {
        FILESYSTEM,    // Templates stored on the filesystem
        CLASSPATH,     // Templates stored in the classpath
        DATABASE       // Templates stored in the database
    }
    
    private final VelocityEngine velocityEngine;
    private final TokenManager tokenManager;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;
    private final Executor templateProcessingExecutor;
    
    // Template cache to improve performance
    private final Map<String, Template> templateCache = new ConcurrentHashMap<>();
    
    // Default template paths
    private final Map<TemplateSource, String> templatePaths = new HashMap<>();

    /**
     * Creates a new TemplateEngine with the specified dependencies.
     *
     * @param velocityEngine The Velocity engine for template processing
     * @param tokenManager The token manager for generating user tokens
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param circuitBreakerRegistry The circuit breaker registry for resilience patterns
     */
    @Inject
    public TemplateEngine(
            VelocityEngine velocityEngine,
            TokenManager tokenManager,
            MeterRegistry meterRegistry,
            Tracer tracer,
            CircuitBreakerRegistry circuitBreakerRegistry) {
        
        this.velocityEngine = velocityEngine;
        this.tokenManager = tokenManager;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Configure circuit breaker for template processing with appropriate thresholds
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to trip the circuit
                .slowCallRateThreshold(50) // 50% slow calls to trip the circuit
                .slowCallDurationThreshold(Duration.ofSeconds(2)) // Calls slower than 2s are considered slow
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30s before trying again
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .minimumNumberOfCalls(10) // Minimum calls before calculating failure rate
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED) // Based on call count
                .slidingWindowSize(100) // Consider the last 100 calls
                .automaticTransitionFromOpenToHalfOpenEnabled(true) // Auto transition to half-open
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("templateProcessing", circuitBreakerConfig);
        
        // Register circuit breaker event listeners for monitoring
        circuitBreaker.getEventPublisher()
                .onSuccess(event -> meterRegistry.counter("circuit.breaker.success").increment())
                .onError(event -> meterRegistry.counter(
                        "circuit.breaker.error",
                        "error", event.getThrowable().getClass().getSimpleName()).increment())
                .onStateTransition(event -> {
                    LOGGER.info("Circuit breaker state changed from {} to {}", 
                            event.getStateTransition().getFromState(),
                            event.getStateTransition().getToState());
                    meterRegistry.counter(
                            "circuit.breaker.state.transition",
                            "from", event.getStateTransition().getFromState().name(),
                            "to", event.getStateTransition().getToState().name()).increment();
                });
        
        // Create a dedicated thread pool for template processing
        this.templateProcessingExecutor = Executors.newVirtualThreadPerTaskExecutor();
        
        // Initialize default template paths
        templatePaths.put(TemplateSource.FILESYSTEM, "templates");
        templatePaths.put(TemplateSource.CLASSPATH, "templates");
        templatePaths.put(TemplateSource.DATABASE, "");
        
        // Register metrics
        meterRegistry.gauge("template.cache.size", templateCache, Map::size);
        meterRegistry.gauge("template.sources.count", templatePaths, Map::size);
        
        LOGGER.info("TemplateEngine initialized with circuit breaker and metrics");
    }
    
    /**
     * Sets the template path for a specific template source.
     *
     * @param source The template source
     * @param path The path to templates for the specified source
     */
    public void setTemplatePath(TemplateSource source, String path) {
        templatePaths.put(source, path);
    }
    
    /**
     * Clears the template cache.
     */
    public void clearCache() {
        templateCache.clear();
    }
    
    /**
     * Prepares a Velocity context with standard variables and user-specific data.
     *
     * @param server The server configuration
     * @param user The user for whom the context is being prepared
     * @return A VelocityContext with standard variables
     */
    public VelocityContext prepareContext(Server server, User user) {
        Span span = tracer.spanBuilder("prepareTemplateContext")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        Timer.Sample contextTimer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            VelocityContext velocityContext = new VelocityContext();

            if (user != null) {
                span.setAttribute("user.id", user.getId());
                velocityContext.put("user", user);
                velocityContext.put("timezone", UserUtil.getTimezone(server, user));
                try {
                    String token = tokenManager.generateToken(user.getId());
                    velocityContext.put("token", token);
                    span.setAttribute("token.generated", true);
                } catch (IOException | GeneralSecurityException | StorageException e) {
                    LOGGER.warn("Token generation failed for user {}", user.getId(), e);
                    span.recordException(e);
                    span.setAttribute("token.error", e.getClass().getSimpleName());
                    meterRegistry.counter("token.generation.error", 
                            "error", e.getClass().getSimpleName()).increment();
                }
            } else {
                span.setAttribute("user.present", false);
            }

            // Add standard context variables
            velocityContext.put("webUrl", velocityEngine.getProperty("web.url"));
            velocityContext.put("dateTool", new DateTool());
            velocityContext.put("numberTool", new NumberTool());
            velocityContext.put("locale", Locale.getDefault());
            velocityContext.put("timestamp", System.currentTimeMillis());
            
            // Add correlation ID for tracing
            String correlationId = span.getSpanContext().getTraceId();
            velocityContext.put("correlationId", correlationId);
            
            // Add a helper method for safe HTML escaping
            velocityContext.put("escapeHtml", new org.apache.velocity.tools.generic.EscapeTool());
            
            // Add environment information
            velocityContext.put("environment", System.getProperty("environment", "production"));
            
            meterRegistry.counter("template.context.created").increment();
            return velocityContext;
        } finally {
            contextTimer.stop(meterRegistry.timer("template.context.preparation.time", 
                    "user_present", String.valueOf(user != null)));
            span.end();
        }
    }
    
    /**
     * Gets a template from the specified source.
     *
     * @param name The template name
     * @param source The template source
     * @return The template
     */
    public Template getTemplate(String name, TemplateSource source) {
        String cacheKey = source.name() + "-" + name;
        
        // Check cache first
        Template cachedTemplate = templateCache.get(cacheKey);
        if (cachedTemplate != null) {
            meterRegistry.counter("template.cache.hit", 
                    "template", name, 
                    "source", source.name()).increment();
            return cachedTemplate;
        }
        
        meterRegistry.counter("template.cache.miss", 
                "template", name, 
                "source", source.name()).increment();
        
        Span span = tracer.spanBuilder("loadTemplate")
                .setAttribute("template.name", name)
                .setAttribute("template.source", source.name())
                .setAttribute("cache.status", "miss")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        Timer.Sample loadTimer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            Template template;
            
            switch (source) {
                case FILESYSTEM:
                    String templateFilePath = Paths.get(templatePaths.get(TemplateSource.FILESYSTEM), name + ".vm").toString();
                    span.setAttribute("template.path", templateFilePath);
                    template = velocityEngine.getTemplate(templateFilePath, StandardCharsets.UTF_8.name());
                    break;
                    
                case CLASSPATH:
                    String classpathTemplate = templatePaths.get(TemplateSource.CLASSPATH) + "/" + name + ".vm";
                    span.setAttribute("template.path", classpathTemplate);
                    template = velocityEngine.getTemplate(classpathTemplate, StandardCharsets.UTF_8.name());
                    break;
                    
                case DATABASE:
                    // Implementation for database templates would go here
                    // For now, we'll throw an exception
                    span.setAttribute("error.type", "UnsupportedOperation");
                    throw new UnsupportedOperationException("Database templates not yet implemented");
                    
                default:
                    span.setAttribute("error.type", "IllegalArgument");
                    throw new IllegalArgumentException("Unknown template source: " + source);
            }
            
            // Cache the template
            templateCache.put(cacheKey, template);
            
            // Record cache size after update
            meterRegistry.gauge("template.cache.size.after.update", templateCache, Map::size);
            
            return template;
        } catch (Exception e) {
            span.recordException(e);
            span.setAttribute("error", true);
            span.setAttribute("error.message", e.getMessage());
            
            meterRegistry.counter("template.load.error", 
                    "template", name, 
                    "source", source.name(), 
                    "error", e.getClass().getSimpleName()).increment();
                    
            LOGGER.error("Failed to load template: {} from source: {}", name, source, e);
            throw new TemplateException("Failed to load template: " + name, e);
        } finally {
            loadTimer.stop(meterRegistry.timer("template.load.time", 
                    "template", name, 
                    "source", source.name()));
            span.end();
        }
    }
    
    /**
     * Formats a message using the specified template and context.
     *
     * @param velocityContext The context containing variables for the template
     * @param name The template name
     * @param source The template source
     * @return The formatted notification message
     */
    public NotificationMessage formatMessage(VelocityContext velocityContext, String name, TemplateSource source) {
        Span parentSpan = Span.current();
        String correlationId = parentSpan.getSpanContext().getTraceId();
        
        Span span = tracer.spanBuilder("formatMessage")
                .setAttribute("template.name", name)
                .setAttribute("template.source", source.name())
                .setAttribute("correlation.id", correlationId)
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        Timer.Sample formatTimer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            // Use circuit breaker to protect against template rendering failures
            return circuitBreaker.executeSupplier(() -> {
                StringWriter writer = new StringWriter();
                Template template = getTemplate(name, source);
                template.merge(velocityContext, writer);
                String content = writer.toString();
                
                // Add additional metrics for template size
                meterRegistry.counter("template.render.count", 
                        "template", name, 
                        "source", source.name()).increment();
                meterRegistry.summary("template.content.size", 
                        "template", name, 
                        "source", source.name()).record(content.length());
                
                return new NotificationMessage(
                        (String) velocityContext.get("subject"), 
                        content, 
                        correlationId);
            });
        } catch (Exception e) {
            span.recordException(e);
            meterRegistry.counter("template.render.error", 
                    "template", name, 
                    "source", source.name(), 
                    "error", e.getClass().getSimpleName()).increment();
            LOGGER.error("Failed to format message with template: {}", name, e);
            throw new TemplateException("Failed to format message with template: " + name, e);
        } finally {
            formatTimer.stop(meterRegistry.timer("template.format.time", 
                    "template", name, 
                    "source", source.name()));
            span.end();
        }
    }
    
    /**
     * Asynchronously formats a message using the specified template and context.
     *
     * @param velocityContext The context containing variables for the template
     * @param name The template name
     * @param source The template source
     * @return A CompletableFuture that will complete with the formatted notification message
     */
    public CompletableFuture<NotificationMessage> formatMessageAsync(
            VelocityContext velocityContext, String name, TemplateSource source) {
        
        Span parentSpan = Span.current();
        Context context = Context.current();
        String correlationId = parentSpan.getSpanContext().getTraceId();
        
        // Record that an async operation was started
        meterRegistry.counter("template.async.started", 
                "template", name, 
                "source", source.name()).increment();
        
        Timer.Sample asyncTimer = Timer.start(meterRegistry);
        
        return CompletableFuture.supplyAsync(() -> {
            // Propagate the trace context to the async task
            try (Scope scope = context.makeCurrent()) {
                NotificationMessage result = formatMessage(velocityContext, name, source);
                // Record successful completion
                meterRegistry.counter("template.async.completed", 
                        "template", name, 
                        "source", source.name()).increment();
                return result;
            }
        }, templateProcessingExecutor)
        .whenComplete((result, error) -> {
            // Stop the timer when the future completes (success or error)
            asyncTimer.stop(meterRegistry.timer("template.async.time", 
                    "template", name, 
                    "source", source.name(), 
                    "status", error == null ? "success" : "error"));
            
            if (error != null) {
                // Record the error
                meterRegistry.counter("template.async.error", 
                        "template", name, 
                        "source", source.name(), 
                        "error", error.getClass().getSimpleName()).increment();
                LOGGER.error("Async template processing failed for template: {} with correlation ID: {}", 
                        name, correlationId, error);
            }
        });
    }
    
    /**
     * Checks if a template exists in the specified source.
     *
     * @param name The template name
     * @param source The template source
     * @return true if the template exists, false otherwise
     */
    public boolean templateExists(String name, TemplateSource source) {
        try {
            switch (source) {
                case FILESYSTEM:
                    Path path = Paths.get(templatePaths.get(TemplateSource.FILESYSTEM), name + ".vm");
                    return Files.exists(path);
                    
                case CLASSPATH:
                    String resourcePath = templatePaths.get(TemplateSource.CLASSPATH) + "/" + name + ".vm";
                    return getClass().getClassLoader().getResource(resourcePath) != null;
                    
                case DATABASE:
                    // Implementation for database templates would go here
                    return false;
                    
                default:
                    return false;
            }
        } catch (Exception e) {
            LOGGER.warn("Error checking if template exists: {} in source: {}", name, source, e);
            return false;
        }
    }
    
    /**
     * Exception thrown when template processing fails.
     */
    public static class TemplateException extends RuntimeException {
        public TemplateException(String message) {
            super(message);
        }
        
        public TemplateException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}