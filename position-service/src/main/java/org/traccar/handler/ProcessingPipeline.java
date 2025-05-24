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
package org.traccar.handler;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.annotation.PostConstruct;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Position;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * ProcessingPipeline is responsible for orchestrating the position processing pipeline
 * in the Position Processing Service. It manages the registration and ordering of position
 * handlers, ensures sequential processing of positions through the handler chain, and
 * implements circuit breaker and retry policies for resilience.
 * 
 * The pipeline provides the following features:
 * - Registration of position handlers in a specific order
 * - Sequential processing of positions through the handler chain
 * - Circuit breaker pattern to prevent cascading failures
 * - Retry mechanism with exponential backoff for transient failures
 * - OpenTelemetry instrumentation for distributed tracing
 * - Metrics for pipeline performance and throughput monitoring
 * - Thread safety for concurrent processing in a microservices environment
 */
@Singleton
public class ProcessingPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingPipeline.class);
    
    private final List<BasePositionHandler> handlers = new ArrayList<>();
    private final ConcurrentMap<Long, AtomicInteger> deviceProcessingCounters = new ConcurrentHashMap<>();
    
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;
    private final Executor asyncExecutor;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    private Timer processingTimer;
    private Counter processedPositionsCounter;
    private Counter failedPositionsCounter;
    private Counter filteredPositionsCounter;
    
    /**
     * Constructs a new ProcessingPipeline with the necessary dependencies.
     *
     * @param circuitBreakerRegistry Registry for circuit breakers
     * @param retryRegistry Registry for retry configurations
     * @param asyncExecutor Executor for asynchronous processing
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     */
    @Inject
    public ProcessingPipeline(
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            Executor asyncExecutor,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.retryRegistry = retryRegistry;
        this.asyncExecutor = asyncExecutor;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
    }
    
    /**
     * Initializes metrics and default circuit breaker/retry configurations.
     */
    @PostConstruct
    public void init() {
        // Initialize metrics
        processingTimer = Timer.builder("position.processing.time")
                .description("Time taken to process a position through the pipeline")
                .register(meterRegistry);
        
        processedPositionsCounter = Counter.builder("position.processed")
                .description("Number of positions processed successfully")
                .register(meterRegistry);
        
        failedPositionsCounter = Counter.builder("position.failed")
                .description("Number of positions that failed processing")
                .register(meterRegistry);
        
        filteredPositionsCounter = Counter.builder("position.filtered")
                .description("Number of positions filtered out during processing")
                .register(meterRegistry);
        
        // Configure default circuit breaker if not already configured
        if (!circuitBreakerRegistry.getAllCircuitBreakers().iterator().hasNext()) {
            CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                    .failureRateThreshold(50)
                    .waitDurationInOpenState(Duration.ofSeconds(10))
                    .permittedNumberOfCallsInHalfOpenState(5)
                    .slidingWindowSize(10)
                    .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                    .automaticTransitionFromOpenToHalfOpenEnabled(true)
                    .build();
            
            circuitBreakerRegistry.addConfiguration("default", circuitBreakerConfig);
        }
        
        // Configure default retry if not already configured
        if (!retryRegistry.getAllRetries().iterator().hasNext()) {
            RetryConfig retryConfig = RetryConfig.custom()
                    .maxAttempts(3)
                    .waitDuration(Duration.ofMillis(500))
                    .retryExceptions(RuntimeException.class)
                    .enableExponentialBackoff(true)
                    .exponentialBackoffMultiplier(2.0)
                    .build();
            
            retryRegistry.addConfiguration("default", retryConfig);
        }
        
        LOGGER.info("Position processing pipeline initialized with {} handlers", handlers.size());
    }
    
    /**
     * Registers a position handler to the pipeline.
     *
     * @param handler The handler to register
     * @return This ProcessingPipeline instance for method chaining
     */
    public synchronized ProcessingPipeline registerHandler(BasePositionHandler handler) {
        handlers.add(handler);
        String handlerName = handler.getClass().getSimpleName();
        LOGGER.info("Registered position handler: {} at index {}", handlerName, handlers.size() - 1);
        
        // Create metrics for this handler if they don't exist yet
        if (meterRegistry != null) {
            Timer.builder("position.handler.time." + handlerName)
                    .description("Time taken by handler " + handlerName)
                    .register(meterRegistry);
            
            Counter.builder("position.handler.count." + handlerName)
                    .description("Number of positions processed by handler " + handlerName)
                    .register(meterRegistry);
        }
        
        return this;
    }
    
    /**
     * Registers multiple position handlers to the pipeline in the given order.
     *
     * @param handlers The handlers to register in order
     * @return This ProcessingPipeline instance for method chaining
     */
    public synchronized ProcessingPipeline registerHandlers(BasePositionHandler... handlers) {
        for (BasePositionHandler handler : handlers) {
            registerHandler(handler);
        }
        return this;
    }
    
    /**
     * Processes a position through the pipeline of handlers.
     *
     * @param position The position to process
     * @return CompletableFuture that completes when processing is done
     */
    public CompletableFuture<Void> processPosition(Position position) {
        Span span = tracer.spanBuilder("ProcessPosition")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("positionId", position.getId())
                .setAttribute("latitude", position.getLatitude())
                .setAttribute("longitude", position.getLongitude())
                .setAttribute("time", position.getFixTime().getTime())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Processing position for device {}: {}", position.getDeviceId(), position);
            
            // Use timer to measure processing time
            return processingTimer.record(() -> {
                // Get or create counter for this device to track concurrent processing
                AtomicInteger counter = deviceProcessingCounters.computeIfAbsent(
                        position.getDeviceId(), k -> new AtomicInteger(0));
                
                // Increment counter to indicate processing has started for this device
                counter.incrementAndGet();
                
                return processPositionThroughHandlers(position, span)
                        .whenComplete((result, throwable) -> {
                            // Decrement counter when processing is complete
                            counter.decrementAndGet();
                            
                            if (throwable != null) {
                                LOGGER.error("Failed to process position for device {}", 
                                        position.getDeviceId(), throwable);
                                span.setStatus(StatusCode.ERROR, throwable.getMessage());
                                span.recordException(throwable);
                                failedPositionsCounter.increment();
                            } else {
                                span.setStatus(StatusCode.OK);
                                processedPositionsCounter.increment();
                            }
                            
                            span.end();
                        });
            });
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            span.end();
            failedPositionsCounter.increment();
            return CompletableFuture.failedFuture(e);
        }
    }
    
    /**
     * Processes a position through all registered handlers sequentially.
     *
     * @param position The position to process
     * @param parentSpan The parent span for tracing
     * @return CompletableFuture that completes when processing is done
     */
    private CompletableFuture<Void> processPositionThroughHandlers(Position position, Span parentSpan) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        
        // Process the position through the handlers sequentially
        processNextHandler(position, 0, future, parentSpan);
        
        return future;
    }
    
    /**
     * Recursively processes a position through the next handler in the chain.
     *
     * @param position The position to process
     * @param index The index of the next handler to use
     * @param future The future to complete when processing is done
     * @param parentSpan The parent span for tracing
     */
    private void processNextHandler(Position position, int index, CompletableFuture<Void> future, Span parentSpan) {
        // If we've processed all handlers, complete the future
        if (index >= handlers.size()) {
            future.complete(null);
            return;
        }
        
        BasePositionHandler handler = handlers.get(index);
        String handlerName = handler.getClass().getSimpleName();
        
        // Create a span for this handler
        Span handlerSpan = tracer.spanBuilder(handlerName)
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("handler.index", index)
                .setAttribute("handler.name", handlerName)
                .setAttribute("deviceId", position.getDeviceId())
                .startSpan();
        
        try (Scope scope = handlerSpan.makeCurrent()) {
            // Create a circuit breaker for this handler if it doesn't exist
            CircuitBreaker circuitBreaker = circuitBreakerRegistry
                    .circuitBreaker(handlerName, "default");
            
            // Create a retry for this handler if it doesn't exist
            Retry retry = retryRegistry.retry(handlerName, "default");
            
            // Log circuit breaker state for monitoring
            if (circuitBreaker.getState() != CircuitBreaker.State.CLOSED) {
                LOGGER.warn("Circuit breaker for handler {} is in state {}", 
                        handlerName, circuitBreaker.getState());
                handlerSpan.setAttribute("circuitBreaker.state", circuitBreaker.getState().name());
            }
            
            // Wrap the handler execution with circuit breaker and retry
            Supplier<CompletableFuture<Void>> decoratedSupplier = Retry.decorateCompletionStage(
                    retry,
                    CircuitBreaker.decorateCompletionStage(
                            circuitBreaker,
                            () -> executeHandler(handler, position, handlerSpan)
                    )
            );
            
            // Execute the decorated supplier
            decoratedSupplier.get().whenComplete((result, throwable) -> {
                if (throwable != null) {
                    handlerSpan.setStatus(StatusCode.ERROR, throwable.getMessage());
                    handlerSpan.recordException(throwable);
                    handlerSpan.setAttribute("error", true);
                    handlerSpan.setAttribute("error.message", throwable.getMessage());
                    handlerSpan.end();
                    future.completeExceptionally(throwable);
                } else {
                    handlerSpan.setStatus(StatusCode.OK);
                    handlerSpan.end();
                    
                    // Continue to the next handler
                    processNextHandler(position, index + 1, future, parentSpan);
                }
            });
        } catch (Exception e) {
            handlerSpan.setStatus(StatusCode.ERROR, e.getMessage());
            handlerSpan.recordException(e);
            handlerSpan.setAttribute("error", true);
            handlerSpan.setAttribute("error.message", e.getMessage());
            handlerSpan.end();
            future.completeExceptionally(e);
        }
    }
    
    /**
     * Executes a single handler on a position.
     *
     * @param handler The handler to execute
     * @param position The position to process
     * @param span The span for tracing
     * @return CompletableFuture that completes when the handler is done
     */
    private CompletableFuture<Void> executeHandler(BasePositionHandler handler, Position position, Span span) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        String handlerName = handler.getClass().getSimpleName();
        
        // Get the timer for this handler
        Timer handlerTimer = meterRegistry.timer("position.handler.time." + handlerName);
        Counter handlerCounter = meterRegistry.counter("position.handler.count." + handlerName);
        
        // Execute the handler asynchronously with timing
        asyncExecutor.execute(() -> {
            handlerTimer.record(() -> {
                try {
                    handler.handlePosition(position, filtered -> {
                        handlerCounter.increment();
                        
                        if (filtered) {
                            span.setAttribute("filtered", true);
                            filteredPositionsCounter.increment();
                            LOGGER.debug("Position filtered by handler: {}", handlerName);
                        }
                        future.complete(null);
                    });
                } catch (Exception e) {
                    LOGGER.error("Error in handler {}", handlerName, e);
                    future.completeExceptionally(e);
                }
            });
        });
        
        return future;
    }
    
    /**
     * Gets the number of registered handlers in the pipeline.
     *
     * @return The number of handlers
     */
    public synchronized int getHandlerCount() {
        return handlers.size();
    }
    
    /**
     * Gets the current processing count for a specific device.
     *
     * @param deviceId The device ID
     * @return The number of positions currently being processed for the device
     */
    public int getDeviceProcessingCount(long deviceId) {
        AtomicInteger counter = deviceProcessingCounters.get(deviceId);
        return counter != null ? counter.get() : 0;
    }
    
    /**
     * Gets the total number of positions processed by the pipeline.
     *
     * @return The total number of processed positions
     */
    public long getProcessedPositionsCount() {
        return (long) processedPositionsCounter.count();
    }
    
    /**
     * Gets the total number of positions that failed processing.
     *
     * @return The total number of failed positions
     */
    public long getFailedPositionsCount() {
        return (long) failedPositionsCounter.count();
    }
    
    /**
     * Gets the total number of positions filtered out during processing.
     *
     * @return The total number of filtered positions
     */
    public long getFilteredPositionsCount() {
        return (long) filteredPositionsCounter.count();
    }
}