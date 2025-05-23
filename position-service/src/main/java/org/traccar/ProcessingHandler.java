/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.model.Position;
import org.traccar.handler.BasePositionHandler;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.TransactionManager;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Position processing handler for the Position Service.
 * 
 * This handler is responsible for processing positions received from the Protocol Service
 * via a message broker. It implements the transaction outbox pattern for reliable message
 * publishing, adds distributed tracing with correlation IDs, configures circuit breakers
 * for external service calls, and integrates with health check and metrics collection.
 */
@Singleton
public class ProcessingHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingHandler.class);

    private final Storage storage;
    private final TransactionManager transactionManager;
    private final TransactionOutboxManager outboxManager;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final MeterRegistry meterRegistry;
    private final Timer processingTimer;
    private final BasePositionHandler[] handlers;

    /**
     * Constructs a new ProcessingHandler with the necessary dependencies.
     *
     * @param config Configuration
     * @param storage Storage for persisting positions
     * @param transactionManager Transaction manager for database operations
     * @param outboxManager Transaction outbox manager for reliable message publishing
     * @param circuitBreakerRegistry Registry for circuit breakers
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param propagator OpenTelemetry context propagator
     * @param meterRegistry Metrics registry
     * @param handlers Array of position handlers to process the position
     */
    @Inject
    public ProcessingHandler(
            Config config,
            Storage storage,
            TransactionManager transactionManager,
            TransactionOutboxManager outboxManager,
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            TextMapPropagator propagator,
            MeterRegistry meterRegistry,
            BasePositionHandler[] handlers) {
        this.storage = storage;
        this.transactionManager = transactionManager;
        this.outboxManager = outboxManager;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.tracer = tracer;
        this.propagator = propagator;
        this.meterRegistry = meterRegistry;
        this.handlers = handlers;
        this.processingTimer = meterRegistry.timer("position.processing.time");
    }

    /**
     * Process a position received from the message broker.
     * 
     * This method extracts the correlation ID from the message headers,
     * creates a new span for distributed tracing, processes the position
     * through the handler chain, and publishes the enriched position to
     * the outbox for reliable delivery to downstream services.
     *
     * @param position Position to process
     * @param headers Message headers containing correlation ID and tracing information
     * @return CompletableFuture that completes when the position is processed
     */
    public CompletableFuture<Void> processPosition(Position position, Map<String, String> headers) {
        // Extract correlation ID from headers or generate a new one
        String correlationId = headers.getOrDefault("x-correlation-id", UUID.randomUUID().toString());
        
        // Extract tracing context from headers
        Context context = propagator.extract(Context.current(), headers, new HeadersGetter());
        
        // Create a new span for position processing
        Span span = tracer.spanBuilder("process-position")
                .setParent(context)
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("position.id", position.getId())
                .setAttribute("position.deviceId", position.getDeviceId())
                .setAttribute("x-correlation-id", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Record metrics for position processing
            return processingTimer.record(() -> {
                try {
                    return processPositionInternal(position, correlationId);
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                    LOGGER.error("Error processing position with correlation ID {}: {}", correlationId, e.getMessage());
                    return CompletableFuture.failedFuture(e);
                } finally {
                    span.end();
                }
            });
        }
    }

    /**
     * Internal method to process a position through the handler chain.
     * 
     * This method uses the transaction outbox pattern to ensure that the position
     * is stored in the database and the enriched position is published to the
     * message broker in a single transaction.
     *
     * @param position Position to process
     * @param correlationId Correlation ID for distributed tracing
     * @return CompletableFuture that completes when the position is processed
     */
    private CompletableFuture<Void> processPositionInternal(Position position, String correlationId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return transactionManager.execute(transactionStorage -> {
                    // Store the position in the database
                    if (position.getId() == 0) {
                        transactionStorage.addObject(position, new Request(new Columns.Exclude("id")));
                    } else {
                        transactionStorage.updateObject(position, new Request(
                                new Condition.Equals("id", position.getId())));
                    }
                    
                    // Process the position through the handler chain
                    Position processedPosition = processPositionWithHandlers(position, correlationId);
                    
                    // Add the enriched position to the outbox for reliable delivery
                    outboxManager.addToOutbox(transactionStorage, "enriched-positions", processedPosition, correlationId);
                    
                    return processedPosition;
                });
            } catch (StorageException e) {
                LOGGER.error("Storage error processing position with correlation ID {}: {}", correlationId, e.getMessage());
                throw new RuntimeException(e);
            }
        }).thenApply(processedPosition -> {
            // Increment the processed positions counter
            meterRegistry.counter("position.processed").increment();
            return null;
        });
    }

    /**
     * Process a position through the handler chain with circuit breaker protection.
     * 
     * This method applies each handler to the position in sequence, with circuit breaker
     * protection for handlers that make external service calls.
     *
     * @param position Position to process
     * @param correlationId Correlation ID for distributed tracing
     * @return Processed position after passing through all handlers
     */
    private Position processPositionWithHandlers(Position position, String correlationId) {
        Position processedPosition = position;
        
        for (BasePositionHandler handler : handlers) {
            String handlerName = handler.getClass().getSimpleName();
            Span handlerSpan = tracer.spanBuilder("handler-" + handlerName)
                    .setParent(Context.current())
                    .setAttribute("handler.name", handlerName)
                    .setAttribute("x-correlation-id", correlationId)
                    .startSpan();
            
            try (Scope scope = handlerSpan.makeCurrent()) {
                // Check if this handler requires circuit breaker protection
                if (requiresCircuitBreaker(handler)) {
                    CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(handlerName);
                    
                    // Execute the handler with circuit breaker protection
                    processedPosition = executeWithCircuitBreaker(circuitBreaker, () -> {
                        return applyHandler(handler, processedPosition);
                    }, processedPosition);
                } else {
                    // Execute the handler directly
                    processedPosition = applyHandler(handler, processedPosition);
                }
                
                // Record handler success
                meterRegistry.counter("handler.success", "handler", handlerName).increment();
            } catch (Exception e) {
                // Record handler failure
                handlerSpan.recordException(e);
                handlerSpan.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                meterRegistry.counter("handler.failure", "handler", handlerName).increment();
                LOGGER.error("Error in handler {} with correlation ID {}: {}", handlerName, correlationId, e.getMessage());
            } finally {
                handlerSpan.end();
            }
        }
        
        return processedPosition;
    }

    /**
     * Apply a handler to a position.
     *
     * @param handler Handler to apply
     * @param position Position to process
     * @return Processed position
     */
    private Position applyHandler(BasePositionHandler handler, Position position) {
        CompletableFuture<Position> future = new CompletableFuture<>();
        
        handler.handlePosition(position, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                if (success) {
                    future.complete(position);
                } else {
                    future.complete(null);
                }
            }
        });
        
        Position result = future.join();
        return result != null ? result : position;
    }

    /**
     * Execute a function with circuit breaker protection.
     *
     * @param circuitBreaker Circuit breaker to use
     * @param supplier Function to execute
     * @param fallback Fallback value if the circuit breaker is open
     * @return Result of the function or fallback value
     */
    private <T> T executeWithCircuitBreaker(CircuitBreaker circuitBreaker, Supplier<T> supplier, T fallback) {
        try {
            return circuitBreaker.executeSupplier(supplier);
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker {} is open, using fallback", circuitBreaker.getName());
            return fallback;
        }
    }

    /**
     * Check if a handler requires circuit breaker protection.
     * 
     * Handlers that make external service calls should be protected by a circuit breaker.
     *
     * @param handler Handler to check
     * @return True if the handler requires circuit breaker protection
     */
    private boolean requiresCircuitBreaker(BasePositionHandler handler) {
        // Handlers that make external service calls should be protected by a circuit breaker
        String handlerName = handler.getClass().getSimpleName();
        return handlerName.equals("GeocoderHandler") || 
               handlerName.equals("GeolocationHandler") || 
               handlerName.equals("SpeedLimitHandler") || 
               handlerName.equals("PositionForwardingHandler");
    }

    /**
     * TextMapGetter implementation for extracting tracing context from message headers.
     */
    private static class HeadersGetter implements TextMapGetter<Map<String, String>> {
        @Override
        public Iterable<String> keys(Map<String, String> headers) {
            return headers.keySet();
        }

        @Override
        public String get(Map<String, String> headers, String key) {
            return headers.get(key);
        }
    }
}