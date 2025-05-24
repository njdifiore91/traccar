/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.model.Position;

import javax.inject.Inject;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Base abstract class for position handlers in the position processing pipeline.
 * Provides standardized error handling and OpenTelemetry instrumentation for distributed tracing.
 */
public abstract class BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(BasePositionHandler.class);
    
    private final Tracer tracer;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Callback interface for signaling whether a position has been processed or filtered.
     */
    public interface Callback {
        /**
         * Called when position processing is complete.
         * 
         * @param filtered true if the position was filtered out, false otherwise
         */
        void processed(boolean filtered);
    }
    
    /**
     * Constructor with OpenTelemetry tracer injection.
     * 
     * @param tracer OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public BasePositionHandler(Tracer tracer) {
        this.tracer = tracer;
    }

    /**
     * Abstract method that concrete handlers must implement to process positions.
     * 
     * @param position Position to be processed
     * @param callback Callback to signal processing completion
     */
    public abstract void onPosition(Position position, Callback callback);

    /**
     * Handles a position by creating a span for tracing and invoking the onPosition method.
     * Provides error handling and ensures the callback is always called.
     * 
     * @param position Position to be processed
     * @param callback Callback to signal processing completion
     */
    public void handlePosition(Position position, Callback callback) {
        // Get current context for distributed tracing
        Context parentContext = Context.current();
        
        // Create a span for this handler operation
        String handlerName = this.getClass().getSimpleName();
        Span span = tracer.spanBuilder(handlerName + ".handlePosition")
                .setParent(parentContext)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("position.deviceId", position.getDeviceId())
                .setAttribute("position.id", position.getId())
                .startSpan();
        
        // Use the span in the current context
        try (Scope scope = span.makeCurrent()) {
            // Add trace ID to MDC for correlation in logs
            MDC.put("traceId", span.getSpanContext().getTraceId());
            MDC.put("spanId", span.getSpanContext().getSpanId());
            MDC.put("deviceId", String.valueOf(position.getDeviceId()));
            
            // Acquire read lock for thread safety during position processing
            lock.readLock().lock();
            try {
                LOGGER.debug("Processing position with handler: {}", handlerName);
                onPosition(position, filtered -> {
                    span.setAttribute("position.filtered", filtered);
                    span.setStatus(StatusCode.OK);
                    callback.processed(filtered);
                });
            } catch (RuntimeException e) {
                // Record error in span
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                
                // Log error with structured context
                LOGGER.warn("Position handler failed: {} - Error: {}", handlerName, e.getMessage(), e);
                callback.processed(false);
            } finally {
                lock.readLock().unlock();
                MDC.remove("traceId");
                MDC.remove("spanId");
                MDC.remove("deviceId");
                span.end();
            }
        }
    }
}