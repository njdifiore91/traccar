/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.handler.events;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class MediaEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MediaEventHandler.class);

    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Map<String, Timer> mediaTimers;

    /**
     * Constructs a new MediaEventHandler with the necessary dependencies.
     *
     * @param cacheManager The cache manager for retrieving device information
     * @param circuitBreakerRegistry Registry for circuit breaker configuration
     * @param retryRegistry Registry for retry configuration
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     */
    @Inject
    public MediaEventHandler(
            CacheManager cacheManager,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("mediaEventHandler");
        this.retry = retryRegistry.retry("mediaEventHandler");
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize timers for each media type
        this.mediaTimers = Map.of(
                Position.KEY_IMAGE, Timer.builder("media.event.detection")
                        .tag("type", "image")
                        .description("Time taken to detect and process image media events")
                        .register(meterRegistry),
                Position.KEY_VIDEO, Timer.builder("media.event.detection")
                        .tag("type", "video")
                        .description("Time taken to detect and process video media events")
                        .register(meterRegistry),
                Position.KEY_AUDIO, Timer.builder("media.event.detection")
                        .tag("type", "audio")
                        .description("Time taken to detect and process audio media events")
                        .register(meterRegistry)
        );
        
        // Register counters for media events by type
        meterRegistry.counter("media.event.count", "type", "image");
        meterRegistry.counter("media.event.count", "type", "video");
        meterRegistry.counter("media.event.count", "type", "audio");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Extract correlation ID from position attributes if available
        String correlationId = position.hasAttribute("correlationId") 
                ? position.getString("correlationId") 
                : Span.current().getSpanContext().getTraceId();
        
        // Create a span for media event detection
        Span span = tracer.spanBuilder("media.event.detection")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Process each media type with circuit breaker and retry pattern
            Stream.of(Position.KEY_IMAGE, Position.KEY_VIDEO, Position.KEY_AUDIO)
                    .filter(position::hasAttribute)
                    .forEach(type -> {
                        Timer.Sample sample = Timer.start(meterRegistry);
                        try {
                            // Create and dispatch the media event with resilience patterns
                            processMediaEvent(position, type, callback, correlationId);
                            
                            // Record successful media event detection
                            meterRegistry.counter("media.event.count", "type", getMediaTypeTag(type)).increment();
                            
                            // Record timing for successful media event detection
                            sample.stop(mediaTimers.get(type));
                            
                            // Add successful detection to span
                            span.addEvent("Media detected", Map.of(
                                    "mediaType", type,
                                    "filePath", position.getString(type)));
                            
                        } catch (Exception e) {
                            // Record failure metrics
                            meterRegistry.counter("media.event.errors", 
                                    "type", getMediaTypeTag(type),
                                    "error", e.getClass().getSimpleName()).increment();
                            
                            // Add error information to span
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, "Failed to process media event: " + e.getMessage());
                            
                            LOGGER.warn("Failed to process media event of type {}: {}", type, e.getMessage());
                        }
                    });
            
            span.setStatus(StatusCode.OK);
        } finally {
            span.end();
        }
    }
    
    /**
     * Process a single media event with circuit breaker and retry pattern.
     *
     * @param position The position containing media data
     * @param mediaType The type of media (image, video, audio)
     * @param callback The callback to notify of detected events
     * @param correlationId The correlation ID for distributed tracing
     */
    private void processMediaEvent(Position position, String mediaType, Callback callback, String correlationId) {
        // Define the media event creation as a supplier that can be decorated with resilience patterns
        Supplier<Event> eventSupplier = () -> {
            // Verify device exists in cache (protected by circuit breaker)
            circuitBreaker.acquirePermission();
            try {
                if (cacheManager.getObject(org.traccar.model.Device.class, position.getDeviceId()) == null) {
                    LOGGER.warn("Device not found in cache: {}", position.getDeviceId());
                }
                
                // Create the media event
                Event event = new Event(Event.TYPE_MEDIA, position);
                event.set("media", mediaType);
                event.set("file", position.getString(mediaType));
                
                // Propagate correlation ID
                event.set("correlationId", correlationId);
                
                return event;
            } catch (Exception e) {
                // Record circuit breaker failure
                circuitBreaker.onError(0, TimeUnit.MILLISECONDS, e);
                throw e;
            }
        };
        
        // Apply retry and circuit breaker patterns
        Event event = Retry.decorateSupplier(retry, 
                CircuitBreaker.decorateSupplier(circuitBreaker, eventSupplier)).get();
        
        // Dispatch the event
        callback.eventDetected(event);
    }
    
    /**
     * Convert media type key to a tag-friendly format for metrics.
     *
     * @param mediaType The media type key
     * @return A simplified tag value for the media type
     */
    private String getMediaTypeTag(String mediaType) {
        switch (mediaType) {
            case Position.KEY_IMAGE:
                return "image";
            case Position.KEY_VIDEO:
                return "video";
            case Position.KEY_AUDIO:
                return "audio";
            default:
                return "unknown";
        }
    }
}