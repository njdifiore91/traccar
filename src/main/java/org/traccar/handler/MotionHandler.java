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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.concurrent.CompletableFuture;

/**
 * Motion handler processes position updates to determine if a device is in motion.
 * It supports both synchronous (direct) and asynchronous (message broker) processing modes.
 */
@Singleton
public class MotionHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(MotionHandler.class);

    private final CacheManager cacheManager;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final MessagePublisher messagePublisher;
    private final boolean asyncProcessingEnabled;

    private final Counter motionDetectedCounter;
    private final Counter motionNotDetectedCounter;
    private final Timer motionProcessingTimer;

    /**
     * Constructs a MotionHandler with required dependencies.
     *
     * @param cacheManager Cache manager for device attribute lookup
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param messagePublisher Message broker publisher for async processing
     */
    @Inject
    public MotionHandler(
            CacheManager cacheManager,
            Tracer tracer,
            MeterRegistry meterRegistry,
            MessagePublisher messagePublisher) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.messagePublisher = messagePublisher;
        
        // Check if async processing is enabled via configuration
        this.asyncProcessingEnabled = messagePublisher != null && 
                messagePublisher.isEnabled("position.motion");

        // Initialize metrics
        this.motionDetectedCounter = Counter.builder("traccar.motion.detected")
                .description("Number of motion events detected")
                .register(meterRegistry);
        
        this.motionNotDetectedCounter = Counter.builder("traccar.motion.not_detected")
                .description("Number of stationary events detected")
                .register(meterRegistry);
        
        this.motionProcessingTimer = Timer.builder("traccar.motion.processing_time")
                .description("Time taken to process motion detection")
                .register(meterRegistry);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for motion detection processing
        Span span = tracer.spanBuilder("motion.detection")
                .setParent(Context.current())
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("positionId", String.valueOf(position.getId()))
                .startSpan();
        
        try {
            // Use timer to measure motion detection processing time
            motionProcessingTimer.record(() -> {
                processMotion(position);
            });
            
            // If async processing is enabled, publish to message broker and complete callback
            if (asyncProcessingEnabled) {
                span.setAttribute("processing.mode", "async");
                CompletableFuture.runAsync(() -> {
                    try {
                        messagePublisher.publish("position.motion", position);
                        LOGGER.debug("Published motion state for device {} to message broker", 
                                position.getDeviceId());
                    } catch (Exception e) {
                        LOGGER.error("Failed to publish motion state to message broker", e);
                        span.setStatus(StatusCode.ERROR, "Failed to publish to message broker: " + e.getMessage());
                    }
                });
                callback.processed(false);
            } else {
                // Direct processing mode
                span.setAttribute("processing.mode", "direct");
                callback.processed(false);
            }
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.error("Error processing motion for device {}", position.getDeviceId(), e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            callback.processed(false);
        } finally {
            span.end();
        }
    }
    
    /**
     * Process motion detection for a position.
     * 
     * @param position The position to process
     */
    private void processMotion(Position position) {
        if (!position.hasAttribute(Position.KEY_MOTION)) {
            double threshold = AttributeUtil.lookup(
                    cacheManager, Keys.EVENT_MOTION_SPEED_THRESHOLD, position.getDeviceId());
            boolean isInMotion = position.getSpeed() > threshold;
            position.set(Position.KEY_MOTION, isInMotion);
            
            // Update metrics based on motion state
            if (isInMotion) {
                motionDetectedCounter.increment();
            } else {
                motionNotDetectedCounter.increment();
            }
            
            LOGGER.debug("Motion state for device {}: {}, speed: {}, threshold: {}", 
                    position.getDeviceId(), isInMotion, position.getSpeed(), threshold);
        }
    }
}