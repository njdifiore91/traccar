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
import jakarta.inject.Singleton;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.context.Context;

/**
 * Position processor that performs device motion detection by checking whether a Position object
 * contains the KEY_MOTION attribute and, if not, computing it by comparing the device's current speed
 * to a configurable threshold.
 */
@Singleton
public class MotionHandler extends BasePositionHandler {

    private final CacheManager cacheManager;
    private final Tracer tracer;
    private final LongCounter motionDetectionCounter;

    /**
     * Constructs a new MotionHandler with the specified dependencies.
     *
     * @param cacheManager The cache manager for retrieving device attributes
     * @param tracer The OpenTelemetry tracer for performance monitoring
     * @param meter The OpenTelemetry meter for metrics collection
     */
    @Inject
    public MotionHandler(CacheManager cacheManager, Tracer tracer, Meter meter) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.motionDetectionCounter = meter.counterBuilder("motion.detection.count")
                .setDescription("Number of motion detections performed")
                .build();
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for motion detection processing
        Span span = tracer.spanBuilder("motion.detection")
                .setParent(Context.current())
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .startSpan();
        
        try {
            if (!position.hasAttribute(Position.KEY_MOTION)) {
                // Record the motion detection operation
                motionDetectionCounter.add(1);
                
                // Get the motion speed threshold from device attributes
                double threshold = AttributeUtil.lookup(
                        cacheManager, Keys.EVENT_MOTION_SPEED_THRESHOLD, position.getDeviceId());
                
                // Set the motion attribute based on speed comparison
                boolean isInMotion = position.getSpeed() > threshold;
                position.set(Position.KEY_MOTION, isInMotion);
                
                // Add motion detection result to the span
                span.setAttribute("motion.detected", isInMotion);
                span.setAttribute("motion.speed", position.getSpeed());
                span.setAttribute("motion.threshold", threshold);
            } else {
                // Motion already determined, just record it in the span
                span.setAttribute("motion.predefined", true);
            }
            
            // Mark the span as successful
            span.setStatus(io.opentelemetry.api.trace.StatusCode.OK);
        } catch (Exception e) {
            // Record the error in the span
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            throw e; // Re-throw to be handled by BasePositionHandler
        } finally {
            // End the span regardless of success or failure
            span.end();
            callback.processed(false);
        }
    }
}