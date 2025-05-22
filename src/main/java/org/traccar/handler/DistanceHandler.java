/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2015 Amila Silva
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
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.DistanceCalculator;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.concurrent.CompletableFuture;

public class DistanceHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistanceHandler.class);

    private final CacheManager cacheManager;
    private final MessagePublisher messagePublisher;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    private final boolean filter;
    private final int minError;
    private final int maxError;
    private final boolean asyncProcessingEnabled;

    @Inject
    public DistanceHandler(Config config, CacheManager cacheManager, 
                          MessagePublisher messagePublisher, 
                          Tracer tracer, 
                          MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.messagePublisher = messagePublisher;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        this.filter = config.getBoolean(Keys.COORDINATES_FILTER);
        this.minError = config.getInteger(Keys.COORDINATES_MIN_ERROR);
        this.maxError = config.getInteger(Keys.COORDINATES_MAX_ERROR);
        this.asyncProcessingEnabled = config.getBoolean(Keys.PROCESSING_ASYNC_ENABLED, false);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for distance calculation
        Span span = tracer.spanBuilder("distance.calculate")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            if (asyncProcessingEnabled) {
                processPositionAsync(position, callback, span);
            } else {
                processPositionSync(position, callback, span);
            }
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            LOGGER.warn("Distance calculation failed", e);
            callback.processed(false);
        } finally {
            span.end();
        }
    }

    private void processPositionSync(Position position, Callback callback, Span span) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            calculateDistance(position, span);
            sample.stop(meterRegistry.timer("distance.calculation.time"));
            meterRegistry.counter("distance.calculation.count").increment();
            callback.processed(false);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            meterRegistry.counter("distance.calculation.error").increment();
            throw e;
        }
    }

    private void processPositionAsync(Position position, Callback callback, Span span) {
        // Create a new context with the current span
        Context context = Context.current();
        
        CompletableFuture.runAsync(() -> {
            Timer.Sample sample = Timer.start(meterRegistry);
            
            try (Scope scope = context.makeCurrent()) {
                calculateDistance(position, span);
                sample.stop(meterRegistry.timer("distance.calculation.time"));
                meterRegistry.counter("distance.calculation.count").increment();
                
                // Publish the position to the message broker for further processing
                messagePublisher.publish("position.processed", position);
            } catch (Exception e) {
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                meterRegistry.counter("distance.calculation.error").increment();
                LOGGER.warn("Async distance calculation failed", e);
            }
        });
        
        // Mark as processed immediately since we're handling it asynchronously
        callback.processed(false);
    }

    private void calculateDistance(Position position, Span span) {
        double distance = 0.0;
        if (position.hasAttribute(Position.KEY_DISTANCE)) {
            distance = position.getDouble(Position.KEY_DISTANCE);
            span.setAttribute("distance.provided", true);
        } else {
            span.setAttribute("distance.provided", false);
        }
        
        double totalDistance;
        Position last = cacheManager.getPosition(position.getDeviceId());
        
        if (last != null) {
            span.setAttribute("previous_position.available", true);
            totalDistance = last.getDouble(Position.KEY_TOTAL_DISTANCE);
            
            if (!position.hasAttribute(Position.KEY_DISTANCE)) {
                distance = DistanceCalculator.distance(
                        position.getLatitude(), position.getLongitude(),
                        last.getLatitude(), last.getLongitude());
                span.setAttribute("distance.calculated", true);
            }
            
            if (filter && last.getLatitude() != 0 && last.getLongitude() != 0) {
                boolean satisfiesMin = minError == 0 || distance > minError;
                boolean satisfiesMax = maxError == 0 || distance < maxError || position.getValid();
                
                span.setAttribute("filter.applied", true);
                span.setAttribute("filter.satisfiesMin", satisfiesMin);
                span.setAttribute("filter.satisfiesMax", satisfiesMax);
                
                if (!satisfiesMin || !satisfiesMax) {
                    position.setValid(last.getValid());
                    position.setLatitude(last.getLatitude());
                    position.setLongitude(last.getLongitude());
                    distance = 0;
                    span.setAttribute("position.filtered", true);
                } else {
                    span.setAttribute("position.filtered", false);
                }
            } else {
                span.setAttribute("filter.applied", false);
            }
        } else {
            span.setAttribute("previous_position.available", false);
            totalDistance = 0.0;
        }
        
        position.set(Position.KEY_DISTANCE, distance);
        position.set(Position.KEY_TOTAL_DISTANCE, totalDistance + distance);
        
        span.setAttribute("distance.value", distance);
        span.setAttribute("distance.total", totalDistance + distance);
        
        // Record metrics
        meterRegistry.gauge("distance.last", distance);
        meterRegistry.gauge("distance.total", totalDistance + distance);
    }
}