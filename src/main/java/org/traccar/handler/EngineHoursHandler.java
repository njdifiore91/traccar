/*
 * Copyright 2018 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2018 Andrey Kunitsyn (andrey@traccar.org)
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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.concurrent.CompletableFuture;

/**
 * Handler for calculating engine hours based on ignition status.
 * Supports both direct processing and asynchronous processing via message broker.
 */
public class EngineHoursHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineHoursHandler.class);

    private final CacheManager cacheManager;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final MessagePublisher messagePublisher;
    private final boolean asyncProcessingEnabled;

    /**
     * Constructs the EngineHoursHandler with required dependencies.
     *
     * @param cacheManager Cache manager for retrieving previous positions
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param messagePublisher Message broker publisher for asynchronous processing
     * @param asyncProcessingEnabled Flag to enable/disable asynchronous processing
     */
    @Inject
    public EngineHoursHandler(
            CacheManager cacheManager,
            Tracer tracer,
            MeterRegistry meterRegistry,
            MessagePublisher messagePublisher,
            boolean asyncProcessingEnabled) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.messagePublisher = messagePublisher;
        this.asyncProcessingEnabled = asyncProcessingEnabled;
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("EngineHoursHandler.onPosition").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", position.getDeviceId());
            
            if (asyncProcessingEnabled) {
                // Asynchronous processing via message broker
                span.addEvent("Starting asynchronous processing");
                processAsync(position)
                    .thenAccept(processed -> {
                        span.addEvent("Asynchronous processing completed");
                        callback.processed(false);
                    })
                    .exceptionally(ex -> {
                        LOGGER.error("Error in asynchronous engine hours processing", ex);
                        span.recordException(ex);
                        callback.processed(false);
                        return null;
                    });
            } else {
                // Direct synchronous processing
                span.addEvent("Starting synchronous processing");
                Timer.Sample sample = Timer.start(meterRegistry);
                processEngineHours(position);
                sample.stop(meterRegistry.timer("enginehours.calculation", 
                        "deviceId", String.valueOf(position.getDeviceId())));
                span.addEvent("Synchronous processing completed");
                callback.processed(false);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Process engine hours calculation asynchronously via message broker.
     *
     * @param position Position to process
     * @return CompletableFuture that completes when processing is done
     */
    private CompletableFuture<Void> processAsync(Position position) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            // Publish position to message broker for asynchronous processing
            // The actual processing will be done by a consumer service
            messagePublisher.publishEngineHoursCalculationRequest(position, result -> {
                // This is the callback that will be invoked when processing is complete
                if (result.isSuccess()) {
                    // If the message broker processed the position and returned updated engine hours
                    if (result.getUpdatedPosition() != null && result.getUpdatedPosition().hasAttribute(Position.KEY_HOURS)) {
                        position.set(Position.KEY_HOURS, result.getUpdatedPosition().getLong(Position.KEY_HOURS));
                    }
                    future.complete(null);
                } else {
                    // If there was an error in asynchronous processing, fall back to direct processing
                    LOGGER.warn("Falling back to direct engine hours processing due to async processing failure");
                    Timer.Sample sample = Timer.start(meterRegistry);
                    processEngineHours(position);
                    sample.stop(meterRegistry.timer("enginehours.calculation.fallback", 
                            "deviceId", String.valueOf(position.getDeviceId())));
                    future.complete(null);
                }
            });
        } catch (Exception e) {
            LOGGER.error("Error publishing engine hours calculation request", e);
            // Fall back to direct processing on exception
            Timer.Sample sample = Timer.start(meterRegistry);
            processEngineHours(position);
            sample.stop(meterRegistry.timer("enginehours.calculation.fallback", 
                    "deviceId", String.valueOf(position.getDeviceId())));
            future.complete(null);
        }
        return future;
    }

    /**
     * Direct synchronous processing of engine hours calculation.
     *
     * @param position Position to process
     */
    private void processEngineHours(Position position) {
        if (!position.hasAttribute(Position.KEY_HOURS)) {
            Position last = cacheManager.getPosition(position.getDeviceId());
            if (last != null) {
                long hours = last.getLong(Position.KEY_HOURS);
                if (last.getBoolean(Position.KEY_IGNITION) && position.getBoolean(Position.KEY_IGNITION)) {
                    hours += position.getDeviceTime().getTime() - last.getDeviceTime().getTime();
                }
                if (hours != 0) {
                    position.set(Position.KEY_HOURS, hours);
                }
            }
        }
    }
}