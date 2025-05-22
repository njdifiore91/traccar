/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;

/**
 * Handler that adjusts position coordinates based on configured hemisphere settings.
 * Supports both direct callback processing and asynchronous processing via message broker.
 */
public class HemisphereHandler extends BasePositionHandler {

    private int latitudeFactor;
    private int longitudeFactor;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final LongCounter hemisphereAdjustmentCounter;

    /**
     * Constructs a new HemisphereHandler with the specified configuration and dependencies.
     *
     * @param config Configuration for hemisphere settings
     * @param messageProducer Producer for asynchronous message processing (optional)
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meter OpenTelemetry meter for metrics collection
     */
    @Inject
    public HemisphereHandler(Config config, 
                            MessageProducer messageProducer,
                            Tracer tracer,
                            Meter meter) {
        String latitudeHemisphere = config.getString(Keys.LOCATION_LATITUDE_HEMISPHERE);
        if (latitudeHemisphere != null) {
            if (latitudeHemisphere.equalsIgnoreCase("N")) {
                latitudeFactor = 1;
            } else if (latitudeHemisphere.equalsIgnoreCase("S")) {
                latitudeFactor = -1;
            }
        }
        String longitudeHemisphere = config.getString(Keys.LOCATION_LONGITUDE_HEMISPHERE);
        if (longitudeHemisphere != null) {
            if (longitudeHemisphere.equalsIgnoreCase("E")) {
                longitudeFactor = 1;
            } else if (longitudeHemisphere.equalsIgnoreCase("W")) {
                longitudeFactor = -1;
            }
        }
        
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        
        // Initialize metrics
        this.hemisphereAdjustmentCounter = meter.counterBuilder("hemisphere.adjustments")
                .setDescription("Number of position adjustments made by the hemisphere handler")
                .build();
    }

    /**
     * Processes a position by adjusting its coordinates based on configured hemisphere settings.
     * Supports both direct callback processing and asynchronous processing via message broker.
     *
     * @param position The position to process
     * @param callback The callback to invoke after processing
     */
    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("hemisphere.adjust")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", position.getDeviceId())
                .startSpan();
        
        try {
            // Attach the span to the current context
            Context context = Context.current().with(span);
            
            // Process the position
            boolean adjusted = false;
            
            if (latitudeFactor != 0) {
                double originalLatitude = position.getLatitude();
                position.setLatitude(Math.abs(originalLatitude) * latitudeFactor);
                span.setAttribute("original.latitude", originalLatitude);
                span.setAttribute("adjusted.latitude", position.getLatitude());
                adjusted = true;
            }
            
            if (longitudeFactor != 0) {
                double originalLongitude = position.getLongitude();
                position.setLongitude(Math.abs(originalLongitude) * longitudeFactor);
                span.setAttribute("original.longitude", originalLongitude);
                span.setAttribute("adjusted.longitude", position.getLongitude());
                adjusted = true;
            }
            
            // Record metrics if adjustment was made
            if (adjusted) {
                hemisphereAdjustmentCounter.add(1, Attributes.of(
                        AttributeKey.longKey("deviceId"), position.getDeviceId()));
            }
            
            // Handle processing based on available mechanisms
            if (messageProducer != null) {
                // Asynchronous processing via message broker
                messageProducer.publishPosition(position);
                span.addEvent("Published to message broker");
                callback.processed(true); // Mark as processed asynchronously
            } else {
                // Direct processing via callback
                span.addEvent("Processed directly");
                callback.processed(false); // Continue processing pipeline
            }
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }
}