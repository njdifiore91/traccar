/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongCounter;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.traccar.config.Config;
import org.traccar.helper.model.GeofenceUtil;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.List;

/**
 * GeofenceHandler enriches incoming Position events with geofence identifiers
 * computed from the server configuration and geofence cache.
 * 
 * This handler is part of the position-service microservice and is designed to be
 * thread-safe for concurrent processing in a distributed environment.
 */
@Singleton
public class GeofenceHandler extends BasePositionHandler {

    private final Config config;
    private final CacheManager cacheManager;
    private final Tracer tracer;
    private final LongCounter geofenceDetectionCounter;

    /**
     * Constructs a new GeofenceHandler with the required dependencies.
     *
     * @param config The system configuration
     * @param cacheManager The cache manager for geofence data access
     * @param tracer The OpenTelemetry tracer for performance monitoring
     * @param meter The OpenTelemetry meter for metrics collection
     */
    @Inject
    public GeofenceHandler(Config config, CacheManager cacheManager, Tracer tracer, Meter meter) {
        this.config = config;
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.geofenceDetectionCounter = meter.counterBuilder("geofence.detection.count")
                .setDescription("Number of geofence detections processed")
                .build();
    }

    /**
     * Processes the position to determine which geofences it intersects with.
     * Enriches the position with geofence IDs if any are detected.
     *
     * @param position The position to process
     * @param callback The callback to invoke after processing
     */
    @Override
    public void onPosition(Position position, Callback callback) {
        Span span = tracer.spanBuilder("GeofenceHandler.onPosition").startSpan();
        try {
            span.setAttribute("deviceId", position.getDeviceId());
            
            List<Long> geofenceIds = GeofenceUtil.getCurrentGeofences(config, cacheManager, position);
            if (!geofenceIds.isEmpty()) {
                position.setGeofenceIds(geofenceIds);
                geofenceDetectionCounter.add(1, 
                    Span.current().getSpanContext().getTraceId(),
                    "deviceId", String.valueOf(position.getDeviceId()),
                    "geofenceCount", String.valueOf(geofenceIds.size()));
                span.setAttribute("geofenceCount", geofenceIds.size());
            }
            callback.processed(false);
        } finally {
            span.end();
        }
    }
}