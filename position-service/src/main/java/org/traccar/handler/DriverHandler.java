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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Driver;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

/**
 * Handler that assigns driver IDs to positions based on linked drivers.
 * This handler is part of the position processing pipeline in the position-service.
 */
@Singleton
public class DriverHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(DriverHandler.class);
    
    private final CacheManager cacheManager;
    private final boolean useLinkedDriver;
    private final Tracer tracer;

    /**
     * Initialize the driver handler with required dependencies.
     *
     * @param config Configuration provider for settings
     * @param cacheManager Cache manager for retrieving driver information
     * @param tracer OpenTelemetry tracer for performance monitoring
     */
    @Inject
    public DriverHandler(Config config, CacheManager cacheManager, Tracer tracer) {
        this.cacheManager = cacheManager;
        this.useLinkedDriver = config.getBoolean(Keys.PROCESSING_USE_LINKED_DRIVER);
        this.tracer = tracer;
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        Span span = tracer.spanBuilder("DriverHandler.onPosition").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", position.getDeviceId());
            span.setAttribute("useLinkedDriver", useLinkedDriver);
            span.setAttribute("hasDriverId", position.hasAttribute(Position.KEY_DRIVER_UNIQUE_ID));
            
            if (useLinkedDriver && !position.hasAttribute(Position.KEY_DRIVER_UNIQUE_ID)) {
                var drivers = cacheManager.getDeviceObjects(position.getDeviceId(), Driver.class);
                span.setAttribute("driversFound", !drivers.isEmpty());
                
                if (!drivers.isEmpty()) {
                    Driver driver = drivers.iterator().next();
                    position.set(Position.KEY_DRIVER_UNIQUE_ID, driver.getUniqueId());
                    LOGGER.debug("Assigned driver {} to position for device {}", 
                            driver.getUniqueId(), position.getDeviceId());
                }
            }
            callback.processed(false);
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.warn("Error assigning driver to position", e);
            callback.processed(false);
        } finally {
            span.end();
        }
    }
}