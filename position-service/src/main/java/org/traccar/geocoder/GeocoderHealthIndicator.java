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
package org.traccar.geocoder;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Health indicator for geocoding services that exposes health metrics for monitoring
 * and integrates with service discovery for health checks.
 */
@Singleton
public class GeocoderHealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderHealthIndicator.class);

    private final Map<String, Geocoder> geocoders = new HashMap<>();
    private final MeterRegistry meterRegistry;

    /**
     * Initialize the health indicator with metrics registry.
     *
     * @param meterRegistry Metrics registry for exposing health metrics
     */
    @Inject
    public GeocoderHealthIndicator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        LOGGER.info("Initialized geocoder health indicator");
    }

    /**
     * Register a geocoder for health monitoring.
     *
     * @param name Name of the geocoder provider
     * @param geocoder Geocoder instance to monitor
     */
    public void registerGeocoder(String name, Geocoder geocoder) {
        geocoders.put(name, geocoder);
        
        // Register a gauge that reports the health status (1 for healthy, 0 for unhealthy)
        Gauge.builder("geocoder.health", () -> isGeocoderHealthy(name) ? 1 : 0)
                .tag("provider", name)
                .description("Health status of geocoder provider")
                .register(meterRegistry);
        
        LOGGER.debug("Registered geocoder for health monitoring: {}", name);
    }

    /**
     * Check if a specific geocoder is healthy.
     *
     * @param name Name of the geocoder provider
     * @return true if the geocoder is healthy, false otherwise
     */
    public boolean isGeocoderHealthy(String name) {
        Geocoder geocoder = geocoders.get(name);
        if (geocoder == null) {
            LOGGER.warn("Attempted to check health of unknown geocoder: {}", name);
            return false;
        }
        return geocoder.isHealthy();
    }

    /**
     * Check if all registered geocoders are healthy.
     *
     * @return true if all geocoders are healthy, false if any are unhealthy
     */
    public boolean areAllGeocodersHealthy() {
        return geocoders.entrySet().stream()
                .allMatch(entry -> {
                    boolean healthy = entry.getValue().isHealthy();
                    if (!healthy) {
                        LOGGER.warn("Geocoder unhealthy: {}", entry.getKey());
                    }
                    return healthy;
                });
    }

    /**
     * Check if any registered geocoder is healthy.
     * This is useful for determining if the service can operate in degraded mode.
     *
     * @return true if at least one geocoder is healthy, false if all are unhealthy
     */
    public boolean isAnyGeocoderHealthy() {
        return geocoders.entrySet().stream()
                .anyMatch(entry -> entry.getValue().isHealthy());
    }

    /**
     * Get the count of healthy geocoders.
     *
     * @return Number of healthy geocoders
     */
    public int getHealthyGeocoderCount() {
        return (int) geocoders.entrySet().stream()
                .filter(entry -> entry.getValue().isHealthy())
                .count();
    }

    /**
     * Get the total count of registered geocoders.
     *
     * @return Total number of registered geocoders
     */
    public int getTotalGeocoderCount() {
        return geocoders.size();
    }

    /**
     * Get a map of geocoder names to their health status.
     *
     * @return Map of geocoder names to boolean health status
     */
    public Map<String, Boolean> getGeocoderHealthStatus() {
        Map<String, Boolean> status = new HashMap<>();
        geocoders.forEach((name, geocoder) -> status.put(name, geocoder.isHealthy()));
        return status;
    }
}