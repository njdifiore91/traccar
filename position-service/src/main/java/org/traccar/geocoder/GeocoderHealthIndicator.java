/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;

/**
 * Implements a health indicator for geocoding services that exposes health metrics
 * for monitoring and integrates with service discovery for health checks.
 */
@Singleton
public class GeocoderHealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderHealthIndicator.class);

    private final Map<String, Geocoder> geocoders = new HashMap<>();
    private final MeterRegistry meterRegistry;

    /**
     * Creates a new GeocoderHealthIndicator with the specified meter registry.
     *
     * @param meterRegistry Registry for metrics collection
     */
    @Inject
    public GeocoderHealthIndicator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Registers a geocoder for health monitoring.
     *
     * @param name Name of the geocoder provider
     * @param geocoder Geocoder instance to monitor
     */
    public void registerGeocoder(String name, Geocoder geocoder) {
        geocoders.put(name, geocoder);
        LOGGER.info("Registered geocoder for health monitoring: {}", name);

        // Register a gauge that reports the health status of the geocoder
        if (geocoder instanceof TomTomGeocoder) {
            Gauge.builder("geocoder.health", () -> ((TomTomGeocoder) geocoder).isHealthy() ? 1 : 0)
                    .tag("provider", name)
                    .description("Health status of the geocoder provider")
                    .register(meterRegistry);
        }
    }

    /**
     * Checks the health of all registered geocoders.
     *
     * @return Map of provider names to health status
     */
    public Map<String, Boolean> checkHealth() {
        Map<String, Boolean> healthStatus = new HashMap<>();

        for (Map.Entry<String, Geocoder> entry : geocoders.entrySet()) {
            String name = entry.getKey();
            Geocoder geocoder = entry.getValue();
            boolean healthy = true;

            // Check health based on geocoder type
            if (geocoder instanceof TomTomGeocoder) {
                healthy = ((TomTomGeocoder) geocoder).isHealthy();
            }

            healthStatus.put(name, healthy);
        }

        return healthStatus;
    }

    /**
     * Determines if the geocoding service as a whole is healthy.
     * The service is considered healthy if at least one provider is healthy.
     *
     * @return true if at least one geocoder is healthy, false otherwise
     */
    public boolean isHealthy() {
        return checkHealth().values().stream().anyMatch(Boolean::booleanValue);
    }

    /**
     * Provides detailed health information for Kubernetes liveness and readiness probes.
     *
     * @return Map containing health details
     */
    public Map<String, Object> getHealthDetails() {
        Map<String, Object> details = new HashMap<>();
        details.put("status", isHealthy() ? "UP" : "DOWN");
        details.put("providers", checkHealth());
        return details;
    }
}