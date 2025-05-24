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
package org.traccar.geocoder;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implements a health indicator for geocoding services that exposes health metrics for monitoring
 * and integrates with service discovery for health checks. This component is critical for the
 * microservices architecture as it provides visibility into the health and availability of external
 * geocoding services, enabling automated failover and alerting when services degrade.
 */
@Singleton
public class GeocoderHealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderHealthIndicator.class);

    private final List<JsonGeocoder> geocoders = new ArrayList<>();
    private final MeterRegistry meterRegistry;

    /**
     * Creates a new GeocoderHealthIndicator.
     *
     * @param meterRegistry The meter registry for metrics collection
     */
    @Inject
    public GeocoderHealthIndicator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        LOGGER.info("GeocoderHealthIndicator initialized");
    }

    /**
     * Registers a geocoder for health monitoring.
     *
     * @param geocoder The geocoder to register
     * @param name The name of the geocoder
     */
    public void registerGeocoder(JsonGeocoder geocoder, String name) {
        geocoders.add(geocoder);
        LOGGER.info("Registered geocoder for health monitoring: {}", name);

        // Register health gauge metric
        if (meterRegistry != null) {
            meterRegistry.gauge("geocoder.health", 
                    Tags.of(Tag.of("name", name)), 
                    geocoder, 
                    g -> g.isHealthy() ? 1.0 : 0.0);
            LOGGER.debug("Registered health gauge for geocoder: {}", name);
        }
    }

    /**
     * Checks the health of all registered geocoders.
     *
     * @return A map containing the health status of each geocoder
     */
    public Map<String, Object> checkHealth() {
        Map<String, Object> health = new HashMap<>();
        boolean overallHealth = true;

        for (JsonGeocoder geocoder : geocoders) {
            boolean geocoderHealth = geocoder.isHealthy();
            overallHealth = overallHealth && geocoderHealth;

            // Add circuit breaker state if available
            CircuitBreaker.State state = geocoder.getCircuitBreakerState();
            if (state != null) {
                health.put("circuitBreakerState", state.name());
            }

            // Add metrics if available
            health.put("metrics", geocoder.getMetrics());
        }

        health.put("status", overallHealth ? "UP" : "DOWN");
        return health;
    }

    /**
     * Gets the overall health status of all geocoders.
     *
     * @return true if all geocoders are healthy, false otherwise
     */
    public boolean isHealthy() {
        for (JsonGeocoder geocoder : geocoders) {
            if (!geocoder.isHealthy()) {
                return false;
            }
        }
        return true;
    }
}