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
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Health indicator for geocoding services that exposes health metrics for monitoring
 * and integrates with service discovery for health checks.
 */
@Component
public class GeocoderHealthIndicator implements HealthIndicator {

    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Map<String, Geocoder> geocoders;

    /**
     * Constructs a GeocoderHealthIndicator with the specified circuit breaker registry and geocoders.
     *
     * @param circuitBreakerRegistry The circuit breaker registry
     * @param geocoders The map of geocoder names to geocoder instances
     */
    public GeocoderHealthIndicator(CircuitBreakerRegistry circuitBreakerRegistry, Map<String, Geocoder> geocoders) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.geocoders = geocoders;
    }

    /**
     * Provides health information for geocoding services.
     *
     * @return The health status of geocoding services
     */
    @Override
    public Health health() {
        Map<String, Object> details = new HashMap<>();
        boolean allHealthy = true;

        for (Map.Entry<String, Geocoder> entry : geocoders.entrySet()) {
            String geocoderName = entry.getKey();
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.find(geocoderName).orElse(null);

            if (circuitBreaker != null) {
                CircuitBreaker.State state = circuitBreaker.getState();
                boolean isHealthy = state != CircuitBreaker.State.OPEN;

                Map<String, Object> geocoderDetails = new HashMap<>();
                geocoderDetails.put("state", state.name());
                geocoderDetails.put("failureRate", circuitBreaker.getMetrics().getFailureRate());
                geocoderDetails.put("slowCallRate", circuitBreaker.getMetrics().getSlowCallRate());
                geocoderDetails.put("numberOfFailedCalls", circuitBreaker.getMetrics().getNumberOfFailedCalls());
                geocoderDetails.put("numberOfSlowCalls", circuitBreaker.getMetrics().getNumberOfSlowCalls());
                geocoderDetails.put("numberOfSuccessfulCalls", circuitBreaker.getMetrics().getNumberOfSuccessfulCalls());

                details.put(geocoderName, geocoderDetails);

                if (!isHealthy) {
                    allHealthy = false;
                }
            } else {
                details.put(geocoderName, "No circuit breaker configured");
            }
        }

        if (allHealthy) {
            return Health.up().withDetails(details).build();
        } else {
            return Health.down().withDetails(details).build();
        }
    }
}