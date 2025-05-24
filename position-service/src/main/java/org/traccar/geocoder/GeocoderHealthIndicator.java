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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderHealthIndicator.class);

    private final ResilienceConfig resilienceConfig;
    private final Map<String, Geocoder> geocoders;

    @Autowired
    public GeocoderHealthIndicator(ResilienceConfig resilienceConfig, Map<String, Geocoder> geocoders) {
        this.resilienceConfig = resilienceConfig;
        this.geocoders = geocoders;
    }

    @Override
    public Health health() {
        Map<String, CircuitBreaker.State> states = resilienceConfig.getCircuitBreakerStates();
        Map<String, Object> details = new HashMap<>();
        boolean isHealthy = true;

        // Check if any circuit breakers are open
        for (Map.Entry<String, CircuitBreaker.State> entry : states.entrySet()) {
            String provider = entry.getKey();
            CircuitBreaker.State state = entry.getValue();
            details.put(provider + ".state", state.name());
            
            // If any circuit breaker is OPEN or FORCED_OPEN, mark as unhealthy
            if (state == CircuitBreaker.State.OPEN || state == CircuitBreaker.State.FORCED_OPEN) {
                isHealthy = false;
                LOGGER.warn("Geocoder circuit breaker for {} is in {} state", provider, state);
            }
        }

        // Add metrics for each geocoder
        for (Map.Entry<String, Geocoder> entry : geocoders.entrySet()) {
            String provider = entry.getKey();
            Geocoder geocoder = entry.getValue();
            
            if (geocoder instanceof GeoapifyGeocoder) {
                GeoapifyGeocoder geoapifyGeocoder = (GeoapifyGeocoder) geocoder;
                details.put(provider + ".metrics", geoapifyGeocoder.getMetrics());
            }
            // Add similar checks for other geocoder implementations as they are updated
        }

        // Add circuit breaker metrics
        details.put("circuitBreakerMetrics", resilienceConfig.getCircuitBreakerMetrics());

        if (isHealthy) {
            return Health.up().withDetails(details).build();
        } else {
            return Health.down().withDetails(details).build();
        }
    }

    /**
     * Checks if a specific geocoder provider is healthy
     * 
     * @param provider the geocoder provider name
     * @return true if the provider is healthy, false otherwise
     */
    public boolean isProviderHealthy(String provider) {
        Map<String, CircuitBreaker.State> states = resilienceConfig.getCircuitBreakerStates();
        CircuitBreaker.State state = states.get(provider);
        
        if (state == null) {
            // If we don't have a circuit breaker for this provider, assume it's healthy
            return true;
        }
        
        return state != CircuitBreaker.State.OPEN && state != CircuitBreaker.State.FORCED_OPEN;
    }

    /**
     * Gets detailed health information for all geocoder providers
     * 
     * @return a map of provider names to their health status
     */
    public Map<String, Object> getDetailedHealth() {
        Map<String, Object> health = new HashMap<>();
        Map<String, CircuitBreaker.State> states = resilienceConfig.getCircuitBreakerStates();
        
        for (Map.Entry<String, CircuitBreaker.State> entry : states.entrySet()) {
            String provider = entry.getKey();
            CircuitBreaker.State state = entry.getValue();
            
            Map<String, Object> providerHealth = new HashMap<>();
            providerHealth.put("state", state.name());
            providerHealth.put("healthy", state != CircuitBreaker.State.OPEN && state != CircuitBreaker.State.FORCED_OPEN);
            
            // Add metrics if available
            Map<String, Map<String, Float>> metrics = resilienceConfig.getCircuitBreakerMetrics();
            if (metrics.containsKey(provider)) {
                providerHealth.put("metrics", metrics.get(provider));
            }
            
            health.put(provider, providerHealth);
        }
        
        return health;
    }
}