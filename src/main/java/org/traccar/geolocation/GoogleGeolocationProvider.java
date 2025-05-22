/*
 * Copyright 2016 - 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.geolocation;

import jakarta.ws.rs.client.Client;

import io.opentelemetry.api.trace.Tracer;

import javax.inject.Inject;
import javax.inject.Named;

/**
 * Google Geolocation API provider.
 * Implements circuit breaker pattern for resilience and includes distributed tracing.
 */
public class GoogleGeolocationProvider extends UniversalGeolocationProvider {

    private static final String URL = "https://www.googleapis.com/geolocation/v1/geolocate";

    /**
     * Creates a new instance of the GoogleGeolocationProvider.
     *
     * @param client HTTP client for making requests
     * @param key Google Geolocation API key
     * @param circuitBreakerFactory Factory for creating circuit breakers
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metricsCollector Collector for geolocation metrics
     * @param serviceManager Service manager for service discovery
     */
    @Inject
    public GoogleGeolocationProvider(
            Client client, 
            @Named("geolocation.google.key") String key,
            GeolocationCircuitBreakerFactory circuitBreakerFactory,
            Tracer tracer,
            GeolocationMetricsCollector metricsCollector,
            GeolocationServiceManager serviceManager) {
        
        // Use service discovery to get the URL if available, otherwise use the default URL
        String serviceUrl = serviceManager.getServiceUrl("google-geolocation").orElse(URL);
        
        // Pass all dependencies to the parent class constructor
        super(client, serviceUrl, key, circuitBreakerFactory, tracer, metricsCollector);
    }

}