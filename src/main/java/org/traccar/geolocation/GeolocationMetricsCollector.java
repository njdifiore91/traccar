/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import org.traccar.model.Network;

/**
 * Collects and reports performance metrics for geolocation operations using OpenTelemetry.
 * Tracks success rates, response times, and error rates for different geolocation providers.
 * Exposes metrics for monitoring systems via Prometheus endpoint.
 */
public class GeolocationMetricsCollector {

    private final Meter meter;
    private final LongCounter successCounter;
    private final LongCounter failureCounter;
    private final DoubleHistogram requestDurationHistogram;
    private final LongCounter requestCounter;

    /**
     * Creates a new GeolocationMetricsCollector with the provided OpenTelemetry instance.
     *
     * @param openTelemetry The OpenTelemetry instance to use for metrics collection
     */
    public GeolocationMetricsCollector(OpenTelemetry openTelemetry) {
        this.meter = openTelemetry.getMeter("org.traccar.geolocation");
        
        // Create counter for all geolocation requests
        this.requestCounter = meter.counterBuilder("geolocation_requests_total")
                .setDescription("Total number of geolocation requests")
                .setUnit("{requests}")
                .build();
        
        // Create counters for successful and failed geolocation requests
        this.successCounter = meter.counterBuilder("geolocation_requests_success_total")
                .setDescription("Total number of successful geolocation requests")
                .setUnit("{requests}")
                .build();
        
        this.failureCounter = meter.counterBuilder("geolocation_requests_failure_total")
                .setDescription("Total number of failed geolocation requests")
                .setUnit("{requests}")
                .build();
        
        // Create histogram for request duration
        this.requestDurationHistogram = meter.histogramBuilder("geolocation_request_duration_seconds")
                .setDescription("Duration of geolocation requests")
                .setUnit("s")
                .build();
    }

    /**
     * Records a successful geolocation request.
     *
     * @param providerName The name of the geolocation provider
     * @param durationMillis The duration of the request in milliseconds
     */
    public void recordSuccess(String providerName, long durationMillis) {
        Attributes attributes = Attributes.builder()
                .put("provider", providerName)
                .build();
        
        requestCounter.add(1, attributes);
        successCounter.add(1, attributes);
        requestDurationHistogram.record(durationMillis / 1000.0, attributes);
    }

    /**
     * Records a failed geolocation request.
     *
     * @param providerName The name of the geolocation provider
     * @param errorType The type of error that occurred
     * @param durationMillis The duration of the request in milliseconds
     */
    public void recordFailure(String providerName, String errorType, long durationMillis) {
        Attributes attributes = Attributes.builder()
                .put("provider", providerName)
                .put("error_type", errorType)
                .build();
        
        Attributes requestAttributes = Attributes.builder()
                .put("provider", providerName)
                .build();
        
        requestCounter.add(1, requestAttributes);
        failureCounter.add(1, attributes);
        requestDurationHistogram.record(durationMillis / 1000.0, attributes);
    }

    /**
     * Creates a callback wrapper that records metrics for geolocation operations.
     *
     * @param providerName The name of the geolocation provider
     * @param startTime The start time of the request in milliseconds
     * @param callback The original callback to wrap
     * @return A new callback that records metrics and delegates to the original callback
     */
    /**
     * Tracks a geolocation request for a specific network.
     * 
     * @param provider The geolocation provider being used
     * @param network The network information for the request
     * @param callback The original callback to be invoked after the request completes
     * @return A wrapped callback that collects metrics
     */
    public GeolocationProvider.LocationProviderCallback trackRequest(
            GeolocationProvider provider,
            Network network,
            GeolocationProvider.LocationProviderCallback callback) {
        String providerName = provider.getClass().getSimpleName();
        long startTime = System.currentTimeMillis();
        return wrapCallback(providerName, startTime, callback);
    }
    
    /**
     * Creates a callback wrapper that records metrics for geolocation operations.
     *
     * @param providerName The name of the geolocation provider
     * @param startTime The start time of the request in milliseconds
     * @param callback The original callback to wrap
     * @return A new callback that records metrics and delegates to the original callback
     */
    public GeolocationProvider.LocationProviderCallback wrapCallback(
            String providerName,
            long startTime,
            GeolocationProvider.LocationProviderCallback callback) {
        
        return new GeolocationProvider.LocationProviderCallback() {
            @Override
            public void onSuccess(double latitude, double longitude, double accuracy) {
                long duration = System.currentTimeMillis() - startTime;
                recordSuccess(providerName, duration);
                callback.onSuccess(latitude, longitude, accuracy);
            }

            @Override
            public void onFailure(Throwable e) {
                long duration = System.currentTimeMillis() - startTime;
                String errorType = e.getClass().getSimpleName();
                recordFailure(providerName, errorType, duration);
                callback.onFailure(e);
            }
        };
    }
}