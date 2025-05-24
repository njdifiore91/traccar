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

import com.google.inject.Inject;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Health indicator for geocoding services that exposes health metrics for monitoring
 * and integrates with service discovery for health checks.
 * <p>
 * This component provides visibility into the health and availability of external geocoding services,
 * enabling automated failover and alerting when services degrade.
 */
public class GeocoderHealthIndicator implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderHealthIndicator.class);

    private static final double DEFAULT_TEST_LATITUDE = 40.7128; // New York City
    private static final double DEFAULT_TEST_LONGITUDE = -74.0060;
    private static final long DEFAULT_TIMEOUT_MS = 5000; // 5 seconds

    private final Geocoder geocoder;
    private final MeterRegistry meterRegistry;
    private final AtomicReference<Status> currentStatus = new AtomicReference<>(Status.UNKNOWN);
    private final AtomicReference<String> lastError = new AtomicReference<>("");

    private double testLatitude = DEFAULT_TEST_LATITUDE;
    private double testLongitude = DEFAULT_TEST_LONGITUDE;
    private long timeoutMs = DEFAULT_TIMEOUT_MS;

    /**
     * Creates a new GeocoderHealthIndicator.
     *
     * @param geocoder The geocoder service to monitor
     * @param meterRegistry The meter registry for exposing metrics
     */
    @Inject
    public GeocoderHealthIndicator(Geocoder geocoder, MeterRegistry meterRegistry) {
        this.geocoder = geocoder;
        this.meterRegistry = meterRegistry;
        initializeMetrics();
    }

    /**
     * Initializes metrics for monitoring geocoder health.
     */
    private void initializeMetrics() {
        meterRegistry.gauge("geocoder.health.status", 
                Arrays.asList(Tag.of("provider", getGeocoderProviderName())),
                currentStatus, 
                status -> status.get() == Status.UP ? 1.0 : 0.0);
    }

    /**
     * Gets the name of the geocoder provider for metrics and health reporting.
     *
     * @return The provider name
     */
    private String getGeocoderProviderName() {
        String className = geocoder.getClass().getSimpleName();
        return className.endsWith("Geocoder") 
                ? className.substring(0, className.length() - "Geocoder".length()).toLowerCase() 
                : className.toLowerCase();
    }

    /**
     * Sets custom coordinates for geocoder health check.
     *
     * @param latitude Test latitude
     * @param longitude Test longitude
     * @return This health indicator instance for method chaining
     */
    public GeocoderHealthIndicator withTestCoordinates(double latitude, double longitude) {
        this.testLatitude = latitude;
        this.testLongitude = longitude;
        return this;
    }

    /**
     * Sets custom timeout for geocoder health check.
     *
     * @param timeoutMs Timeout in milliseconds
     * @return This health indicator instance for method chaining
     */
    public GeocoderHealthIndicator withTimeout(long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    /**
     * Performs a health check on the geocoder service.
     *
     * @return Health status with details
     */
    @Override
    public Health health() {
        try {
            CompletableFuture<String> future = new CompletableFuture<>();
            
            geocoder.getAddress(testLatitude, testLongitude, new Geocoder.ReverseGeocoderCallback() {
                @Override
                public void onSuccess(String address) {
                    future.complete(address);
                }

                @Override
                public void onFailure(Throwable e) {
                    future.completeExceptionally(e);
                }
            });

            String address = future.get(timeoutMs, TimeUnit.MILLISECONDS);
            
            if (address != null && !address.isEmpty()) {
                updateStatus(Status.UP, "");
                return Health.up()
                        .withDetail("provider", getGeocoderProviderName())
                        .withDetail("testCoordinates", testLatitude + "," + testLongitude)
                        .withDetail("responseTime", "< " + timeoutMs + "ms")
                        .build();
            } else {
                String error = "Empty address returned from geocoder";
                updateStatus(Status.DOWN, error);
                return Health.down()
                        .withDetail("provider", getGeocoderProviderName())
                        .withDetail("testCoordinates", testLatitude + "," + testLongitude)
                        .withDetail("error", error)
                        .build();
            }
        } catch (Exception e) {
            String error = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
            updateStatus(Status.DOWN, error);
            LOGGER.warn("Geocoder health check failed: {}", error);
            return Health.down(e)
                    .withDetail("provider", getGeocoderProviderName())
                    .withDetail("testCoordinates", testLatitude + "," + testLongitude)
                    .withDetail("error", error)
                    .build();
        }
    }

    /**
     * Updates the current status and records metrics.
     *
     * @param status The new status
     * @param error Error message if status is DOWN
     */
    private void updateStatus(Status status, String error) {
        Status previousStatus = currentStatus.getAndSet(status);
        lastError.set(error);
        
        if (previousStatus != status) {
            if (status == Status.DOWN) {
                LOGGER.warn("Geocoder service {} is DOWN: {}", getGeocoderProviderName(), error);
                // Record failure event for alerting
                meterRegistry.counter("geocoder.health.status.change", 
                        Arrays.asList(
                            Tag.of("provider", getGeocoderProviderName()),
                            Tag.of("status", "down"),
                            Tag.of("previous", previousStatus.getCode())
                        )).increment();
            } else if (status == Status.UP && previousStatus == Status.DOWN) {
                LOGGER.info("Geocoder service {} is back UP", getGeocoderProviderName());
                // Record recovery event
                meterRegistry.counter("geocoder.health.status.change", 
                        Arrays.asList(
                            Tag.of("provider", getGeocoderProviderName()),
                            Tag.of("status", "up"),
                            Tag.of("previous", previousStatus.getCode())
                        )).increment();
            }
        }
    }

    /**
     * Gets the current health status.
     *
     * @return Current status
     */
    public Status getCurrentStatus() {
        return currentStatus.get();
    }

    /**
     * Gets the last error message.
     *
     * @return Last error message or empty string if no error
     */
    public String getLastError() {
        return lastError.get();
    }
}