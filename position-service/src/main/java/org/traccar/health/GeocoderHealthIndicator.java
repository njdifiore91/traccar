/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.health;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.geocoder.Geocoder;
import org.traccar.geocoder.Geocoder.ReverseGeocoderCallback;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Health indicator for geocoder service connectivity and performance.
 * Monitors the health of the geocoder service used by the Position Processing Service.
 */
@Component
public class GeocoderHealthIndicator implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderHealthIndicator.class);

    private static final String GEOCODER_TYPE_KEY = "geocoderType";
    private static final String CONNECTION_STATUS_KEY = "connectionStatus";
    private static final String RESPONSE_TIME_KEY = "responseTimeMs";
    private static final String ERROR_RATE_KEY = "errorRate";
    private static final String REQUESTS_TOTAL_KEY = "requestsTotal";
    private static final String ERRORS_TOTAL_KEY = "errorsTotal";
    private static final String LAST_ERROR_KEY = "lastError";
    private static final String LAST_SUCCESS_TIME_KEY = "lastSuccessTime";

    private static final int TIMEOUT_SECONDS = 5;
    private static final double ERROR_THRESHOLD = 0.1; // 10% error rate threshold
    private static final long MAX_RESPONSE_TIME_MS = 2000; // 2 seconds max response time

    // Test coordinates (San Francisco)
    private static final double TEST_LATITUDE = 37.7749;
    private static final double TEST_LONGITUDE = -122.4194;

    @Autowired
    private Config config;

    @Autowired
    private Geocoder geocoder;

    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    // Metrics for tracking geocoder performance
    private final AtomicInteger totalRequests = new AtomicInteger(0);
    private final AtomicInteger failedRequests = new AtomicInteger(0);
    private final AtomicLong lastSuccessTimestamp = new AtomicLong(0);
    private String lastErrorMessage = null;

    // Metrics for Prometheus/Micrometer
    private Timer responseTimeTimer;
    private Counter requestsCounter;
    private Counter errorsCounter;

    @Autowired
    public void init() {
        if (meterRegistry != null) {
            responseTimeTimer = Timer.builder("geocoder.response.time")
                    .description("Geocoder service response time")
                    .register(meterRegistry);

            requestsCounter = Counter.builder("geocoder.requests.total")
                    .description("Total number of geocoder requests")
                    .register(meterRegistry);

            errorsCounter = Counter.builder("geocoder.errors.total")
                    .description("Total number of geocoder errors")
                    .register(meterRegistry);
        }
    }

    @Override
    public Health health() {
        if (geocoder == null) {
            return Health.down()
                    .withDetail("error", "Geocoder service not configured")
                    .build();
        }

        String geocoderType = config.getString(Keys.GEOCODER_TYPE);
        if (geocoderType == null || geocoderType.isEmpty()) {
            geocoderType = "unknown";
        }

        Health.Builder builder = new Health.Builder();
        builder.withDetail(GEOCODER_TYPE_KEY, geocoderType);

        try {
            // Test geocoder with a sample request
            CompletableFuture<String> future = new CompletableFuture<>();
            long startTime = System.currentTimeMillis();

            if (requestsCounter != null) {
                requestsCounter.increment();
            }
            totalRequests.incrementAndGet();

            geocoder.getAddress(TEST_LATITUDE, TEST_LONGITUDE, new ReverseGeocoderCallback() {
                @Override
                public void onSuccess(String address) {
                    long responseTime = System.currentTimeMillis() - startTime;
                    if (responseTimeTimer != null) {
                        responseTimeTimer.record(responseTime, TimeUnit.MILLISECONDS);
                    }
                    lastSuccessTimestamp.set(System.currentTimeMillis());
                    future.complete(address);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (errorsCounter != null) {
                        errorsCounter.increment();
                    }
                    failedRequests.incrementAndGet();
                    lastErrorMessage = e.getMessage();
                    future.completeExceptionally(e);
                }
            });

            // Wait for the geocoder response with timeout
            String address = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            long responseTime = System.currentTimeMillis() - startTime;

            // Calculate error rate
            double errorRate = totalRequests.get() > 0 
                    ? (double) failedRequests.get() / totalRequests.get() 
                    : 0.0;

            // Add metrics to health check
            builder.withDetail(CONNECTION_STATUS_KEY, "connected")
                   .withDetail(RESPONSE_TIME_KEY, responseTime)
                   .withDetail(ERROR_RATE_KEY, errorRate)
                   .withDetail(REQUESTS_TOTAL_KEY, totalRequests.get())
                   .withDetail(ERRORS_TOTAL_KEY, failedRequests.get())
                   .withDetail(LAST_SUCCESS_TIME_KEY, lastSuccessTimestamp.get());

            // Determine health status based on error rate and response time
            if (errorRate > ERROR_THRESHOLD) {
                builder.down()
                       .withDetail("error", "Error rate exceeds threshold: " + errorRate);
            } else if (responseTime > MAX_RESPONSE_TIME_MS) {
                builder.down()
                       .withDetail("error", "Response time exceeds threshold: " + responseTime + "ms");
            } else {
                builder.up();
            }

            // Include sample address in the health check
            if (address != null && !address.isEmpty()) {
                builder.withDetail("sampleAddress", address);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return handleGeocoderError(builder, "Geocoder request interrupted", e);
        } catch (ExecutionException e) {
            return handleGeocoderError(builder, "Geocoder execution error", e.getCause());
        } catch (TimeoutException e) {
            return handleGeocoderError(builder, "Geocoder request timed out after " + TIMEOUT_SECONDS + " seconds", e);
        } catch (Exception e) {
            return handleGeocoderError(builder, "Unexpected error checking geocoder health", e);
        }

        return builder.build();
    }

    private Health handleGeocoderError(Health.Builder builder, String message, Throwable e) {
        LOGGER.error(message, e);
        if (errorsCounter != null) {
            errorsCounter.increment();
        }
        failedRequests.incrementAndGet();
        lastErrorMessage = e.getMessage();

        // Calculate error rate
        double errorRate = totalRequests.get() > 0 
                ? (double) failedRequests.get() / totalRequests.get() 
                : 0.0;

        return builder.down()
                .withDetail(CONNECTION_STATUS_KEY, "error")
                .withDetail(ERROR_RATE_KEY, errorRate)
                .withDetail(REQUESTS_TOTAL_KEY, totalRequests.get())
                .withDetail(ERRORS_TOTAL_KEY, failedRequests.get())
                .withDetail(LAST_ERROR_KEY, lastErrorMessage)
                .withDetail(LAST_SUCCESS_TIME_KEY, lastSuccessTimestamp.get())
                .withDetail("error", message + ": " + e.getMessage())
                .build();
    }
}