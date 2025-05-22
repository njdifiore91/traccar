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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and reports metrics for geocoding operations using Micrometer.
 * Enables monitoring of geocoding performance, success rates, and service availability.
 * Tracks metrics such as request counts, response times, error rates, and cache hit ratios
 * for each geocoding provider.
 */
public class GeocoderMetrics {

    private final MeterRegistry registry;
    private final String providerName;
    
    private final Timer requestTimer;
    private final Counter requestCounter;
    private final Counter successCounter;
    private final Counter errorCounter;
    private final Counter cacheHitCounter;
    private final Counter cacheMissCounter;
    
    private final AtomicInteger activeRequests = new AtomicInteger(0);
    private final AtomicLong totalRequests = new AtomicLong(0);
    private final AtomicLong cacheHits = new AtomicLong(0);

    /**
     * Creates a new GeocoderMetrics instance for the specified provider.
     *
     * @param registry     The Micrometer registry to use for metrics reporting
     * @param providerName The name of the geocoding provider (e.g., "google", "nominatim")
     */
    public GeocoderMetrics(MeterRegistry registry, String providerName) {
        this.registry = registry;
        this.providerName = providerName;

        // Initialize metrics with provider tag
        requestTimer = Timer.builder("geocoder.request.duration")
                .description("Time taken to complete geocoding requests")
                .tag("provider", providerName)
                .register(registry);

        requestCounter = Counter.builder("geocoder.requests.total")
                .description("Total number of geocoding requests")
                .tag("provider", providerName)
                .register(registry);

        successCounter = Counter.builder("geocoder.requests.success")
                .description("Number of successful geocoding requests")
                .tag("provider", providerName)
                .register(registry);

        errorCounter = Counter.builder("geocoder.requests.error")
                .description("Number of failed geocoding requests")
                .tag("provider", providerName)
                .register(registry);

        cacheHitCounter = Counter.builder("geocoder.cache.hits")
                .description("Number of geocoding cache hits")
                .tag("provider", providerName)
                .register(registry);

        cacheMissCounter = Counter.builder("geocoder.cache.misses")
                .description("Number of geocoding cache misses")
                .tag("provider", providerName)
                .register(registry);

        // Register gauges for derived metrics
        Gauge.builder("geocoder.requests.active", activeRequests, AtomicInteger::get)
                .description("Number of active geocoding requests")
                .tag("provider", providerName)
                .register(registry);

        Gauge.builder("geocoder.cache.hit.ratio", this, GeocoderMetrics::getCacheHitRatio)
                .description("Ratio of cache hits to total requests")
                .tag("provider", providerName)
                .register(registry);

        Gauge.builder("geocoder.requests.error.ratio", this, GeocoderMetrics::getErrorRatio)
                .description("Ratio of failed requests to total requests")
                .tag("provider", providerName)
                .register(registry);
    }

    /**
     * Records the start of a geocoding request.
     * Increments the active requests counter and the total requests counter.
     *
     * @return A Timer.Sample that should be used to record the request duration
     */
    public Timer.Sample recordRequestStart() {
        activeRequests.incrementAndGet();
        requestCounter.increment();
        totalRequests.incrementAndGet();
        return Timer.start(registry);
    }

    /**
     * Records the successful completion of a geocoding request.
     * Decrements the active requests counter and increments the success counter.
     *
     * @param sample The Timer.Sample created by recordRequestStart()
     */
    public void recordRequestSuccess(Timer.Sample sample) {
        activeRequests.decrementAndGet();
        successCounter.increment();
        sample.stop(requestTimer);
    }

    /**
     * Records a failed geocoding request.
     * Decrements the active requests counter and increments the error counter.
     *
     * @param sample The Timer.Sample created by recordRequestStart()
     */
    public void recordRequestError(Timer.Sample sample) {
        activeRequests.decrementAndGet();
        errorCounter.increment();
        sample.stop(requestTimer);
    }

    /**
     * Records a cache hit for a geocoding request.
     * Increments the cache hit counter and updates the cache hit ratio.
     */
    public void recordCacheHit() {
        cacheHitCounter.increment();
        cacheHits.incrementAndGet();
    }

    /**
     * Records a cache miss for a geocoding request.
     * Increments the cache miss counter.
     */
    public void recordCacheMiss() {
        cacheMissCounter.increment();
    }

    /**
     * Records the duration of a geocoding request directly.
     * This method can be used when the Timer.Sample approach is not suitable.
     *
     * @param durationNanos The duration of the request in nanoseconds
     */
    public void recordRequestDuration(long durationNanos) {
        requestTimer.record(durationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Calculates the cache hit ratio (hits / total requests).
     *
     * @return The cache hit ratio as a double between 0.0 and 1.0
     */
    private double getCacheHitRatio() {
        long total = totalRequests.get();
        return total > 0 ? (double) cacheHits.get() / total : 0.0;
    }

    /**
     * Calculates the error ratio (errors / total requests).
     *
     * @return The error ratio as a double between 0.0 and 1.0
     */
    private double getErrorRatio() {
        long total = totalRequests.get();
        long errors = (long) errorCounter.count();
        return total > 0 ? (double) errors / total : 0.0;
    }

    /**
     * Gets the name of the geocoding provider associated with these metrics.
     *
     * @return The provider name
     */
    public String getProviderName() {
        return providerName;
    }
}