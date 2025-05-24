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
package org.traccar.geocoder;

/**
 * Contains metrics information for a geocoder service.
 * This class is used for performance monitoring and metrics collection in a microservices architecture.
 */
public class GeocoderMetrics {

    private final long totalRequests;
    private final long successfulRequests;
    private final long failedRequests;
    private final long averageResponseTimeMs;
    private final long maxResponseTimeMs;
    private final long requestsInLastMinute;
    private final long cacheHits;
    private final long cacheMisses;
    private final long circuitBreakerTrips;
    private final long fallbackActivations;
    private final long retryAttempts;

    /**
     * Constructs a new GeocoderMetrics with the specified parameters.
     *
     * @param totalRequests total number of requests made
     * @param successfulRequests number of successful requests
     * @param failedRequests number of failed requests
     * @param averageResponseTimeMs average response time in milliseconds
     * @param maxResponseTimeMs maximum response time in milliseconds
     * @param requestsInLastMinute number of requests in the last minute
     * @param cacheHits number of cache hits
     * @param cacheMisses number of cache misses
     * @param circuitBreakerTrips number of times the circuit breaker has tripped
     * @param fallbackActivations number of times fallback providers were activated
     * @param retryAttempts number of retry attempts made
     */
    public GeocoderMetrics(
            long totalRequests,
            long successfulRequests,
            long failedRequests,
            long averageResponseTimeMs,
            long maxResponseTimeMs,
            long requestsInLastMinute,
            long cacheHits,
            long cacheMisses,
            long circuitBreakerTrips,
            long fallbackActivations,
            long retryAttempts) {
        this.totalRequests = totalRequests;
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
        this.averageResponseTimeMs = averageResponseTimeMs;
        this.maxResponseTimeMs = maxResponseTimeMs;
        this.requestsInLastMinute = requestsInLastMinute;
        this.cacheHits = cacheHits;
        this.cacheMisses = cacheMisses;
        this.circuitBreakerTrips = circuitBreakerTrips;
        this.fallbackActivations = fallbackActivations;
        this.retryAttempts = retryAttempts;
    }

    /**
     * Returns the total number of requests made.
     *
     * @return total number of requests
     */
    public long getTotalRequests() {
        return totalRequests;
    }

    /**
     * Returns the number of successful requests.
     *
     * @return number of successful requests
     */
    public long getSuccessfulRequests() {
        return successfulRequests;
    }

    /**
     * Returns the number of failed requests.
     *
     * @return number of failed requests
     */
    public long getFailedRequests() {
        return failedRequests;
    }

    /**
     * Returns the average response time in milliseconds.
     *
     * @return average response time in milliseconds
     */
    public long getAverageResponseTimeMs() {
        return averageResponseTimeMs;
    }

    /**
     * Returns the maximum response time in milliseconds.
     *
     * @return maximum response time in milliseconds
     */
    public long getMaxResponseTimeMs() {
        return maxResponseTimeMs;
    }

    /**
     * Returns the number of requests in the last minute.
     *
     * @return number of requests in the last minute
     */
    public long getRequestsInLastMinute() {
        return requestsInLastMinute;
    }

    /**
     * Returns the number of cache hits.
     *
     * @return number of cache hits
     */
    public long getCacheHits() {
        return cacheHits;
    }

    /**
     * Returns the number of cache misses.
     *
     * @return number of cache misses
     */
    public long getCacheMisses() {
        return cacheMisses;
    }

    /**
     * Returns the number of times the circuit breaker has tripped.
     *
     * @return number of circuit breaker trips
     */
    public long getCircuitBreakerTrips() {
        return circuitBreakerTrips;
    }

    /**
     * Returns the number of times fallback providers were activated.
     *
     * @return number of fallback activations
     */
    public long getFallbackActivations() {
        return fallbackActivations;
    }

    /**
     * Returns the number of retry attempts made.
     *
     * @return number of retry attempts
     */
    public long getRetryAttempts() {
        return retryAttempts;
    }

    /**
     * Calculates the success rate as a percentage.
     *
     * @return success rate as a percentage (0-100)
     */
    public double getSuccessRate() {
        return totalRequests > 0 ? (successfulRequests * 100.0) / totalRequests : 0;
    }

    /**
     * Calculates the cache hit rate as a percentage.
     *
     * @return cache hit rate as a percentage (0-100)
     */
    public double getCacheHitRate() {
        long totalCacheRequests = cacheHits + cacheMisses;
        return totalCacheRequests > 0 ? (cacheHits * 100.0) / totalCacheRequests : 0;
    }

    /**
     * Builder pattern for creating GeocoderMetrics instances.
     */
    public static class Builder {
        private long totalRequests;
        private long successfulRequests;
        private long failedRequests;
        private long averageResponseTimeMs;
        private long maxResponseTimeMs;
        private long requestsInLastMinute;
        private long cacheHits;
        private long cacheMisses;
        private long circuitBreakerTrips;
        private long fallbackActivations;
        private long retryAttempts;

        public Builder totalRequests(long totalRequests) {
            this.totalRequests = totalRequests;
            return this;
        }

        public Builder successfulRequests(long successfulRequests) {
            this.successfulRequests = successfulRequests;
            return this;
        }

        public Builder failedRequests(long failedRequests) {
            this.failedRequests = failedRequests;
            return this;
        }

        public Builder averageResponseTimeMs(long averageResponseTimeMs) {
            this.averageResponseTimeMs = averageResponseTimeMs;
            return this;
        }

        public Builder maxResponseTimeMs(long maxResponseTimeMs) {
            this.maxResponseTimeMs = maxResponseTimeMs;
            return this;
        }

        public Builder requestsInLastMinute(long requestsInLastMinute) {
            this.requestsInLastMinute = requestsInLastMinute;
            return this;
        }

        public Builder cacheHits(long cacheHits) {
            this.cacheHits = cacheHits;
            return this;
        }

        public Builder cacheMisses(long cacheMisses) {
            this.cacheMisses = cacheMisses;
            return this;
        }

        public Builder circuitBreakerTrips(long circuitBreakerTrips) {
            this.circuitBreakerTrips = circuitBreakerTrips;
            return this;
        }

        public Builder fallbackActivations(long fallbackActivations) {
            this.fallbackActivations = fallbackActivations;
            return this;
        }

        public Builder retryAttempts(long retryAttempts) {
            this.retryAttempts = retryAttempts;
            return this;
        }

        public GeocoderMetrics build() {
            return new GeocoderMetrics(
                    totalRequests,
                    successfulRequests,
                    failedRequests,
                    averageResponseTimeMs,
                    maxResponseTimeMs,
                    requestsInLastMinute,
                    cacheHits,
                    cacheMisses,
                    circuitBreakerTrips,
                    fallbackActivations,
                    retryAttempts);
        }
    }
}