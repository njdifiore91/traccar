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

/**
 * Contains metrics information for a geocoder service.
 * This class is used for monitoring and performance analysis in the microservices architecture.
 */
public class GeocoderMetrics {

    private final long totalRequests;
    private final long successfulRequests;
    private final long failedRequests;
    private final long averageResponseTimeMs;
    private final long cacheHits;
    private final long cacheMisses;
    private final long circuitBreakerOpenCount;
    private final long retryCount;

    /**
     * Creates a new GeocoderMetrics with minimal information.
     *
     * @param totalRequests Total number of requests made
     * @param successfulRequests Number of successful requests
     * @param failedRequests Number of failed requests
     * @param averageResponseTimeMs Average response time in milliseconds
     */
    public GeocoderMetrics(long totalRequests, long successfulRequests, long failedRequests, long averageResponseTimeMs) {
        this(totalRequests, successfulRequests, failedRequests, averageResponseTimeMs, 0, 0, 0, 0);
    }

    /**
     * Creates a new GeocoderMetrics with detailed information.
     *
     * @param totalRequests Total number of requests made
     * @param successfulRequests Number of successful requests
     * @param failedRequests Number of failed requests
     * @param averageResponseTimeMs Average response time in milliseconds
     * @param cacheHits Number of cache hits
     * @param cacheMisses Number of cache misses
     * @param circuitBreakerOpenCount Number of times the circuit breaker opened
     * @param retryCount Number of retry attempts
     */
    public GeocoderMetrics(long totalRequests, long successfulRequests, long failedRequests, long averageResponseTimeMs,
                          long cacheHits, long cacheMisses, long circuitBreakerOpenCount, long retryCount) {
        this.totalRequests = totalRequests;
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
        this.averageResponseTimeMs = averageResponseTimeMs;
        this.cacheHits = cacheHits;
        this.cacheMisses = cacheMisses;
        this.circuitBreakerOpenCount = circuitBreakerOpenCount;
        this.retryCount = retryCount;
    }

    /**
     * Gets the total number of requests made.
     *
     * @return Total requests
     */
    public long getTotalRequests() {
        return totalRequests;
    }

    /**
     * Gets the number of successful requests.
     *
     * @return Successful requests
     */
    public long getSuccessfulRequests() {
        return successfulRequests;
    }

    /**
     * Gets the number of failed requests.
     *
     * @return Failed requests
     */
    public long getFailedRequests() {
        return failedRequests;
    }

    /**
     * Gets the average response time in milliseconds.
     *
     * @return Average response time
     */
    public long getAverageResponseTimeMs() {
        return averageResponseTimeMs;
    }

    /**
     * Gets the number of cache hits.
     *
     * @return Cache hits
     */
    public long getCacheHits() {
        return cacheHits;
    }

    /**
     * Gets the number of cache misses.
     *
     * @return Cache misses
     */
    public long getCacheMisses() {
        return cacheMisses;
    }

    /**
     * Gets the number of times the circuit breaker opened.
     *
     * @return Circuit breaker open count
     */
    public long getCircuitBreakerOpenCount() {
        return circuitBreakerOpenCount;
    }

    /**
     * Gets the number of retry attempts.
     *
     * @return Retry count
     */
    public long getRetryCount() {
        return retryCount;
    }
}