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

import java.util.concurrent.CompletableFuture;

/**
 * Interface for geocoding services that convert coordinates to human-readable addresses
 * and vice versa. This interface supports resilience patterns, health reporting, and metrics
 * collection for microservices architecture.
 */
public interface Geocoder {

    /**
     * Asynchronously get address string for specified location.
     *
     * @param latitude latitude
     * @param longitude longitude
     * @return future with address string
     */
    CompletableFuture<String> getAddress(double latitude, double longitude);

    /**
     * Asynchronously get address string for specified location with fallback support.
     * If the primary geocoding service fails, the system will attempt to use fallback providers.
     *
     * @param latitude latitude
     * @param longitude longitude
     * @param useFallback whether to use fallback providers if primary fails
     * @return future with address string
     */
    CompletableFuture<String> getAddress(double latitude, double longitude, boolean useFallback);

    /**
     * Check if the geocoder service is available and functioning properly.
     *
     * @return true if the service is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Get detailed health status information about the geocoder service.
     *
     * @return a HealthStatus object containing health metrics
     */
    GeocoderHealthStatus getHealthStatus();

    /**
     * Register this geocoder instance with the service discovery system.
     * This allows the geocoder to be dynamically discovered by other services.
     *
     * @param serviceId unique identifier for this geocoder service
     * @return true if registration was successful, false otherwise
     */
    boolean registerWithServiceDiscovery(String serviceId);

    /**
     * Deregister this geocoder instance from the service discovery system.
     *
     * @param serviceId unique identifier for this geocoder service
     * @return true if deregistration was successful, false otherwise
     */
    boolean deregisterFromServiceDiscovery(String serviceId);

    /**
     * Resolve the endpoint for a geocoding service using service discovery.
     *
     * @param serviceType the type of geocoding service to resolve
     * @return the resolved endpoint URL or null if not found
     */
    String resolveServiceEndpoint(String serviceType);

    /**
     * Get metrics about the geocoder service operations.
     *
     * @return a GeocoderMetrics object containing performance and usage metrics
     */
    GeocoderMetrics getMetrics();

    /**
     * Reset the circuit breaker if it's in an open state.
     * This allows manual recovery from failure states.
     *
     * @return true if the circuit breaker was reset, false otherwise
     */
    boolean resetCircuitBreaker();

    /**
     * Configure the retry policy for this geocoder.
     *
     * @param maxRetries maximum number of retry attempts
     * @param initialDelayMs initial delay in milliseconds before first retry
     * @param maxDelayMs maximum delay in milliseconds between retries
     */
    void configureRetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs);

    /**
     * Configure the circuit breaker for this geocoder.
     *
     * @param failureThreshold number of failures before opening the circuit
     * @param resetTimeoutMs time in milliseconds before attempting to close the circuit
     */
    void configureCircuitBreaker(int failureThreshold, long resetTimeoutMs);

    /**
     * Set a fallback geocoder to use when this geocoder fails.
     *
     * @param fallbackGeocoder the geocoder to use as a fallback
     */
    void setFallbackGeocoder(Geocoder fallbackGeocoder);

    /**
     * Get the current fallback geocoder.
     *
     * @return the current fallback geocoder or null if none is set
     */
    Geocoder getFallbackGeocoder();
}