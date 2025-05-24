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

import org.traccar.database.StatisticsManager;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for geocoding services that convert coordinates to human-readable addresses
 * and vice versa. This interface supports resilience patterns, health reporting, and metrics
 * collection for microservices architecture.
 */
public interface Geocoder {

    /**
     * Callback interface for asynchronous reverse geocoding.
     */
    interface ReverseGeocoderCallback {
        void onSuccess(String address);
        void onFailure(Throwable e);
    }

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
     * Get address string for specified location with callback.
     *
     * @param latitude latitude
     * @param longitude longitude
     * @param callback callback for handling the result
     * @return address string if available immediately, null otherwise
     */
    default String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        CompletableFuture<String> future = getAddress(latitude, longitude);
        future.whenComplete((address, error) -> {
            if (error != null) {
                callback.onFailure(error);
            } else {
                callback.onSuccess(address);
            }
        });
        return null;
    }

    /**
     * Set the statistics manager for tracking geocoder usage.
     *
     * @param statisticsManager the statistics manager
     */
    default void setStatisticsManager(StatisticsManager statisticsManager) {
        // Default implementation does nothing
    }

    /**
     * Check if the geocoder service is available and functioning properly.
     *
     * @return true if the service is healthy, false otherwise
     */
    default boolean isHealthy() {
        return true;
    }

    /**
     * Get detailed health status information about the geocoder service.
     *
     * @return a HealthStatus object containing health metrics
     */
    default GeocoderHealthStatus getHealthStatus() {
        return new GeocoderHealthStatus(isHealthy(), "Default health status", 0);
    }

    /**
     * Register this geocoder instance with the service discovery system.
     * This allows the geocoder to be dynamically discovered by other services.
     *
     * @param serviceId unique identifier for this geocoder service
     * @return true if registration was successful, false otherwise
     */
    default boolean registerWithServiceDiscovery(String serviceId) {
        return true;
    }

    /**
     * Deregister this geocoder instance from the service discovery system.
     *
     * @param serviceId unique identifier for this geocoder service
     * @return true if deregistration was successful, false otherwise
     */
    default boolean deregisterFromServiceDiscovery(String serviceId) {
        return true;
    }

    /**
     * Resolve the endpoint for a geocoding service using service discovery.
     *
     * @param serviceType the type of geocoding service to resolve
     * @return the resolved endpoint URL or null if not found
     */
    default String resolveServiceEndpoint(String serviceType) {
        return null;
    }

    /**
     * Get metrics about the geocoder service operations.
     *
     * @return a GeocoderMetrics object containing performance and usage metrics
     */
    default GeocoderMetrics getMetrics() {
        return new GeocoderMetrics(0, 0, 0, 0);
    }

    /**
     * Reset the circuit breaker if it's in an open state.
     * This allows manual recovery from failure states.
     *
     * @return true if the circuit breaker was reset, false otherwise
     */
    default boolean resetCircuitBreaker() {
        return false;
    }

    /**
     * Configure the retry policy for this geocoder.
     *
     * @param maxRetries maximum number of retry attempts
     * @param initialDelayMs initial delay in milliseconds before first retry
     * @param maxDelayMs maximum delay in milliseconds between retries
     */
    default void configureRetryPolicy(int maxRetries, long initialDelayMs, long maxDelayMs) {
        // Default implementation does nothing
    }

    /**
     * Configure the circuit breaker for this geocoder.
     *
     * @param failureThreshold number of failures before opening the circuit
     * @param resetTimeoutMs time in milliseconds before attempting to close the circuit
     */
    default void configureCircuitBreaker(int failureThreshold, long resetTimeoutMs) {
        // Default implementation does nothing
    }

    /**
     * Set a fallback geocoder to use when this geocoder fails.
     *
     * @param fallbackGeocoder the geocoder to use as a fallback
     */
    default void setFallbackGeocoder(Geocoder fallbackGeocoder) {
        // Default implementation does nothing
    }

    /**
     * Get the current fallback geocoder.
     *
     * @return the current fallback geocoder or null if none is set
     */
    default Geocoder getFallbackGeocoder() {
        return null;
    }
}