/*
 * Copyright 2012 - 2023 Anton Tananaev (anton@traccar.org)
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

import java.util.Map;

/**
 * Interface for geocoder implementations that convert coordinates to address information.
 * Extended to support service discovery, metrics collection, health checking, and distributed tracing.
 */
public interface Geocoder {

    /**
     * Callback interface for asynchronous reverse geocoding operations.
     */
    interface ReverseGeocoderCallback {

        /**
         * Called when geocoding operation completes successfully.
         * 
         * @param address The address string result
         */
        void onSuccess(String address);

        /**
         * Called when geocoding operation fails.
         * 
         * @param e The exception that occurred
         */
        void onFailure(Throwable e);

    }

    /**
     * Get address for specified location using callback for asynchronous processing.
     * 
     * @param latitude Latitude
     * @param longitude Longitude
     * @param callback Callback for asynchronous result handling
     * @return Address as string if operation is synchronous, null otherwise
     */
    String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback);

    /**
     * Set statistics manager for legacy metrics collection.
     * 
     * @param statisticsManager Statistics manager instance
     */
    void setStatisticsManager(StatisticsManager statisticsManager);
    
    /**
     * Register with service discovery system.
     * Implementations should register themselves with the service discovery mechanism.
     * 
     * @param serviceId Unique identifier for this geocoder service
     * @param metadata Additional metadata for service registration
     */
    default void registerWithServiceDiscovery(String serviceId, Map<String, String> metadata) {
        // Default implementation does nothing
    }
    
    /**
     * Deregister from service discovery system.
     * Implementations should remove themselves from the service discovery registry.
     * 
     * @param serviceId Unique identifier for this geocoder service
     */
    default void deregisterFromServiceDiscovery(String serviceId) {
        // Default implementation does nothing
    }
    
    /**
     * Get metrics for this geocoder implementation.
     * Returns metrics in OpenMetrics format for Prometheus scraping.
     * 
     * @return Map of metric names to values
     */
    default Map<String, Double> getMetrics() {
        // Default implementation returns empty map
        return Map.of();
    }
    
    /**
     * Check health status of the geocoder service.
     * Used for health checks in containerized environments.
     * 
     * @return true if geocoder is healthy, false otherwise
     */
    default boolean isHealthy() {
        // Default implementation assumes healthy
        return true;
    }
    
    /**
     * Get detailed health information for the geocoder service.
     * Used for health check endpoints in Spring Boot Actuator.
     * 
     * @return Map of health check names to status (UP/DOWN) and details
     */
    default Map<String, Object> getHealthDetails() {
        // Default implementation returns basic health status
        return Map.of("status", isHealthy() ? "UP" : "DOWN");
    }
    
    /**
     * Set trace context for distributed tracing.
     * Implementations should propagate this context in external calls.
     * 
     * @param traceId Trace identifier from OpenTelemetry
     * @param spanId Span identifier from OpenTelemetry
     * @param sampled Whether this trace is being sampled
     */
    default void setTraceContext(String traceId, String spanId, boolean sampled) {
        // Default implementation does nothing
    }
    
    /**
     * Configure the geocoder from environment variables.
     * Used for containerized environments where configuration is passed via environment.
     * 
     * @param environmentPrefix Prefix for environment variables specific to this geocoder
     */
    default void configureFromEnvironment(String environmentPrefix) {
        // Default implementation does nothing
    }
    
    /**
     * Initialize the geocoder with container-specific configuration.
     * 
     * @param containerConfig Map of configuration parameters
     */
    default void initializeForContainer(Map<String, String> containerConfig) {
        // Default implementation does nothing
    }
}