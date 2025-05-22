/*
 * Copyright 2020 Anton Tananaev (anton@traccar.org)
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
package org.traccar.speedlimit;

import java.util.Map;

/**
 * SpeedLimitProvider interface defines the contract for services that provide speed limit information.
 * In the microservices architecture, implementations of this interface can be discovered dynamically
 * through service discovery mechanisms and can participate in distributed tracing and circuit breaking.
 */
public interface SpeedLimitProvider {

    /**
     * Callback interface for asynchronous speed limit retrieval operations.
     */
    interface SpeedLimitProviderCallback {

        /**
         * Called when speed limit is successfully retrieved.
         *
         * @param speedLimit The speed limit value in the configured unit system
         */
        void onSuccess(double speedLimit);

        /**
         * Called when speed limit retrieval fails.
         *
         * @param e The exception that caused the failure
         */
        void onFailure(Throwable e);

    }

    /**
     * Health status of the speed limit provider.
     */
    enum HealthStatus {
        /**
         * Provider is operational and responding within expected parameters.
         */
        HEALTHY,
        
        /**
         * Provider is operational but experiencing degraded performance.
         */
        DEGRADED,
        
        /**
         * Provider is not operational.
         */
        UNHEALTHY
    }

    /**
     * Retrieves the speed limit for a given location.
     * This method does not support distributed tracing context propagation.
     * Consider using {@link #getSpeedLimit(double, double, Map, SpeedLimitProviderCallback)} instead.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Callback to handle the result
     */
    void getSpeedLimit(double latitude, double longitude, SpeedLimitProviderCallback callback);

    /**
     * Retrieves the speed limit for a given location with distributed tracing context.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param tracingContext OpenTelemetry tracing context to propagate
     * @param callback Callback to handle the result
     */
    default void getSpeedLimit(double latitude, double longitude, Map<String, String> tracingContext, SpeedLimitProviderCallback callback) {
        // Default implementation falls back to the non-tracing method
        getSpeedLimit(latitude, longitude, callback);
    }

    /**
     * Reports the current health status of this provider.
     * Used by service discovery mechanisms to determine service health.
     *
     * @return Current health status
     */
    default HealthStatus getHealthStatus() {
        return HealthStatus.HEALTHY; // Default implementation assumes healthy
    }

    /**
     * Provides detailed health metrics for monitoring systems.
     *
     * @return Map of metric names to values
     */
    default Map<String, Object> getHealthMetrics() {
        return Map.of("status", getHealthStatus().name());
    }

    /**
     * Registers this provider with the service discovery system.
     * Implementations should register themselves with the configured service registry.
     *
     * @param serviceId Unique identifier for this service instance
     * @param metadata Additional metadata for service registration
     * @return true if registration was successful, false otherwise
     */
    default boolean register(String serviceId, Map<String, String> metadata) {
        return true; // Default implementation assumes successful registration
    }

    /**
     * Deregisters this provider from the service discovery system.
     *
     * @param serviceId Unique identifier for this service instance
     * @return true if deregistration was successful, false otherwise
     */
    default boolean deregister(String serviceId) {
        return true; // Default implementation assumes successful deregistration
    }

    /**
     * Resets the circuit breaker if it's in an open state.
     * This can be used to manually attempt recovery after failures.
     *
     * @return true if reset was successful, false otherwise
     */
    default boolean resetCircuitBreaker() {
        return true; // Default implementation assumes successful reset
    }

    /**
     * Gets the current state of the circuit breaker.
     *
     * @return String representation of circuit breaker state (CLOSED, OPEN, HALF_OPEN, etc.)
     */
    default String getCircuitBreakerState() {
        return "CLOSED"; // Default implementation assumes closed circuit
    }
}