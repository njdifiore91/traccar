/*
 * Copyright 2015 - 2016 Anton Tananaev (anton@traccar.org)
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.trace.Span;
import org.traccar.model.Network;

/**
 * Interface for geolocation providers that resolve geographic coordinates
 * from network information.
 */
public interface GeolocationProvider {

    /**
     * Callback interface for asynchronous geolocation operations.
     * Includes support for distributed tracing context.
     */
    interface LocationProviderCallback {

        /**
         * Called when geolocation operation completes successfully.
         *
         * @param latitude  the resolved latitude
         * @param longitude the resolved longitude
         * @param accuracy  the accuracy of the location in meters
         */
        void onSuccess(double latitude, double longitude, double accuracy);

        /**
         * Called when geolocation operation fails.
         *
         * @param e the exception that caused the failure
         */
        void onFailure(Throwable e);

        /**
         * Get the current tracing span associated with this callback.
         * Used for distributed tracing context propagation.
         *
         * @return the current OpenTelemetry span or null if tracing is not enabled
         */
        default Span getTracingSpan() {
            return null;
        }

        /**
         * Set the current tracing span for this callback.
         * Used for distributed tracing context propagation.
         *
         * @param span the OpenTelemetry span to associate with this callback
         */
        default void setTracingSpan(Span span) {
            // Default implementation does nothing
        }
    }

    /**
     * Get location based on network information.
     *
     * @param network  the network information to use for geolocation
     * @param callback the callback to invoke when operation completes
     */
    void getLocation(Network network, LocationProviderCallback callback);

    /**
     * Get the name of this geolocation provider.
     * Used for service discovery and metrics reporting.
     *
     * @return the provider name
     */
    default String getProviderName() {
        return getClass().getSimpleName();
    }

    /**
     * Check if this provider is currently available.
     * Used for circuit breaker and health check integration.
     *
     * @return true if the provider is available, false otherwise
     */
    default boolean isAvailable() {
        return true;
    }

    /**
     * Get the current health status of this provider.
     * Used for health reporting and monitoring.
     *
     * @return a HealthStatus object representing the current health state
     */
    default HealthStatus getHealthStatus() {
        return new HealthStatus(isAvailable(), null);
    }

    /**
     * Reset the circuit breaker if it's in open state.
     * Used to manually attempt recovery after failures.
     */
    default void resetCircuitBreaker() {
        // Default implementation does nothing
    }

    /**
     * Get the current success rate of this provider.
     * Used for metrics collection and circuit breaker decisions.
     *
     * @return the success rate as a value between 0.0 and 1.0
     */
    default double getSuccessRate() {
        return 1.0; // Default implementation assumes 100% success rate
    }

    /**
     * Health status class for geolocation providers.
     */
    class HealthStatus {
        private final boolean available;
        private final String message;

        /**
         * Create a new health status.
         *
         * @param available whether the provider is available
         * @param message   optional message describing the health status
         */
        public HealthStatus(boolean available, String message) {
            this.available = available;
            this.message = message;
        }

        /**
         * Check if the provider is available.
         *
         * @return true if available, false otherwise
         */
        public boolean isAvailable() {
            return available;
        }

        /**
         * Get the health status message.
         *
         * @return the message or null if not set
         */
        public String getMessage() {
            return message;
        }
    }
}