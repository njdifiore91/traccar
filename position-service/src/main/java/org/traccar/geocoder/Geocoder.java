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

/**
 * Interface for geocoding services that convert coordinates to human-readable addresses.
 * Updated to support resilience patterns and health status reporting for microservices architecture.
 */
public interface Geocoder {

    /**
     * Interface for asynchronous reverse geocoding callbacks.
     */
    interface ReverseGeocoderCallback {
        void onSuccess(Address address);
        void onFailure(Throwable e);
    }

    /**
     * Gets a human-readable address for the specified coordinates.
     * This method can be implemented synchronously or asynchronously.
     *
     * @param latitude Latitude coordinate
     * @param longitude Longitude coordinate
     * @param callback Optional callback for asynchronous execution (if null, executes synchronously)
     * @return Address object if executed synchronously, null if using callback
     */
    Address getAddress(double latitude, double longitude, ReverseGeocoderCallback callback);

    /**
     * Sets the statistics manager for metrics collection.
     *
     * @param statisticsManager Statistics manager instance
     */
    void setStatisticsManager(StatisticsManager statisticsManager);
    
    /**
     * Reports the health status of the geocoder.
     * Used by the health check system for service discovery and monitoring.
     *
     * @return true if the geocoder is healthy and operational, false otherwise
     */
    default boolean isHealthy() {
        return true; // Default implementation assumes healthy
    }
}