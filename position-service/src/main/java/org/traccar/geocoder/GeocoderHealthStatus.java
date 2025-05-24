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
 * Contains health status information for a geocoder service.
 * This class is used for health reporting and monitoring in the microservices architecture.
 */
public class GeocoderHealthStatus {

    private final boolean healthy;
    private final String statusMessage;
    private final long responseTimeMs;
    private final int successfulRequests;
    private final int failedRequests;
    private final String lastError;

    /**
     * Creates a new GeocoderHealthStatus with minimal information.
     *
     * @param healthy Whether the geocoder service is healthy
     * @param statusMessage Status message describing the current state
     * @param responseTimeMs Average response time in milliseconds
     */
    public GeocoderHealthStatus(boolean healthy, String statusMessage, long responseTimeMs) {
        this(healthy, statusMessage, responseTimeMs, 0, 0, null);
    }

    /**
     * Creates a new GeocoderHealthStatus with detailed information.
     *
     * @param healthy Whether the geocoder service is healthy
     * @param statusMessage Status message describing the current state
     * @param responseTimeMs Average response time in milliseconds
     * @param successfulRequests Count of successful requests
     * @param failedRequests Count of failed requests
     * @param lastError Last error message or null if no errors
     */
    public GeocoderHealthStatus(boolean healthy, String statusMessage, long responseTimeMs,
                               int successfulRequests, int failedRequests, String lastError) {
        this.healthy = healthy;
        this.statusMessage = statusMessage;
        this.responseTimeMs = responseTimeMs;
        this.successfulRequests = successfulRequests;
        this.failedRequests = failedRequests;
        this.lastError = lastError;
    }

    /**
     * Gets whether the geocoder service is healthy.
     *
     * @return true if healthy, false otherwise
     */
    public boolean isHealthy() {
        return healthy;
    }

    /**
     * Gets the status message describing the current state.
     *
     * @return Status message
     */
    public String getStatusMessage() {
        return statusMessage;
    }

    /**
     * Gets the average response time in milliseconds.
     *
     * @return Response time in milliseconds
     */
    public long getResponseTimeMs() {
        return responseTimeMs;
    }

    /**
     * Gets the count of successful requests.
     *
     * @return Count of successful requests
     */
    public int getSuccessfulRequests() {
        return successfulRequests;
    }

    /**
     * Gets the count of failed requests.
     *
     * @return Count of failed requests
     */
    public int getFailedRequests() {
        return failedRequests;
    }

    /**
     * Gets the last error message.
     *
     * @return Last error message or null if no errors
     */
    public String getLastError() {
        return lastError;
    }
}