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
 * Contains health status information for a geocoder service.
 * This class is used for health reporting and monitoring in a microservices architecture.
 */
public class GeocoderHealthStatus {

    private final boolean available;
    private final long responseTimeMs;
    private final int errorCount;
    private final String lastError;
    private final String providerName;
    private final String endpointUrl;
    private final boolean circuitBreakerOpen;
    private final long lastSuccessfulRequestTime;

    /**
     * Constructs a new GeocoderHealthStatus with the specified parameters.
     *
     * @param available whether the geocoder service is available
     * @param responseTimeMs average response time in milliseconds
     * @param errorCount number of errors encountered
     * @param lastError description of the last error encountered
     * @param providerName name of the geocoder provider
     * @param endpointUrl URL of the geocoder service endpoint
     * @param circuitBreakerOpen whether the circuit breaker is open
     * @param lastSuccessfulRequestTime timestamp of the last successful request
     */
    public GeocoderHealthStatus(
            boolean available,
            long responseTimeMs,
            int errorCount,
            String lastError,
            String providerName,
            String endpointUrl,
            boolean circuitBreakerOpen,
            long lastSuccessfulRequestTime) {
        this.available = available;
        this.responseTimeMs = responseTimeMs;
        this.errorCount = errorCount;
        this.lastError = lastError;
        this.providerName = providerName;
        this.endpointUrl = endpointUrl;
        this.circuitBreakerOpen = circuitBreakerOpen;
        this.lastSuccessfulRequestTime = lastSuccessfulRequestTime;
    }

    /**
     * Returns whether the geocoder service is available.
     *
     * @return true if the service is available, false otherwise
     */
    public boolean isAvailable() {
        return available;
    }

    /**
     * Returns the average response time in milliseconds.
     *
     * @return average response time in milliseconds
     */
    public long getResponseTimeMs() {
        return responseTimeMs;
    }

    /**
     * Returns the number of errors encountered.
     *
     * @return number of errors
     */
    public int getErrorCount() {
        return errorCount;
    }

    /**
     * Returns the description of the last error encountered.
     *
     * @return last error description or null if no errors
     */
    public String getLastError() {
        return lastError;
    }

    /**
     * Returns the name of the geocoder provider.
     *
     * @return provider name
     */
    public String getProviderName() {
        return providerName;
    }

    /**
     * Returns the URL of the geocoder service endpoint.
     *
     * @return endpoint URL
     */
    public String getEndpointUrl() {
        return endpointUrl;
    }

    /**
     * Returns whether the circuit breaker is open.
     *
     * @return true if the circuit breaker is open, false otherwise
     */
    public boolean isCircuitBreakerOpen() {
        return circuitBreakerOpen;
    }

    /**
     * Returns the timestamp of the last successful request.
     *
     * @return timestamp of the last successful request
     */
    public long getLastSuccessfulRequestTime() {
        return lastSuccessfulRequestTime;
    }

    /**
     * Builder pattern for creating GeocoderHealthStatus instances.
     */
    public static class Builder {
        private boolean available;
        private long responseTimeMs;
        private int errorCount;
        private String lastError;
        private String providerName;
        private String endpointUrl;
        private boolean circuitBreakerOpen;
        private long lastSuccessfulRequestTime;

        public Builder available(boolean available) {
            this.available = available;
            return this;
        }

        public Builder responseTimeMs(long responseTimeMs) {
            this.responseTimeMs = responseTimeMs;
            return this;
        }

        public Builder errorCount(int errorCount) {
            this.errorCount = errorCount;
            return this;
        }

        public Builder lastError(String lastError) {
            this.lastError = lastError;
            return this;
        }

        public Builder providerName(String providerName) {
            this.providerName = providerName;
            return this;
        }

        public Builder endpointUrl(String endpointUrl) {
            this.endpointUrl = endpointUrl;
            return this;
        }

        public Builder circuitBreakerOpen(boolean circuitBreakerOpen) {
            this.circuitBreakerOpen = circuitBreakerOpen;
            return this;
        }

        public Builder lastSuccessfulRequestTime(long lastSuccessfulRequestTime) {
            this.lastSuccessfulRequestTime = lastSuccessfulRequestTime;
            return this;
        }

        public GeocoderHealthStatus build() {
            return new GeocoderHealthStatus(
                    available,
                    responseTimeMs,
                    errorCount,
                    lastError,
                    providerName,
                    endpointUrl,
                    circuitBreakerOpen,
                    lastSuccessfulRequestTime);
        }
    }
}