/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.forward;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for handling results of forwarding operations with support for
 * asynchronous callbacks, distributed tracing, structured error information,
 * circuit breaker integration, and metrics collection.
 */
public interface ResultHandler {

    /**
     * Represents structured error information for forwarding operations.
     */
    class ErrorInfo {
        private final String errorCode;
        private final String errorMessage;
        private final Throwable cause;
        private final Map<String, String> metadata;

        public ErrorInfo(String errorCode, String errorMessage, Throwable cause, Map<String, String> metadata) {
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
            this.cause = cause;
            this.metadata = metadata;
        }

        public String getErrorCode() {
            return errorCode;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public Throwable getCause() {
            return cause;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }
    }

    /**
     * Synchronous callback for handling operation results.
     * 
     * @param success   true if the operation was successful, false otherwise
     * @param throwable the exception that occurred, or null if the operation was successful
     */
    void onResult(boolean success, Throwable throwable);

    /**
     * Asynchronous callback for handling operation results.
     * 
     * @param success   true if the operation was successful, false otherwise
     * @param throwable the exception that occurred, or null if the operation was successful
     * @param traceContext map containing distributed tracing context information
     * @return CompletableFuture that completes when the result has been processed
     */
    default CompletableFuture<Void> onResultAsync(boolean success, Throwable throwable, Map<String, String> traceContext) {
        onResult(success, throwable);
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Asynchronous callback for handling operation results with structured error information.
     * 
     * @param success    true if the operation was successful, false otherwise
     * @param errorInfo  structured error information, or null if the operation was successful
     * @param traceContext map containing distributed tracing context information
     * @return CompletableFuture that completes when the result has been processed
     */
    default CompletableFuture<Void> onResultAsync(boolean success, ErrorInfo errorInfo, Map<String, String> traceContext) {
        return onResultAsync(success, errorInfo != null ? errorInfo.getCause() : null, traceContext);
    }

    /**
     * Callback for circuit breaker state changes.
     * 
     * @param open       true if the circuit breaker is open, false otherwise
     * @param serviceName the name of the service associated with the circuit breaker
     * @param metrics    metrics associated with the circuit breaker state change
     */
    default void onCircuitBreakerStateChange(boolean open, String serviceName, Map<String, Object> metrics) {
        // Default implementation does nothing
    }

    /**
     * Callback for collecting metrics related to forwarding operations.
     * 
     * @param operationName the name of the operation being measured
     * @param durationMs    the duration of the operation in milliseconds
     * @param success       true if the operation was successful, false otherwise
     * @param metadata      additional metadata for the metrics
     */
    default void recordMetrics(String operationName, long durationMs, boolean success, Map<String, String> metadata) {
        // Default implementation does nothing
    }
}