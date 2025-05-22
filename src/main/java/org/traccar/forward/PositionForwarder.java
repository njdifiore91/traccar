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

import io.opentelemetry.context.Context;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for forwarding position data to external systems with support for
 * asynchronous processing, distributed tracing, metrics collection, and circuit breakers.
 */
public interface PositionForwarder {

    /**
     * Asynchronously forwards position data to external systems.
     * This method supports distributed tracing, metrics collection, and circuit breaker integration.
     *
     * @param positionData   The position data to forward
     * @param tracingContext The OpenTelemetry context for distributed tracing propagation
     * @param metadata       Additional metadata for the forwarding operation (can be null)
     * @return A CompletableFuture that completes with the forwarding result
     */
    CompletableFuture<ForwardingResult> forwardAsync(PositionData positionData, Context tracingContext, ForwardingMetadata metadata);

    /**
     * Synchronous version of the forward method for backward compatibility.
     * This method internally calls the asynchronous version and blocks until completion.
     *
     * @param positionData   The position data to forward
     * @param resultHandler  The handler to process the result
     */
    default void forward(PositionData positionData, ResultHandler resultHandler) {
        forwardAsync(positionData, Context.current(), null)
                .thenAccept(result -> {
                    if (result.isSuccess()) {
                        resultHandler.onResult(true, result.getMessage());
                    } else {
                        resultHandler.onResult(false, result.getMessage());
                    }
                })
                .exceptionally(e -> {
                    resultHandler.onResult(false, e.getMessage());
                    return null;
                });
    }

    /**
     * Checks if this forwarder is available and healthy.
     * Used for service discovery and health checks.
     *
     * @return A CompletableFuture that completes with the health status
     */
    CompletableFuture<Boolean> checkHealth();

    /**
     * Gets the unique identifier for this forwarder.
     * Used for service discovery and metrics collection.
     *
     * @return The forwarder identifier
     */
    String getForwarderId();

    /**
     * Gets the current metrics for this forwarder.
     * Used for monitoring and observability.
     *
     * @return The current metrics for this forwarder
     */
    ForwardingMetrics getMetrics();

    /**
     * Represents the result of a forwarding operation.
     */
    class ForwardingResult {
        private final boolean success;
        private final String message;
        private final long timestamp;

        public ForwardingResult(boolean success, String message) {
            this.success = success;
            this.message = message;
            this.timestamp = System.currentTimeMillis();
        }

        public boolean isSuccess() {
            return success;
        }

        public String getMessage() {
            return message;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Metadata for a forwarding operation.
     */
    class ForwardingMetadata {
        private final String destinationId;
        private final String correlationId;
        private final int priority;
        private final long timeout;

        public ForwardingMetadata(String destinationId, String correlationId, int priority, long timeout) {
            this.destinationId = destinationId;
            this.correlationId = correlationId;
            this.priority = priority;
            this.timeout = timeout;
        }

        public String getDestinationId() {
            return destinationId;
        }

        public String getCorrelationId() {
            return correlationId;
        }

        public int getPriority() {
            return priority;
        }

        public long getTimeout() {
            return timeout;
        }

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private String destinationId;
            private String correlationId;
            private int priority = 0;
            private long timeout = 30000; // Default 30 seconds

            public Builder destinationId(String destinationId) {
                this.destinationId = destinationId;
                return this;
            }

            public Builder correlationId(String correlationId) {
                this.correlationId = correlationId;
                return this;
            }

            public Builder priority(int priority) {
                this.priority = priority;
                return this;
            }

            public Builder timeout(long timeout) {
                this.timeout = timeout;
                return this;
            }

            public ForwardingMetadata build() {
                return new ForwardingMetadata(destinationId, correlationId, priority, timeout);
            }
        }
    }

    /**
     * Metrics for a position forwarder.
     */
    class ForwardingMetrics {
        private final long totalForwarded;
        private final long successfulForwarded;
        private final long failedForwarded;
        private final double averageLatency;
        private final long lastForwardedTimestamp;

        public ForwardingMetrics(long totalForwarded, long successfulForwarded, long failedForwarded, 
                                double averageLatency, long lastForwardedTimestamp) {
            this.totalForwarded = totalForwarded;
            this.successfulForwarded = successfulForwarded;
            this.failedForwarded = failedForwarded;
            this.averageLatency = averageLatency;
            this.lastForwardedTimestamp = lastForwardedTimestamp;
        }

        public long getTotalForwarded() {
            return totalForwarded;
        }

        public long getSuccessfulForwarded() {
            return successfulForwarded;
        }

        public long getFailedForwarded() {
            return failedForwarded;
        }

        public double getAverageLatency() {
            return averageLatency;
        }

        public long getLastForwardedTimestamp() {
            return lastForwardedTimestamp;
        }

        public double getSuccessRate() {
            return totalForwarded > 0 ? (double) successfulForwarded / totalForwarded : 0.0;
        }
    }
}