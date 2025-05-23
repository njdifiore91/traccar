/*
 * Copyright 2012 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import io.micrometer.core.instrument.MeterRegistry;
import io.netty.util.concurrent.Future;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;

/**
 * Interface for tracker connectors that handle device communication.
 * Provides common functionality for service discovery, health checks,
 * metrics collection, and containerization support.
 *
 * This interface serves as the foundation for both server-side (TrackerServer)
 * and client-side (TrackerClient) connectors in the microservices architecture.
 * It ensures consistent implementation of service discovery, health reporting,
 * metrics collection, and containerization support across all connectors.
 */
public interface TrackerConnector {

    /**
     * Gets the protocol name for this connector.
     *
     * @return The protocol name
     */
    String getProtocol();
    
    /**
     * Gets the service registry used by this connector.
     * 
     * @return The service registry
     */
    ServiceRegistry getServiceRegistry();
    
    /**
     * Gets the meter registry used by this connector for metrics collection.
     * 
     * @return The meter registry
     */
    MeterRegistry getMeterRegistry();

    /**
     * Starts the connector and registers it with service discovery.
     * In a containerized environment, this method should be aware of container
     * lifecycle events and configure itself accordingly.
     *
     * @return A Future that will be notified when the connector is started
     */
    Future<?> start();

    /**
     * Stops the connector and deregisters it from service discovery.
     * In a containerized environment, this method should implement graceful
     * shutdown to allow existing connections to complete before terminating.
     *
     * @return A Future that will be notified when the connector is stopped
     */
    Future<?> stop();

    /**
     * Registers the connector with the service discovery mechanism.
     * This method should create a service instance with appropriate metadata
     * and register it with the service registry.
     *
     * @return The registered service instance
     */
    ServiceInstance registerWithServiceDiscovery();

    /**
     * Deregisters the connector from the service discovery mechanism.
     * This method should be called during graceful shutdown to ensure
     * the service is properly removed from the registry.
     */
    void deregisterFromServiceDiscovery();
    
    /**
     * Gets the current service instance for this connector.
     * 
     * @return The current service instance, or null if not registered
     */
    ServiceInstance getServiceInstance();

    /**
     * Gets the health status of the connector.
     * This method is used by Kubernetes liveness probes to determine
     * if the connector is functioning correctly.
     *
     * @return true if the connector is healthy, false otherwise
     */
    boolean isHealthy();
    
    /**
     * Gets the readiness status of the connector.
     * This method is used by Kubernetes readiness probes to determine
     * if the connector is ready to accept traffic.
     * 
     * @return true if the connector is ready, false otherwise
     */
    boolean isReady();

    /**
     * Gets detailed health information for the connector.
     * This method provides comprehensive health data for monitoring
     * and diagnostics.
     *
     * @return A HealthStatus object containing detailed health information
     */
    HealthStatus getHealthStatus();

    /**
     * Gets the number of active connections or clients.
     * This metric is exposed to Prometheus for monitoring.
     *
     * @return The number of active connections or clients
     */
    int getActiveConnections();

    /**
     * Gets the connection statistics for the connector.
     * This provides comprehensive metrics for monitoring and alerting.
     *
     * @return A ConnectionStatistics object containing connection statistics
     */
    ConnectionStatistics getConnectionStatistics();
    
    /**
     * Records a connection event (accepted, rejected, or closed).
     * This method should update the appropriate metrics.
     * 
     * @param eventType The type of connection event
     * @param durationMillis The duration of the connection in milliseconds (for closed events)
     */
    void recordConnectionEvent(ConnectionEventType eventType, long durationMillis);
    
    /**
     * Enum representing the types of connection events.
     */
    enum ConnectionEventType {
        /**
         * A connection was accepted.
         */
        ACCEPTED,
        
        /**
         * A connection was rejected.
         */
        REJECTED,
        
        /**
         * A connection was closed.
         */
        CLOSED
    }

    /**
     * Class representing the health status of a connector.
     * This class provides detailed health information for monitoring
     * and diagnostics in a containerized environment.
     */
    class HealthStatus {
        private final boolean healthy;
        private final boolean ready;
        private final String status;
        private final String details;
        private final long timestamp;

        /**
         * Constructs a new HealthStatus with the specified parameters.
         *
         * @param healthy Whether the connector is healthy
         * @param ready Whether the connector is ready to accept traffic
         * @param status The status message
         * @param details Detailed health information
         */
        public HealthStatus(boolean healthy, boolean ready, String status, String details) {
            this.healthy = healthy;
            this.ready = ready;
            this.status = status;
            this.details = details;
            this.timestamp = System.currentTimeMillis();
        }

        /**
         * Gets whether the connector is healthy.
         *
         * @return true if the connector is healthy, false otherwise
         */
        public boolean isHealthy() {
            return healthy;
        }
        
        /**
         * Gets whether the connector is ready to accept traffic.
         *
         * @return true if the connector is ready, false otherwise
         */
        public boolean isReady() {
            return ready;
        }

        /**
         * Gets the status message.
         *
         * @return The status message
         */
        public String getStatus() {
            return status;
        }

        /**
         * Gets detailed health information.
         *
         * @return Detailed health information
         */
        public String getDetails() {
            return details;
        }
        
        /**
         * Gets the timestamp when this health status was created.
         *
         * @return The timestamp in milliseconds since epoch
         */
        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Class representing connection statistics for a connector.
     * This class provides metrics for monitoring connection activity
     * and performance in a containerized environment.
     */
    class ConnectionStatistics {
        private final int activeConnections;
        private final long totalAccepted;
        private final long totalRejected;
        private final long totalClosed;
        private final double averageDuration;
        private final double messageRate;
        private final double errorRate;
        private final long timestamp;

        /**
         * Constructs a new ConnectionStatistics with the specified parameters.
         *
         * @param activeConnections The number of active connections
         * @param totalAccepted The total number of accepted connections
         * @param totalRejected The total number of rejected connections
         * @param totalClosed The total number of closed connections
         * @param averageDuration The average connection duration in milliseconds
         * @param messageRate The rate of messages processed per second
         * @param errorRate The rate of errors per second
         */
        public ConnectionStatistics(
                int activeConnections,
                long totalAccepted,
                long totalRejected,
                long totalClosed,
                double averageDuration,
                double messageRate,
                double errorRate) {
            this.activeConnections = activeConnections;
            this.totalAccepted = totalAccepted;
            this.totalRejected = totalRejected;
            this.totalClosed = totalClosed;
            this.averageDuration = averageDuration;
            this.messageRate = messageRate;
            this.errorRate = errorRate;
            this.timestamp = System.currentTimeMillis();
        }

        /**
         * Gets the number of active connections.
         *
         * @return The number of active connections
         */
        public int getActiveConnections() {
            return activeConnections;
        }

        /**
         * Gets the total number of accepted connections.
         *
         * @return The total number of accepted connections
         */
        public long getTotalAccepted() {
            return totalAccepted;
        }

        /**
         * Gets the total number of rejected connections.
         *
         * @return The total number of rejected connections
         */
        public long getTotalRejected() {
            return totalRejected;
        }

        /**
         * Gets the total number of closed connections.
         *
         * @return The total number of closed connections
         */
        public long getTotalClosed() {
            return totalClosed;
        }

        /**
         * Gets the average connection duration in milliseconds.
         *
         * @return The average connection duration in milliseconds
         */
        public double getAverageDuration() {
            return averageDuration;
        }
        
        /**
         * Gets the rate of messages processed per second.
         *
         * @return The message rate
         */
        public double getMessageRate() {
            return messageRate;
        }
        
        /**
         * Gets the rate of errors per second.
         *
         * @return The error rate
         */
        public double getErrorRate() {
            return errorRate;
        }
        
        /**
         * Gets the timestamp when these statistics were collected.
         *
         * @return The timestamp in milliseconds since epoch
         */
        public long getTimestamp() {
            return timestamp;
        }
    }
}