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
package org.traccar.broadcast;

import org.traccar.LifecycleObject;

/**
 * Broadcast service interface for distributing messages across multiple service instances.
 * Supports both direct communication and message broker integration for microservices architecture.
 */
public interface BroadcastService extends LifecycleObject, BroadcastInterface {
    
    /**
     * Checks if this is the only instance of the service running.
     * 
     * @return true if this is a single instance, false if multiple instances are detected
     */
    boolean singleInstance();
    
    /**
     * Registers a listener for broadcast events.
     * 
     * @param listener the broadcast interface implementation to register
     */
    void registerListener(BroadcastInterface listener);
    
    /**
     * Publishes a message to the specified topic on the message broker.
     * 
     * @param topic the topic to publish to
     * @param message the message to publish
     * @param <T> the type of the message
     * @return true if the message was successfully published, false otherwise
     */
    <T> boolean publishToBroker(String topic, T message);
    
    /**
     * Subscribes to a topic on the message broker.
     * 
     * @param topic the topic to subscribe to
     * @param messageClass the class of the message type
     * @param <T> the type of the message
     * @return true if the subscription was successful, false otherwise
     */
    <T> boolean subscribeFromBroker(String topic, Class<T> messageClass);
    
    /**
     * Propagates the current distributed tracing context with the broadcast message.
     * This ensures that trace context is maintained across service boundaries.
     * 
     * @param message the message to attach tracing context to
     * @param <T> the type of the message
     * @return the message with tracing context attached
     */
    <T> T withTracingContext(T message);
    
    /**
     * Registers the service with the service discovery system.
     * 
     * @param serviceName the name of the service
     * @param serviceId a unique identifier for this service instance
     * @param metadata additional metadata for service discovery
     * @return true if registration was successful, false otherwise
     */
    boolean registerWithServiceDiscovery(String serviceName, String serviceId, java.util.Map<String, String> metadata);
    
    /**
     * Discovers service instances by name from the service discovery system.
     * 
     * @param serviceName the name of the service to discover
     * @return a list of service instance information
     */
    java.util.List<ServiceInstance> discoverServiceInstances(String serviceName);
    
    /**
     * Records metrics about broadcast operations.
     * 
     * @param operation the operation being performed (e.g., "send", "receive")
     * @param topic the topic or destination
     * @param status the status of the operation (e.g., "success", "failure")
     * @param durationMs the duration of the operation in milliseconds
     */
    void recordMetrics(String operation, String topic, String status, long durationMs);
    
    /**
     * Represents a discovered service instance.
     */
    interface ServiceInstance {
        /**
         * Gets the service instance ID.
         * 
         * @return the service instance ID
         */
        String getId();
        
        /**
         * Gets the service instance host.
         * 
         * @return the service instance host
         */
        String getHost();
        
        /**
         * Gets the service instance port.
         * 
         * @return the service instance port
         */
        int getPort();
        
        /**
         * Gets the service instance metadata.
         * 
         * @return the service instance metadata
         */
        java.util.Map<String, String> getMetadata();
        
        /**
         * Checks if the service instance is healthy.
         * 
         * @return true if the service instance is healthy, false otherwise
         */
        boolean isHealthy();
    }
}