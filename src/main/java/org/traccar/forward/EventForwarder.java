/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
 * Interface for forwarding events to external systems or message brokers.
 * This interface supports both synchronous and asynchronous forwarding operations,
 * with built-in support for distributed tracing, metrics collection, and circuit breaking.
 */
public interface EventForwarder {
    
    /**
     * Forward an event asynchronously with distributed tracing context.
     * 
     * @param eventData The event data to forward
     * @param tracingContext The OpenTelemetry tracing context for distributed tracing
     * @return A CompletableFuture that completes when the forwarding operation is done
     */
    CompletableFuture<Void> forwardAsync(EventData eventData, Context tracingContext);
    
    /**
     * Forward an event synchronously with distributed tracing context and callback handler.
     * 
     * @param eventData The event data to forward
     * @param resultHandler The callback handler for success/failure notification
     * @param tracingContext The OpenTelemetry tracing context for distributed tracing
     */
    void forward(EventData eventData, ResultHandler resultHandler, Context tracingContext);
    
    /**
     * Forward an event synchronously with callback handler (backward compatibility).
     * 
     * @param eventData The event data to forward
     * @param resultHandler The callback handler for success/failure notification
     */
    default void forward(EventData eventData, ResultHandler resultHandler) {
        forward(eventData, resultHandler, Context.current());
    }
    
    /**
     * Get the service name for service discovery registration.
     * 
     * @return The service name used for discovery
     */
    String getServiceName();
    
    /**
     * Check if the forwarder is healthy and ready to process events.
     * Used by circuit breakers to determine if the forwarder should be used.
     * 
     * @return true if the forwarder is healthy, false otherwise
     */
    boolean isHealthy();
}