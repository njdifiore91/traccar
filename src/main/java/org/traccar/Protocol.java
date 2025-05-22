/*
 * Copyright 2018 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.netty.channel.Channel;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import org.traccar.model.Command;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Protocol interface defines methods for protocol implementations.
 * Each protocol implementation must implement this interface.
 */
public interface Protocol {

    /**
     * Returns protocol name.
     *
     * @return protocol name
     */
    String getName();

    /**
     * Returns list of tracker connectors for this protocol.
     *
     * @return list of connectors
     */
    Collection<TrackerConnector> getConnectorList();

    /**
     * Returns list of supported data commands.
     *
     * @return list of supported commands
     */
    Collection<String> getSupportedDataCommands();

    /**
     * Sends data command to the device.
     *
     * @param channel channel to send command to
     * @param remoteAddress device address
     * @param command command to send
     */
    void sendDataCommand(Channel channel, SocketAddress remoteAddress, Command command);

    /**
     * Sends data command to the device using service discovery.
     * This method supports both direct and service-based communication.
     *
     * @param deviceId device identifier
     * @param command command to send
     * @param tracingContext distributed tracing context
     * @return future that completes when command is sent
     */
    default CompletableFuture<Void> sendDataCommand(long deviceId, Command command, Context tracingContext) {
        throw new UnsupportedOperationException("Service-based command dispatch not supported by this protocol");
    }

    /**
     * Sends data command asynchronously via message broker.
     *
     * @param deviceId device identifier
     * @param command command to send
     * @param tracingContext distributed tracing context
     * @return future that completes when command is dispatched to broker
     */
    default CompletableFuture<Void> sendDataCommandAsync(long deviceId, Command command, Context tracingContext) {
        throw new UnsupportedOperationException("Asynchronous command dispatch not supported by this protocol");
    }

    /**
     * Returns list of supported text commands.
     *
     * @return list of supported commands
     */
    Collection<String> getSupportedTextCommands();

    /**
     * Sends text command to the device.
     *
     * @param destAddress destination address
     * @param command command to send
     * @throws Exception if any error occurs
     */
    void sendTextCommand(String destAddress, Command command) throws Exception;

    /**
     * Returns list of supported push commands.
     *
     * @return list of supported commands
     */
    Collection<String> getSupportedPushCommands();

    /**
     * Registers protocol with service discovery.
     *
     * @param serviceId unique service identifier
     * @param metadata service metadata
     * @return true if registration successful
     */
    default boolean registerWithServiceDiscovery(String serviceId, java.util.Map<String, String> metadata) {
        return false;
    }

    /**
     * Collects protocol-specific metrics.
     *
     * @param deviceId device identifier
     * @param operationType operation type (e.g., "decode", "encode")
     * @param startTime operation start time in nanoseconds
     * @param success whether operation was successful
     */
    default void collectMetrics(long deviceId, String operationType, long startTime, boolean success) {
        // Default implementation does nothing
    }

    /**
     * Creates a circuit breaker for the specified operation.
     *
     * @param operationName name of the operation
     * @return circuit breaker instance
     */
    default Object createCircuitBreaker(String operationName) {
        return null;
    }
}