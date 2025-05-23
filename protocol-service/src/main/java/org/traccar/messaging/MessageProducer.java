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
package org.traccar.messaging;

import org.traccar.model.Position;

/**
 * Interface for message broker producers.
 * This abstraction allows for different message broker implementations (Kafka, RabbitMQ, etc.)
 */
public interface MessageProducer {

    /**
     * Send a position to the message broker.
     *
     * @param position Position object to send
     * @throws Exception If there is an error sending the message
     */
    void sendPosition(Position position) throws Exception;

    /**
     * Send a device connection status update to the message broker.
     *
     * @param deviceId Device identifier
     * @param connected Connection status
     * @throws Exception If there is an error sending the message
     */
    void sendDeviceConnectionStatus(long deviceId, boolean connected) throws Exception;

    /**
     * Close the producer and release resources.
     */
    void close();
}