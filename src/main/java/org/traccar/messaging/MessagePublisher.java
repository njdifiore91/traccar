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
package org.traccar.messaging;

import java.util.concurrent.CompletableFuture;

/**
 * Interface for publishing messages to a message broker.
 * This abstraction allows for different message broker implementations
 * (e.g., Kafka, RabbitMQ) to be used interchangeably.
 */
public interface MessagePublisher {

    /**
     * Publishes a message to the specified topic.
     *
     * @param topic The topic to publish the message to
     * @param message The message to publish
     * @return A CompletableFuture that completes when the message is published
     */
    CompletableFuture<Void> publish(String topic, Object message);

}