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
package org.traccar;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for publishing messages to a message broker.
 * Implementations can support different message brokers like Kafka or RabbitMQ.
 */
public interface MessagePublisher {

    /**
     * Publishes a message to the specified topic with the given key.
     *
     * @param topic   The topic to publish to
     * @param key     The message key (used for partitioning)
     * @param payload The message payload
     * @return CompletableFuture that completes when the message is published
     */
    CompletableFuture<Void> publish(String topic, String key, String payload);

    /**
     * Publishes a message to the specified topic with the given key and headers.
     *
     * @param topic   The topic to publish to
     * @param key     The message key (used for partitioning)
     * @param payload The message payload
     * @param headers Additional message headers
     * @return CompletableFuture that completes when the message is published
     */
    CompletableFuture<Void> publish(String topic, String key, String payload, Map<String, String> headers);
}