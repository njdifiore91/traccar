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

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.kafka.KafkaMessageBrokerClient;
import org.traccar.messaging.rabbitmq.RabbitMQMessageBrokerClient;

/**
 * Factory for creating message broker clients based on configuration.
 */
@Singleton
public class MessageBrokerClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerClientFactory.class);

    private final Config config;

    @Inject
    public MessageBrokerClientFactory(Config config) {
        this.config = config;
    }

    /**
     * Create a message broker client based on configuration.
     *
     * @return A message broker client implementation
     */
    public MessageBrokerClient create() {
        String brokerType = config.getString(Keys.PROCESSING_REMOTE_BROKER_TYPE.getKey(), "kafka");
        
        LOGGER.info("Creating message broker client of type: {}", brokerType);
        
        switch (brokerType.toLowerCase()) {
            case "kafka":
                return new KafkaMessageBrokerClient(config);
            case "rabbitmq":
                return new RabbitMQMessageBrokerClient(config);
            default:
                throw new IllegalArgumentException("Unsupported message broker type: " + brokerType);
        }
    }
}