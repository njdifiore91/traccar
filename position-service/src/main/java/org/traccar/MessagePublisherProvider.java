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

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

/**
 * Provider for MessagePublisher implementations.
 * Creates the appropriate message publisher based on configuration.
 */
public class MessagePublisherProvider implements Provider<MessagePublisher> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessagePublisherProvider.class);

    private final Config config;
    private final Tracer tracer;

    @Inject
    public MessagePublisherProvider(Config config, Tracer tracer) {
        this.config = config;
        this.tracer = tracer;
    }

    @Override
    public MessagePublisher get() {
        String brokerType = config.getString("message.broker.type", "kafka").toLowerCase();
        
        try {
            switch (brokerType) {
                case "kafka":
                    LOGGER.info("Using Kafka message publisher");
                    return new KafkaMessagePublisher(config, tracer);
                case "rabbitmq":
                    LOGGER.info("Using RabbitMQ message publisher");
                    return new RabbitMQMessagePublisher(config, tracer);
                default:
                    LOGGER.warn("Unknown message broker type: {}, defaulting to Kafka", brokerType);
                    return new KafkaMessagePublisher(config, tracer);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to initialize message publisher for broker type: {}", brokerType, e);
            throw new RuntimeException("Failed to initialize message publisher", e);
        }
    }
}