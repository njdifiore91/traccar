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
package org.traccar;

import io.opentelemetry.context.propagation.TextMapPropagator;

import java.util.Map;

/**
 * Interface for publishing messages to a message broker.
 * 
 * This interface abstracts the details of the message broker implementation,
 * allowing the application to use different message brokers (e.g., Kafka, RabbitMQ)
 * without changing the code that publishes messages.
 */
public interface MessageBrokerPublisher {

    /**
     * Publish a message to the specified topic.
     *
     * @param topic Topic to publish the message to
     * @param payload Message payload as a JSON string
     * @param headers Message headers, including correlation ID and tracing information
     * @throws Exception If there is an error publishing the message
     */
    void publish(String topic, String payload, Map<String, String> headers) throws Exception;

    /**
     * Get the OpenTelemetry context propagator for distributed tracing.
     *
     * @return TextMapPropagator for injecting tracing context into message headers
     */
    TextMapPropagator getPropagator();
}