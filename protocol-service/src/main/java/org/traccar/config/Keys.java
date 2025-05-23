/*
 * Copyright 2019 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.config;

/**
 * Configuration keys for the Protocol Service.
 * This class has been updated to include keys for microservices architecture.
 */
public final class Keys {

    /**
     * Connection timeout in seconds
     */
    public static final ConfigKey PROTOCOL_TIMEOUT = new ConfigKey(
            "protocol.timeout", Integer.class);

    /**
     * Service name for registration and tracing
     */
    public static final ConfigKey SERVICE_NAME = new ConfigKey(
            "service.name", String.class);

    /**
     * Service discovery URL
     */
    public static final ConfigKey SERVICE_DISCOVERY_URL = new ConfigKey(
            "service.discovery.url", String.class);

    /**
     * Kafka bootstrap servers
     */
    public static final ConfigKey KAFKA_BOOTSTRAP_SERVERS = new ConfigKey(
            "kafka.bootstrap.servers", String.class);

    /**
     * Kafka client ID
     */
    public static final ConfigKey KAFKA_CLIENT_ID = new ConfigKey(
            "kafka.client.id", String.class);

    /**
     * Kafka positions topic
     */
    public static final ConfigKey KAFKA_POSITIONS_TOPIC = new ConfigKey(
            "kafka.positions.topic", String.class);

    /**
     * Kafka device status topic
     */
    public static final ConfigKey KAFKA_DEVICE_STATUS_TOPIC = new ConfigKey(
            "kafka.device.status.topic", String.class);

    /**
     * OpenTelemetry collector endpoint
     */
    public static final ConfigKey OTEL_COLLECTOR_ENDPOINT = new ConfigKey(
            "otel.collector.endpoint", String.class);

    /**
     * OpenTelemetry service namespace
     */
    public static final ConfigKey OTEL_SERVICE_NAMESPACE = new ConfigKey(
            "otel.service.namespace", String.class);

    private Keys() {
        // Utility class
    }

}