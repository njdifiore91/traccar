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
package org.traccar.config;

/**
 * Configuration keys used in the application.
 */
public final class Keys {

    private Keys() {
    }

    /**
     * Connection timeout value in seconds. Because sometimes there is no way to detect lost TCP connection old
     * connections stay in open state. On most systems there is a limit on number of open connection, so this leads to
     * problems with establishing new connections when number of devices is high or devices data connections are
     * unstable.
     */
    public static final ConfigKey SERVER_TIMEOUT = new ConfigKey(
            "server.timeout", Integer.class);

    /**
     * Periodic logging of performance information. The value is the logging interval in seconds.
     */
    public static final ConfigKey STATUS_TIMEOUT = new ConfigKey(
            "status.timeout", Integer.class);

    /**
     * Enable events subsystem. Flag to enable all events that are configured.
     */
    public static final ConfigKey EVENT_ENABLE = new ConfigKey(
            "event.enable", Boolean.class);

    /**
     * If true, the event is generated once at the beginning of overspeeding period.
     */
    public static final ConfigKey EVENT_OVERSPEED_NOT_REPEAT = new ConfigKey(
            "event.overspeed.notRepeat", Boolean.class);

    /**
     * Minimal over speed duration to trigger the event. Value in seconds.
     */
    public static final ConfigKey EVENT_OVERSPEED_MINIMAL_DURATION = new ConfigKey(
            "event.overspeed.minimalDuration", Long.class);

    /**
     * Relevant only for geofence speed limits. Use lowest speed limits from all geofences.
     */
    public static final ConfigKey EVENT_OVERSPEED_PREFER_LOWEST = new ConfigKey(
            "event.overspeed.preferLowest", Boolean.class);

    /**
     * Do not generate alert event if same alert was present in last known location.
     */
    public static final ConfigKey EVENT_IGNORE_DUPLICATE_ALERTS = new ConfigKey(
            "event.ignoreDuplicateAlerts", Boolean.class);

    /**
     * If set to true, invalid positions will be filtered out.
     */
    public static final ConfigKey FILTER_INVALID = new ConfigKey(
            "filter.invalid", Boolean.class);

    /**
     * If set to true, zero coordinates will be filtered out.
     */
    public static final ConfigKey FILTER_ZERO = new ConfigKey(
            "filter.zero", Boolean.class);

    /**
     * If set to true, invalid positions will be filtered out.
     */
    public static final ConfigKey FILTER_DUPLICATE = new ConfigKey(
            "filter.duplicate", Boolean.class);

    /**
     * If set to true, positions with accuracy value less than set in filter.accuracy will be filtered out.
     * Default value is false.
     */
    public static final ConfigKey FILTER_APPROXIMATE = new ConfigKey(
            "filter.approximate", Boolean.class);

    /**
     * The minimum accuracy value to use for position filtering.
     * Default value is 100.
     */
    public static final ConfigKey FILTER_ACCURACY = new ConfigKey(
            "filter.accuracy", Integer.class);

    /**
     * The delay in seconds to wait for position fix. Positions with accuracy value less than set in filter.accuracy
     * received within filter.fixTimeout seconds will be discarded.
     * Default value is 300.
     */
    public static final ConfigKey FILTER_FIX_TIMEOUT = new ConfigKey(
            "filter.fixTimeout", Integer.class);

    /**
     * If set to true, remote position filtering will be enabled.
     */
    public static final ConfigKey PROCESSING_REMOTE_ENABLED = new ConfigKey(
            "processing.remote.enabled", Boolean.class);

    /**
     * The type of message broker to use for remote position processing.
     * Supported values: kafka, rabbitmq
     */
    public static final ConfigKey PROCESSING_REMOTE_BROKER_TYPE = new ConfigKey(
            "processing.remote.brokerType", String.class);

    /**
     * The topic to publish positions to for remote processing.
     */
    public static final ConfigKey PROCESSING_REMOTE_POSITIONS_TOPIC = new ConfigKey(
            "processing.remote.positionsTopic", String.class);

    /**
     * Kafka bootstrap servers for remote position processing.
     */
    public static final ConfigKey PROCESSING_REMOTE_KAFKA_BOOTSTRAP_SERVERS = new ConfigKey(
            "processing.remote.kafka.bootstrapServers", String.class);

    /**
     * RabbitMQ host for remote position processing.
     */
    public static final ConfigKey PROCESSING_REMOTE_RABBITMQ_HOST = new ConfigKey(
            "processing.remote.rabbitmq.host", String.class);

    /**
     * RabbitMQ port for remote position processing.
     */
    public static final ConfigKey PROCESSING_REMOTE_RABBITMQ_PORT = new ConfigKey(
            "processing.remote.rabbitmq.port", Integer.class);

    /**
     * RabbitMQ username for remote position processing.
     */
    public static final ConfigKey PROCESSING_REMOTE_RABBITMQ_USERNAME = new ConfigKey(
            "processing.remote.rabbitmq.username", String.class);

    /**
     * RabbitMQ password for remote position processing.
     */
    public static final ConfigKey PROCESSING_REMOTE_RABBITMQ_PASSWORD = new ConfigKey(
            "processing.remote.rabbitmq.password", String.class);

    /**
     * RabbitMQ virtual host for remote position processing.
     */
    public static final ConfigKey PROCESSING_REMOTE_RABBITMQ_VIRTUAL_HOST = new ConfigKey(
            "processing.remote.rabbitmq.virtualHost", String.class);

    /**
     * Enable OpenTelemetry tracing.
     */
    public static final ConfigKey TELEMETRY_TRACING_ENABLED = new ConfigKey(
            "telemetry.tracing.enabled", Boolean.class);

    /**
     * OpenTelemetry tracing endpoint.
     */
    public static final ConfigKey TELEMETRY_TRACING_ENDPOINT = new ConfigKey(
            "telemetry.tracing.endpoint", String.class);

    /**
     * Enable OpenTelemetry metrics.
     */
    public static final ConfigKey TELEMETRY_METRICS_ENABLED = new ConfigKey(
            "telemetry.metrics.enabled", Boolean.class);

    /**
     * OpenTelemetry metrics endpoint.
     */
    public static final ConfigKey TELEMETRY_METRICS_ENDPOINT = new ConfigKey(
            "telemetry.metrics.endpoint", String.class);

    /**
     * OpenTelemetry metrics export interval in milliseconds.
     */
    public static final ConfigKey TELEMETRY_METRICS_EXPORT_INTERVAL = new ConfigKey(
            "telemetry.metrics.exportInterval", Long.class);

    /**
     * OpenTelemetry service name.
     */
    public static final ConfigKey TELEMETRY_SERVICE_NAME = new ConfigKey(
            "telemetry.serviceName", String.class);

    /**
     * Geocoder enable flag.
     */
    public static final ConfigKey GEOCODER_ENABLE = new ConfigKey(
            "geocoder.enable", Boolean.class);

    /**
     * Geocoder format string.
     */
    public static final ConfigKey GEOCODER_FORMAT = new ConfigKey(
            "geocoder.format", String.class);

    /**
     * Geocoder cache size.
     */
    public static final ConfigKey GEOCODER_CACHE_SIZE = new ConfigKey(
            "geocoder.cacheSize", Integer.class);

    /**
     * Geocoder cache expire after write in milliseconds.
     */
    public static final ConfigKey GEOCODER_CACHE_EXPIRE = new ConfigKey(
            "geocoder.cacheExpire", Long.class);

    /**
     * Geocoder type.
     */
    public static final ConfigKey GEOCODER_TYPE = new ConfigKey(
            "geocoder.type", String.class);

    /**
     * Geocoder server URL.
     */
    public static final ConfigKey GEOCODER_URL = new ConfigKey(
            "geocoder.url", String.class);

    /**
     * Geocoder API key.
     */
    public static final ConfigKey GEOCODER_KEY = new ConfigKey(
            "geocoder.key", String.class);

    /**
     * Geocoder language.
     */
    public static final ConfigKey GEOCODER_LANGUAGE = new ConfigKey(
            "geocoder.language", String.class);

    /**
     * Geocoder request timeout in milliseconds.
     */
    public static final ConfigKey GEOCODER_TIMEOUT = new ConfigKey(
            "geocoder.timeout", Integer.class);

    /**
     * Forward enable flag.
     */
    public static final ConfigKey FORWARD_ENABLE = new ConfigKey(
            "forward.enable", Boolean.class);

    /**
     * Forward URL.
     */
    public static final ConfigKey FORWARD_URL = new ConfigKey(
            "forward.url", String.class);

    /**
     * Forward retry enable flag.
     */
    public static final ConfigKey FORWARD_RETRY_ENABLE = new ConfigKey(
            "forward.retry.enable", Boolean.class);

    /**
     * Forward retry delay in milliseconds.
     */
    public static final ConfigKey FORWARD_RETRY_DELAY = new ConfigKey(
            "forward.retry.delay", Integer.class);

    /**
     * Forward retry count.
     */
    public static final ConfigKey FORWARD_RETRY_COUNT = new ConfigKey(
            "forward.retry.count", Integer.class);

    /**
     * Forward retry limit.
     */
    public static final ConfigKey FORWARD_RETRY_LIMIT = new ConfigKey(
            "forward.retry.limit", Long.class);

    /**
     * Forward timeout in milliseconds.
     */
    public static final ConfigKey FORWARD_TIMEOUT = new ConfigKey(
            "forward.timeout", Integer.class);

    /**
     * Forward header name.
     */
    public static final ConfigKey FORWARD_HEADER_NAME = new ConfigKey(
            "forward.header.name", String.class);

    /**
     * Forward header value.
     */
    public static final ConfigKey FORWARD_HEADER_VALUE = new ConfigKey(
            "forward.header.value", String.class);

    /**
     * Forward payload template.
     */
    public static final ConfigKey FORWARD_TEMPLATE = new ConfigKey(
            "forward.template", String.class);

    /**
     * Forward type.
     */
    public static final ConfigKey FORWARD_TYPE = new ConfigKey(
            "forward.type", String.class);

    /**
     * Forward batch size.
     */
    public static final ConfigKey FORWARD_BATCH_SIZE = new ConfigKey(
            "forward.batchSize", Integer.class);

    /**
     * Forward batch interval in milliseconds.
     */
    public static final ConfigKey FORWARD_BATCH_INTERVAL = new ConfigKey(
            "forward.batchInterval", Integer.class);
}