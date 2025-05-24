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
 * Configuration keys used in the Event Processing Service.
 */
public final class Keys {

    /**
     * Private constructor to prevent instantiation.
     */
    private Keys() {
    }

    /**
     * Format to use for message serialization/deserialization (PROTOBUF or JSON).
     */
    public static final String EVENT_MESSAGE_FORMAT = "event.message.format";

    /**
     * Flag to ignore duplicate alerts from the same device.
     */
    public static final String EVENT_IGNORE_DUPLICATE_ALERTS = "event.ignoreDuplicateAlerts";

    /**
     * Topic name for consuming enriched positions.
     */
    public static final String EVENT_POSITION_TOPIC = "event.position.topic";

    /**
     * Topic name for publishing detected events.
     */
    public static final String EVENT_OUTPUT_TOPIC = "event.output.topic";

    /**
     * Consumer group ID for the Event Processing Service.
     */
    public static final String EVENT_CONSUMER_GROUP = "event.consumer.group";

    /**
     * Maximum number of retry attempts for failed message processing.
     */
    public static final String EVENT_MAX_RETRY_ATTEMPTS = "event.max.retry.attempts";

    /**
     * Backoff multiplier for retry attempts.
     */
    public static final String EVENT_RETRY_BACKOFF_MULTIPLIER = "event.retry.backoff.multiplier";

    /**
     * Initial backoff interval in milliseconds for retry attempts.
     */
    public static final String EVENT_RETRY_INITIAL_INTERVAL = "event.retry.initial.interval";

    /**
     * Maximum backoff interval in milliseconds for retry attempts.
     */
    public static final String EVENT_RETRY_MAX_INTERVAL = "event.retry.max.interval";

    /**
     * Dead letter queue topic name for unprocessable messages.
     */
    public static final String EVENT_DEAD_LETTER_QUEUE = "event.dead.letter.queue";

    /**
     * Flag to enable/disable distributed tracing.
     */
    public static final String EVENT_TRACING_ENABLED = "event.tracing.enabled";

    /**
     * Sampling rate for distributed tracing (0.0 to 1.0).
     */
    public static final String EVENT_TRACING_SAMPLING_RATE = "event.tracing.sampling.rate";

    /**
     * Jaeger endpoint for distributed tracing.
     */
    public static final String EVENT_TRACING_JAEGER_ENDPOINT = "event.tracing.jaeger.endpoint";
}