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

/**
 * Constants for message broker topics, consumer groups, and other messaging-related configuration
 * values used throughout the Position Processing Service.
 */
public final class MessageConstants {

    private MessageConstants() {
        // Utility class, prevent instantiation
    }

    /**
     * Topic names for position messages
     */
    public static final class Topics {
        // Raw position messages from Protocol Service
        public static final String RAW_POSITIONS = "raw-positions";
        
        // Enriched position messages for Event Service
        public static final String ENRICHED_POSITIONS = "enriched-positions";
        
        // Dead letter queue topics
        public static final String DLQ_RAW_POSITIONS = "dlq.raw-positions";
        public static final String DLQ_ENRICHED_POSITIONS = "dlq.enriched-positions";
        
        // Retry topics
        public static final String RETRY_RAW_POSITIONS = "retry.raw-positions";
        public static final String RETRY_ENRICHED_POSITIONS = "retry.enriched-positions";
        
        private Topics() {
            // Prevent instantiation
        }
    }

    /**
     * Consumer group names for position processing
     */
    public static final class ConsumerGroups {
        // Consumer group for raw position messages
        public static final String POSITION_PROCESSOR = "position-processor";
        
        // Consumer group for retry processing
        public static final String POSITION_RETRY_PROCESSOR = "position-retry-processor";
        
        // Consumer group for dead letter queue monitoring
        public static final String POSITION_DLQ_MONITOR = "position-dlq-monitor";
        
        private ConsumerGroups() {
            // Prevent instantiation
        }
    }

    /**
     * Headers used in position messages
     */
    public static final class Headers {
        // Correlation ID for distributed tracing
        public static final String CORRELATION_ID = "X-Correlation-ID";
        
        // Original timestamp when the message was created
        public static final String TIMESTAMP = "X-Timestamp";
        
        // Device ID for message routing
        public static final String DEVICE_ID = "X-Device-ID";
        
        // Protocol name
        public static final String PROTOCOL = "X-Protocol";
        
        // Retry count for failed messages
        public static final String RETRY_COUNT = "X-Retry-Count";
        
        // Error message for failed processing
        public static final String ERROR_MESSAGE = "X-Error-Message";
        
        // Error type for failed processing
        public static final String ERROR_TYPE = "X-Error-Type";
        
        private Headers() {
            // Prevent instantiation
        }
    }

    /**
     * Retry configuration constants
     */
    public static final class RetryConfig {
        // Maximum number of retry attempts
        public static final int MAX_RETRY_ATTEMPTS = 3;
        
        // Initial retry delay in milliseconds
        public static final long INITIAL_RETRY_DELAY_MS = 1000;
        
        // Maximum retry delay in milliseconds
        public static final long MAX_RETRY_DELAY_MS = 60000;
        
        // Multiplier for exponential backoff
        public static final double BACKOFF_MULTIPLIER = 2.0;
        
        // Random factor for jitter in retry delay
        public static final double JITTER_FACTOR = 0.5;
        
        private RetryConfig() {
            // Prevent instantiation
        }
    }

    /**
     * Error handling constants
     */
    public static final class ErrorHandling {
        // Error types
        public static final String CONNECTION_ERROR = "CONNECTION_ERROR";
        public static final String SERIALIZATION_ERROR = "SERIALIZATION_ERROR";
        public static final String VALIDATION_ERROR = "VALIDATION_ERROR";
        public static final String PROCESSING_ERROR = "PROCESSING_ERROR";
        public static final String UNKNOWN_ERROR = "UNKNOWN_ERROR";
        
        // Retryable error types
        public static final String[] RETRYABLE_ERRORS = {
            CONNECTION_ERROR,
            SERIALIZATION_ERROR,
            PROCESSING_ERROR
        };
        
        private ErrorHandling() {
            // Prevent instantiation
        }
    }

    /**
     * Kafka-specific configuration constants
     */
    public static final class KafkaConfig {
        // Kafka consumer configuration
        public static final String AUTO_OFFSET_RESET = "earliest";
        public static final boolean ENABLE_AUTO_COMMIT = false;
        public static final int MAX_POLL_RECORDS = 500;
        public static final int MAX_POLL_INTERVAL_MS = 300000; // 5 minutes
        
        // Kafka producer configuration
        public static final String ACKS_CONFIG = "all";
        public static final int RETRIES = 3;
        public static final int BATCH_SIZE = 16384;
        public static final int LINGER_MS = 1;
        public static final int BUFFER_MEMORY = 33554432; // 32MB
        
        private KafkaConfig() {
            // Prevent instantiation
        }
    }

    /**
     * RabbitMQ-specific configuration constants
     */
    public static final class RabbitMQConfig {
        // Exchange names
        public static final String POSITION_EXCHANGE = "position-exchange";
        public static final String DLX_EXCHANGE = "dlx-exchange";
        public static final String RETRY_EXCHANGE = "retry-exchange";
        
        // Queue names
        public static final String RAW_POSITIONS_QUEUE = "raw-positions-queue";
        public static final String ENRICHED_POSITIONS_QUEUE = "enriched-positions-queue";
        public static final String DLQ_RAW_POSITIONS_QUEUE = "dlq-raw-positions-queue";
        public static final String DLQ_ENRICHED_POSITIONS_QUEUE = "dlq-enriched-positions-queue";
        public static final String RETRY_RAW_POSITIONS_QUEUE = "retry-raw-positions-queue";
        public static final String RETRY_ENRICHED_POSITIONS_QUEUE = "retry-enriched-positions-queue";
        
        // Routing keys
        public static final String RAW_POSITIONS_ROUTING_KEY = "raw-positions";
        public static final String ENRICHED_POSITIONS_ROUTING_KEY = "enriched-positions";
        public static final String DLQ_RAW_POSITIONS_ROUTING_KEY = "dlq.raw-positions";
        public static final String DLQ_ENRICHED_POSITIONS_ROUTING_KEY = "dlq.enriched-positions";
        public static final String RETRY_RAW_POSITIONS_ROUTING_KEY = "retry.raw-positions";
        public static final String RETRY_ENRICHED_POSITIONS_ROUTING_KEY = "retry.enriched-positions";
        
        // Consumer configuration
        public static final int PREFETCH_COUNT = 10;
        public static final boolean AUTO_ACK = false;
        
        private RabbitMQConfig() {
            // Prevent instantiation
        }
    }
}