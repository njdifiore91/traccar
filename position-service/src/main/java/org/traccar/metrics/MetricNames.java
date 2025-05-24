/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.metrics;

/**
 * Centralized definition of metric names used throughout the position service.
 * This ensures consistency in metric naming across the service.
 */
public final class MetricNames {

    private MetricNames() {
        // Utility class
    }

    // Position processing metrics
    public static final String POSITION_RECEIVED = "position.received";
    public static final String POSITION_PROCESSED = "position.processed";
    public static final String POSITION_PROCESSING_TIME = "position.processing.time";
    public static final String POSITION_QUEUE_SIZE = "position.queue.size";
    
    // Geocoding metrics
    public static final String GEOCODER_REQUESTS = "geocoder.requests";
    public static final String GEOCODER_REQUEST_TIME = "geocoder.request.time";
    public static final String GEOCODER_ERRORS = "geocoder.errors";
    public static final String GEOCODER_FALLBACKS = "geocoder.fallbacks";
    public static final String GEOCODER_CACHE_HITS = "geocoder.cache.hits";
    public static final String GEOCODER_CACHE_MISSES = "geocoder.cache.misses";
    
    // Database metrics
    public static final String DB_QUERY_TIME = "db.query.time";
    public static final String DB_CONNECTIONS_ACTIVE = "db.connections.active";
    public static final String DB_CONNECTIONS_IDLE = "db.connections.idle";
    
    // Message broker metrics
    public static final String BROKER_PUBLISH_TIME = "broker.publish.time";
    public static final String BROKER_CONSUME_TIME = "broker.consume.time";
    public static final String BROKER_PUBLISH_ERRORS = "broker.publish.errors";
    public static final String BROKER_CONSUME_ERRORS = "broker.consume.errors";
    
    // Circuit breaker metrics
    public static final String CIRCUIT_BREAKER_STATE = "resilience.circuitbreaker.state";
    public static final String CIRCUIT_BREAKER_FAILURES = "resilience.circuitbreaker.failures";
    public static final String CIRCUIT_BREAKER_SUCCESSES = "resilience.circuitbreaker.successes";
    
    // Retry metrics
    public static final String RETRY_CALLS = "resilience.retry.calls";
    public static final String RETRY_SUCCESSES = "resilience.retry.successes";
    public static final String RETRY_FAILURES = "resilience.retry.failures";
    
    // Cache metrics
    public static final String CACHE_HITS = "cache.hits";
    public static final String CACHE_MISSES = "cache.misses";
    public static final String CACHE_SIZE = "cache.size";
    
    // Service health metrics
    public static final String HEALTH_STATUS = "health.status";
    public static final String HEALTH_CHECK_TIME = "health.check.time";
}