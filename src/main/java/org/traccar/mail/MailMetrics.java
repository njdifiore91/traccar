/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.mail;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collects and exposes metrics related to email operations in the Traccar system.
 * <p>
 * This component provides standardized Prometheus metrics for monitoring email sending
 * operations, including success/failure rates, latency, and volume metrics.
 * <p>
 * The metrics follow the OpenMetrics format and support correlation with distributed
 * tracing through shared identifiers.
 */
@Singleton
public class MailMetrics {

    private static final Logger LOGGER = LoggerFactory.getLogger(MailMetrics.class);
    
    private static final String PREFIX = "mail";
    
    // Metric names following Prometheus naming conventions
    private static final String EMAILS_SENT_TOTAL = PREFIX + "_sent_total";
    private static final String EMAILS_FAILED_TOTAL = PREFIX + "_failed_total";
    private static final String EMAIL_PROCESSING_SECONDS = PREFIX + "_processing_seconds";
    private static final String EMAIL_QUEUE_SIZE = PREFIX + "_queue_size";
    private static final String EMAIL_RETRIES_TOTAL = PREFIX + "_retries_total";
    private static final String EMAIL_DLQ_PUBLISHED_TOTAL = PREFIX + "_dlq_published_total";
    private static final String EMAIL_CIRCUIT_BREAKER_STATE = PREFIX + "_circuit_breaker_state";
    
    // Common tags
    private static final String TAG_STATUS = "status";
    private static final String TAG_REASON = "reason";
    private static final String TAG_CHANNEL = "channel";
    private static final String TAG_SYSTEM = "system";
    
    // Status values
    private static final String STATUS_SUCCESS = "success";
    private static final String STATUS_FAILURE = "failure";
    
    // Channel values
    private static final String CHANNEL_SMTP = "smtp";
    
    // System values
    private static final String SYSTEM_TRUE = "true";
    private static final String SYSTEM_FALSE = "false";
    
    // Failure reason values
    private static final String REASON_AUTHENTICATION = "authentication";
    private static final String REASON_CONNECTION = "connection";
    private static final String REASON_TIMEOUT = "timeout";
    private static final String REASON_INVALID_ADDRESS = "invalid_address";
    private static final String REASON_CONFIGURATION = "configuration";
    private static final String REASON_UNKNOWN = "unknown";
    
    // Circuit breaker state values
    private static final String CIRCUIT_CLOSED = "closed";
    private static final String CIRCUIT_OPEN = "open";
    private static final String CIRCUIT_HALF_OPEN = "half_open";
    
    private final MeterRegistry meterRegistry;
    
    // Counters
    private final Counter emailsSentTotal;
    private final Counter emailsFailedTotal;
    private final Map<String, Counter> emailsFailedByReason;
    private final Counter emailRetriesTotal;
    private final Counter emailDlqPublishedTotal;
    
    // Timers
    private final Timer emailProcessingTimer;
    
    // Gauges
    private final AtomicInteger emailQueueSize;
    private final AtomicInteger circuitBreakerState;
    
    /**
     * Creates a new MailMetrics instance with the provided meter registry.
     *
     * @param meterRegistry the meter registry to register metrics with
     */
    @Inject
    public MailMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        LOGGER.info("Initializing mail metrics with registry: {}", meterRegistry.getClass().getSimpleName());
        
        // Initialize counters
        this.emailsSentTotal = Counter.builder(EMAILS_SENT_TOTAL)
                .description("Total number of emails sent")
                .register(meterRegistry);
        
        this.emailsFailedTotal = Counter.builder(EMAILS_FAILED_TOTAL)
                .description("Total number of failed email attempts")
                .register(meterRegistry);
        
        this.emailsFailedByReason = new ConcurrentHashMap<>();
        
        this.emailRetriesTotal = Counter.builder(EMAIL_RETRIES_TOTAL)
                .description("Total number of email retry attempts")
                .register(meterRegistry);
        
        this.emailDlqPublishedTotal = Counter.builder(EMAIL_DLQ_PUBLISHED_TOTAL)
                .description("Total number of emails published to dead letter queue")
                .register(meterRegistry);
        
        // Initialize timers
        this.emailProcessingTimer = Timer.builder(EMAIL_PROCESSING_SECONDS)
                .description("Time taken to process and send emails")
                .publishPercentiles(0.5, 0.95, 0.99) // Publish 50th, 95th, and 99th percentiles
                .publishPercentileHistogram()
                .register(meterRegistry);
        
        // Initialize gauges
        this.emailQueueSize = new AtomicInteger(0);
        Gauge.builder(EMAIL_QUEUE_SIZE, emailQueueSize, AtomicInteger::get)
                .description("Current number of emails in the processing queue")
                .register(meterRegistry);
        
        this.circuitBreakerState = new AtomicInteger(0); // 0=closed, 1=open, 2=half-open
        Gauge.builder(EMAIL_CIRCUIT_BREAKER_STATE, circuitBreakerState, AtomicInteger::get)
                .description("Current state of the email circuit breaker (0=closed, 1=open, 2=half-open)")
                .register(meterRegistry);
        
        LOGGER.info("Mail metrics initialized successfully");
    }
    
    /**
     * Records a successful email send operation.
     *
     * @param channel the channel used to send the email (e.g., "smtp")
     * @param system  whether it's a system-generated email
     * @return a Timer.Sample that should be stopped when the operation completes
     */
    public Timer.Sample recordEmailSendStart(String channel, boolean system) {
        emailQueueSize.incrementAndGet();
        return Timer.start(meterRegistry);
    }
    
    /**
     * Records the completion of a successful email send operation.
     *
     * @param sample  the Timer.Sample started when the operation began
     * @param channel the channel used to send the email (e.g., "smtp")
     * @param system  whether it's a system-generated email
     */
    public void recordEmailSendSuccess(Timer.Sample sample, String channel, boolean system) {
        Tags tags = Tags.of(
                Tag.of(TAG_STATUS, STATUS_SUCCESS),
                Tag.of(TAG_CHANNEL, channel),
                Tag.of(TAG_SYSTEM, system ? SYSTEM_TRUE : SYSTEM_FALSE));
        
        sample.stop(emailProcessingTimer.withTags(tags));
        emailsSentTotal.increment();
        emailQueueSize.decrementAndGet();
    }
    
    /**
     * Records a failed email send operation.
     *
     * @param sample  the Timer.Sample started when the operation began
     * @param channel the channel used to send the email (e.g., "smtp")
     * @param system  whether it's a system-generated email
     * @param reason  the reason for the failure
     */
    public void recordEmailSendFailure(Timer.Sample sample, String channel, boolean system, String reason) {
        Tags tags = Tags.of(
                Tag.of(TAG_STATUS, STATUS_FAILURE),
                Tag.of(TAG_CHANNEL, channel),
                Tag.of(TAG_SYSTEM, system ? SYSTEM_TRUE : SYSTEM_FALSE),
                Tag.of(TAG_REASON, reason));
        
        sample.stop(emailProcessingTimer.withTags(tags));
        emailsFailedTotal.increment();
        
        // Record failure by reason
        emailsFailedByReason.computeIfAbsent(reason, r -> Counter.builder(EMAILS_FAILED_TOTAL)
                .tag(TAG_REASON, r)
                .description("Total number of failed email attempts by reason")
                .register(meterRegistry))
                .increment();
        
        emailQueueSize.decrementAndGet();
    }
    
    /**
     * Records an email retry attempt.
     *
     * @param channel the channel used to send the email (e.g., "smtp")
     * @param system  whether it's a system-generated email
     * @param reason  the reason for the retry
     */
    public void recordEmailRetry(String channel, boolean system, String reason) {
        Tags tags = Tags.of(
                Tag.of(TAG_CHANNEL, channel),
                Tag.of(TAG_SYSTEM, system ? SYSTEM_TRUE : SYSTEM_FALSE),
                Tag.of(TAG_REASON, reason));
        
        Counter counter = Counter.builder(EMAIL_RETRIES_TOTAL)
                .tags(tags)
                .description("Total number of email retry attempts")
                .register(meterRegistry);
        
        counter.increment();
    }
    
    /**
     * Records an email being published to the dead letter queue.
     *
     * @param channel the channel used to send the email (e.g., "smtp")
     * @param system  whether it's a system-generated email
     * @param reason  the reason for publishing to DLQ
     */
    public void recordDlqPublished(String channel, boolean system, String reason) {
        Tags tags = Tags.of(
                Tag.of(TAG_CHANNEL, channel),
                Tag.of(TAG_SYSTEM, system ? SYSTEM_TRUE : SYSTEM_FALSE),
                Tag.of(TAG_REASON, reason));
        
        Counter counter = Counter.builder(EMAIL_DLQ_PUBLISHED_TOTAL)
                .tags(tags)
                .description("Total number of emails published to dead letter queue")
                .register(meterRegistry);
        
        counter.increment();
    }
    
    /**
     * Updates the circuit breaker state metric.
     *
     * @param state the current state of the circuit breaker ("closed", "open", or "half_open")
     */
    public void updateCircuitBreakerState(String state) {
        int stateValue;
        switch (state) {
            case CIRCUIT_CLOSED:
                stateValue = 0;
                break;
            case CIRCUIT_OPEN:
                stateValue = 1;
                break;
            case CIRCUIT_HALF_OPEN:
                stateValue = 2;
                break;
            default:
                LOGGER.warn("Unknown circuit breaker state: {}", state);
                return;
        }
        
        circuitBreakerState.set(stateValue);
    }
    
    /**
     * Gets the current email queue size.
     *
     * @return the current number of emails in the processing queue
     */
    public int getEmailQueueSize() {
        return emailQueueSize.get();
    }
    
    /**
     * Gets the current circuit breaker state.
     *
     * @return the current state of the circuit breaker (0=closed, 1=open, 2=half-open)
     */
    public int getCircuitBreakerState() {
        return circuitBreakerState.get();
    }
    
    /**
     * Resets all metrics to their initial values.
     * This is primarily used for testing purposes.
     */
    public void resetMetrics() {
        LOGGER.info("Resetting all mail metrics");
        emailQueueSize.set(0);
        circuitBreakerState.set(0);
        // Note: Counters cannot be reset in Micrometer, they can only be incremented
    }
    
    /**
     * Helper method to determine the failure reason from an exception.
     *
     * @param exception the exception that caused the failure
     * @return a standardized reason string
     */
    public static String getFailureReasonFromException(Exception exception) {
        if (exception == null) {
            return REASON_UNKNOWN;
        }
        
        String message = exception.getMessage();
        if (message == null) {
            return REASON_UNKNOWN;
        }
        
        message = message.toLowerCase();
        
        if (message.contains("authentication") || message.contains("auth") || 
                message.contains("login") || message.contains("credentials")) {
            return REASON_AUTHENTICATION;
        } else if (message.contains("connect") || message.contains("connection") || 
                message.contains("network") || message.contains("unreachable")) {
            return REASON_CONNECTION;
        } else if (message.contains("timeout") || message.contains("timed out")) {
            return REASON_TIMEOUT;
        } else if (message.contains("address") || message.contains("recipient") || 
                message.contains("email") || message.contains("mailbox")) {
            return REASON_INVALID_ADDRESS;
        } else if (message.contains("config") || message.contains("configuration") || 
                message.contains("setting") || message.contains("property")) {
            return REASON_CONFIGURATION;
        } else {
            return REASON_UNKNOWN;
        }
    }
    
    /**
     * Gets the SMTP channel identifier.
     *
     * @return the SMTP channel identifier string
     */
    public static String getSmtpChannel() {
        return CHANNEL_SMTP;
    }
}