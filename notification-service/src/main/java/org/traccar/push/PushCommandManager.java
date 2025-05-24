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
package org.traccar.push;

import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.github.resilience4j.ratelimiter.RateLimiterRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * PushCommandManager handles push notification commands received from the message broker,
 * applies resilience patterns (circuit breaker, retry, rate limiting), and tracks delivery status.
 */
@Service
public class PushCommandManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(PushCommandManager.class);

    private static final String CIRCUIT_BREAKER_NAME = "firebasePush";
    private static final String RETRY_NAME = "firebasePush";
    private static final String RATE_LIMITER_NAME = "firebasePush";
    
    private static final String PUSH_COMMAND_TOPIC = "notification.command.push";
    private static final String PUSH_STATUS_TOPIC = "notification.status";
    private static final String PUSH_DLQ_TOPIC = "notification.dlq";

    private final FirebaseMessaging firebaseMessaging;
    private final KafkaTemplate<String, PushStatusMessage> kafkaTemplate;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final RateLimiter rateLimiter;
    private final Tracer tracer;
    private final Meter meter;
    private LongCounter pushRequestCounter;
    private LongCounter pushSuccessCounter;
    private LongCounter pushFailureCounter;
    private LongCounter pushRetryCounter;
    private LongCounter pushDlqCounter;

    @Value("${firebase.rate-limit.limit-for-period:100}")
    private int rateLimitForPeriod;

    @Value("${firebase.rate-limit.limit-refresh-period-ms:1000}")
    private int rateLimitRefreshPeriodMs;

    @Value("${firebase.circuit-breaker.failure-rate-threshold:50}")
    private float circuitBreakerFailureRateThreshold;

    @Value("${firebase.circuit-breaker.wait-duration-in-open-state-ms:60000}")
    private long circuitBreakerWaitDurationInOpenStateMs;

    @Value("${firebase.retry.max-attempts:3}")
    private int retryMaxAttempts;

    @Value("${firebase.retry.initial-interval-ms:1000}")
    private long retryInitialIntervalMs;

    @Value("${firebase.retry.multiplier:2.0}")
    private double retryMultiplier;

    @Autowired
    public PushCommandManager(
            FirebaseMessaging firebaseMessaging,
            KafkaTemplate<String, PushStatusMessage> kafkaTemplate,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            RateLimiterRegistry rateLimiterRegistry,
            Tracer tracer,
            Meter meter) {
        this.firebaseMessaging = firebaseMessaging;
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracer;
        this.meter = meter;
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(circuitBreakerFailureRateThreshold)
                .waitDurationInOpenState(Duration.ofMillis(circuitBreakerWaitDurationInOpenStateMs))
                .permittedNumberOfCallsInHalfOpenState(10)
                .slidingWindowSize(100)
                .recordExceptions(FirebaseMessagingException.class, TimeoutException.class)
                .build();
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, circuitBreakerConfig);
        
        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(retryMaxAttempts)
                .waitDuration(Duration.ofMillis(retryInitialIntervalMs))
                .retryExceptions(FirebaseMessagingException.class)
                .ignoreExceptions(IllegalArgumentException.class)
                .retryOnResult(result -> result != null && "UNREGISTERED".equals(result))
                .exponentialBackoff(Duration.ofMillis(retryInitialIntervalMs), Duration.ofSeconds(60), retryMultiplier)
                .build();
        this.retry = retryRegistry.retry(RETRY_NAME, retryConfig);
        
        // Configure rate limiter
        RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMillis(rateLimitRefreshPeriodMs))
                .limitForPeriod(rateLimitForPeriod)
                .timeoutDuration(Duration.ofSeconds(5))
                .build();
        this.rateLimiter = rateLimiterRegistry.rateLimiter(RATE_LIMITER_NAME, rateLimiterConfig);
    }

    @PostConstruct
    public void init() {
        // Initialize OpenTelemetry metrics
        pushRequestCounter = meter.counterBuilder("push.requests.total")
                .setDescription("Total number of push notification requests")
                .build();
        
        pushSuccessCounter = meter.counterBuilder("push.success.total")
                .setDescription("Total number of successful push notifications")
                .build();
        
        pushFailureCounter = meter.counterBuilder("push.failures.total")
                .setDescription("Total number of failed push notifications")
                .build();
        
        pushRetryCounter = meter.counterBuilder("push.retries.total")
                .setDescription("Total number of push notification retries")
                .build();
        
        pushDlqCounter = meter.counterBuilder("push.dlq.total")
                .setDescription("Total number of push notifications sent to DLQ")
                .build();
        
        LOGGER.info("PushCommandManager initialized with rate limit: {} requests per {} ms", 
                rateLimitForPeriod, rateLimitRefreshPeriodMs);
    }

    /**
     * Consumes push notification commands from the message broker.
     * 
     * @param pushCommand The push command message containing token and payload
     */
    @KafkaListener(topics = PUSH_COMMAND_TOPIC, groupId = "notification-service-push")
    public void consumePushCommand(PushCommandMessage pushCommand) {
        String correlationId = pushCommand.getCorrelationId();
        
        // Create a span for this operation
        Span span = tracer.spanBuilder("process_push_command")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("correlation.id", correlationId)
                .setAttribute("token.id", pushCommand.getTokenId())
                .setAttribute("user.id", pushCommand.getUserId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            LOGGER.debug("Received push command: {}", pushCommand);
            pushRequestCounter.add(1, Attributes.of(
                    AttributeKey.stringKey("user_id"), String.valueOf(pushCommand.getUserId())));
            
            // Apply rate limiting
            rateLimiter.acquirePermission();
            
            // Process the push notification with resilience patterns
            sendPushNotification(pushCommand, correlationId, span);
            
        } catch (Exception e) {
            LOGGER.error("Error processing push command: {}", e.getMessage(), e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            
            // Send to DLQ after all retries are exhausted
            sendToDlq(pushCommand, correlationId, e.getMessage());
            pushDlqCounter.add(1);
            
        } finally {
            span.end();
        }
    }

    /**
     * Sends a push notification with circuit breaker and retry patterns applied.
     * 
     * @param pushCommand The push command message
     * @param correlationId The correlation ID for tracing
     * @param parentSpan The parent span for this operation
     */
    private void sendPushNotification(PushCommandMessage pushCommand, String correlationId, Span parentSpan) {
        // Create a child span for the Firebase operation
        Span firebaseSpan = tracer.spanBuilder("send_firebase_push")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("correlation.id", correlationId)
                .setAttribute("token", pushCommand.getToken())
                .startSpan();
        
        try (Scope scope = firebaseSpan.makeCurrent()) {
            // Build the Firebase message
            Message message = Message.builder()
                    .setToken(pushCommand.getToken())
                    .putAllData(pushCommand.getPayload())
                    .putData("correlationId", correlationId)
                    .build();
            
            // Apply circuit breaker and retry patterns
            Supplier<String> firebaseCallSupplier = () -> {
                try {
                    return firebaseMessaging.send(message);
                } catch (FirebaseMessagingException e) {
                    // Record retry attempt
                    if (e.getMessagingErrorCode() != null && 
                            e.getMessagingErrorCode().name().equals("UNAVAILABLE")) {
                        pushRetryCounter.add(1);
                        LOGGER.warn("Transient error sending push notification, will retry: {}", 
                                e.getMessage());
                    }
                    throw new RuntimeException(e);
                }
            };
            
            // Execute with resilience patterns
            String messageId = circuitBreaker.executeSupplier(
                    retry.decorateSupplier(firebaseCallSupplier));
            
            // Record success and publish status
            LOGGER.info("Successfully sent push notification, messageId: {}", messageId);
            pushSuccessCounter.add(1);
            firebaseSpan.setStatus(StatusCode.OK);
            
            // Publish delivery status
            publishDeliveryStatus(pushCommand, correlationId, messageId, true, null);
            
        } catch (Exception e) {
            // Record failure
            LOGGER.error("Failed to send push notification: {}", e.getMessage(), e);
            pushFailureCounter.add(1);
            firebaseSpan.setStatus(StatusCode.ERROR, e.getMessage());
            firebaseSpan.recordException(e);
            
            // Publish failure status
            publishDeliveryStatus(pushCommand, correlationId, null, false, e.getMessage());
            
            // Re-throw to trigger DLQ handling
            throw e;
            
        } finally {
            firebaseSpan.end();
        }
    }

    /**
     * Publishes the delivery status of a push notification to the status topic.
     * 
     * @param pushCommand The original push command
     * @param correlationId The correlation ID for tracing
     * @param messageId The Firebase message ID (if successful)
     * @param success Whether the delivery was successful
     * @param errorMessage The error message (if failed)
     */
    private void publishDeliveryStatus(
            PushCommandMessage pushCommand, 
            String correlationId, 
            String messageId, 
            boolean success, 
            String errorMessage) {
        
        Span span = tracer.spanBuilder("publish_push_status")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("correlation.id", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            PushStatusMessage statusMessage = new PushStatusMessage();
            statusMessage.setCorrelationId(correlationId);
            statusMessage.setUserId(pushCommand.getUserId());
            statusMessage.setTokenId(pushCommand.getTokenId());
            statusMessage.setToken(pushCommand.getToken());
            statusMessage.setSuccess(success);
            statusMessage.setMessageId(messageId);
            statusMessage.setErrorMessage(errorMessage);
            statusMessage.setTimestamp(System.currentTimeMillis());
            
            kafkaTemplate.send(PUSH_STATUS_TOPIC, statusMessage);
            LOGGER.debug("Published push notification status: {}", statusMessage);
            
        } catch (Exception e) {
            LOGGER.error("Failed to publish push status: {}", e.getMessage(), e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Sends a failed push notification to the dead letter queue.
     * 
     * @param pushCommand The original push command
     * @param correlationId The correlation ID for tracing
     * @param errorMessage The error message
     */
    private void sendToDlq(PushCommandMessage pushCommand, String correlationId, String errorMessage) {
        Span span = tracer.spanBuilder("send_to_dlq")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("correlation.id", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Create a DLQ message with error context
            PushDlqMessage dlqMessage = new PushDlqMessage();
            dlqMessage.setCorrelationId(correlationId);
            dlqMessage.setOriginalCommand(pushCommand);
            dlqMessage.setErrorMessage(errorMessage);
            dlqMessage.setTimestamp(System.currentTimeMillis());
            dlqMessage.setRetryCount(retry.getMetrics().getNumberOfFailedCallsWithRetryAttempt());
            
            // Send to DLQ topic
            kafkaTemplate.send(PUSH_DLQ_TOPIC, dlqMessage);
            LOGGER.info("Sent failed push notification to DLQ: {}", correlationId);
            
        } catch (Exception e) {
            LOGGER.error("Failed to send to DLQ: {}", e.getMessage(), e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Message structure for push notification commands.
     */
    public static class PushCommandMessage {
        private String correlationId;
        private long userId;
        private long tokenId;
        private String token;
        private Map<String, String> payload;

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public long getTokenId() {
            return tokenId;
        }

        public void setTokenId(long tokenId) {
            this.tokenId = tokenId;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public Map<String, String> getPayload() {
            return payload;
        }

        public void setPayload(Map<String, String> payload) {
            this.payload = payload;
        }

        @Override
        public String toString() {
            return "PushCommandMessage{" +
                    "correlationId='" + correlationId + '\'' +
                    ", userId=" + userId +
                    ", tokenId=" + tokenId +
                    ", token='" + token + '\'' +
                    ", payload=" + payload +
                    '}';
        }
    }

    /**
     * Message structure for push notification status updates.
     */
    public static class PushStatusMessage {
        private String correlationId;
        private long userId;
        private long tokenId;
        private String token;
        private boolean success;
        private String messageId;
        private String errorMessage;
        private long timestamp;

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public long getTokenId() {
            return tokenId;
        }

        public void setTokenId(long tokenId) {
            this.tokenId = tokenId;
        }

        public String getToken() {
            return token;
        }

        public void setToken(String token) {
            this.token = token;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getMessageId() {
            return messageId;
        }

        public void setMessageId(String messageId) {
            this.messageId = messageId;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "PushStatusMessage{" +
                    "correlationId='" + correlationId + '\'' +
                    ", userId=" + userId +
                    ", tokenId=" + tokenId +
                    ", token='" + token + '\'' +
                    ", success=" + success +
                    ", messageId='" + messageId + '\'' +
                    ", errorMessage='" + errorMessage + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }

    /**
     * Message structure for push notifications sent to the dead letter queue.
     */
    public static class PushDlqMessage {
        private String correlationId;
        private PushCommandMessage originalCommand;
        private String errorMessage;
        private long timestamp;
        private long retryCount;

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }

        public PushCommandMessage getOriginalCommand() {
            return originalCommand;
        }

        public void setOriginalCommand(PushCommandMessage originalCommand) {
            this.originalCommand = originalCommand;
        }

        public String getErrorMessage() {
            return errorMessage;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getRetryCount() {
            return retryCount;
        }

        public void setRetryCount(long retryCount) {
            this.retryCount = retryCount;
        }

        @Override
        public String toString() {
            return "PushDlqMessage{" +
                    "correlationId='" + correlationId + '\'' +
                    ", originalCommand=" + originalCommand +
                    ", errorMessage='" + errorMessage + '\'' +
                    ", timestamp=" + timestamp +
                    ", retryCount=" + retryCount +
                    '}';
        }
    }
}