/*
 * Copyright 2021 - 2022 Anton Tananaev (anton@traccar.org)
 * Copyright 2021 Subodh Ranadive (subodhranadive3103@gmail.com)
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
package org.traccar.sms;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sns.AmazonSNSAsync;
import com.amazonaws.services.sns.AmazonSNSAsyncClientBuilder;
import com.amazonaws.services.sns.model.MessageAttributeValue;
import com.amazonaws.services.sns.model.PublishRequest;
import com.amazonaws.services.sns.model.PublishResult;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import org.traccar.config.Config;
import org.traccar.config.Keys;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * AWS SNS implementation of the SmsManager interface for sending SMS messages.
 * Includes circuit breaker pattern, distributed tracing, and dead letter queue integration.
 */
@Singleton
public class SnsSmsClient implements SmsManager, HealthCheck {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnsSmsClient.class);
    
    private final AmazonSNSAsync snsClient;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Tracer tracer;
    private final DeadLetterQueueService deadLetterQueueService;
    
    /**
     * Creates a new SnsSmsClient with the specified configuration and dependencies.
     *
     * @param config The notification service configuration
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param deadLetterQueueService Service for handling failed message deliveries
     */
    @Inject
    public SnsSmsClient(Config config, Tracer tracer, DeadLetterQueueService deadLetterQueueService) {
        this.tracer = tracer;
        this.deadLetterQueueService = deadLetterQueueService;
        
        // Initialize AWS SNS client with credentials from configuration
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(
                config.getString(Keys.SMS_AWS_ACCESS), config.getString(Keys.SMS_AWS_SECRET));
        snsClient = AmazonSNSAsyncClientBuilder.standard()
                .withRegion(config.getString(Keys.SMS_AWS_REGION))
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials)).build();
        
        // Configure circuit breaker with appropriate thresholds
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to trip circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before attempting again
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker("sns-sms-client");
        
        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait 500ms
                .retryExceptions(Exception.class) // Retry on all exceptions
                .ignoreExceptions(TimeoutException.class) // Don't retry on timeouts
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry("sns-sms-retry");
        
        LOGGER.info("SnsSmsClient initialized with circuit breaker and retry policies");
    }
    
    @Override
    public void sendMessage(String phone, String message, boolean command) {
        String correlationId = MDC.get("correlationId");
        if (correlationId == null) {
            correlationId = java.util.UUID.randomUUID().toString();
            MDC.put("correlationId", correlationId);
        }
        
        // Create span for distributed tracing
        Span span = tracer.spanBuilder("sns.sendSms")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("phone.masked", maskPhoneNumber(phone))
                .setAttribute("message.length", message.length())
                .setAttribute("correlation.id", correlationId)
                .setAttribute("command", command)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Prepare SMS attributes
            Map<String, MessageAttributeValue> smsAttributes = new HashMap<>();
            smsAttributes.put(
                    "AWS.SNS.SMS.SenderID",
                    new MessageAttributeValue().withStringValue("SNS").withDataType("String"));
            smsAttributes.put(
                    "AWS.SNS.SMS.SMSType",
                    new MessageAttributeValue().withStringValue("Transactional").withDataType("String"));
            
            // Create publish request
            PublishRequest publishRequest = new PublishRequest()
                    .withMessage(message)
                    .withPhoneNumber(phone)
                    .withMessageAttributes(smsAttributes);
            
            // Wrap SNS publish with circuit breaker and retry
            Supplier<CompletableFuture<PublishResult>> publishSupplier = () -> {
                CompletableFuture<PublishResult> future = new CompletableFuture<>();
                
                snsClient.publishAsync(publishRequest, new AsyncHandler<>() {
                    @Override
                    public void onError(Exception exception) {
                        LOGGER.error("SMS send failed to {}: {}", maskPhoneNumber(phone), exception.getMessage(), exception);
                        span.setStatus(StatusCode.ERROR, exception.getMessage());
                        span.recordException(exception);
                        future.completeExceptionally(exception);
                        
                        // Send to dead letter queue for later retry/analysis
                        deadLetterQueueService.sendToDeadLetterQueue("sms", phone, message, correlationId, exception);
                    }
                    
                    @Override
                    public void onSuccess(PublishRequest request, PublishResult result) {
                        LOGGER.debug("SMS sent successfully to {}, message ID: {}", 
                                maskPhoneNumber(phone), result.getMessageId());
                        span.setAttribute("aws.sns.messageId", result.getMessageId());
                        future.complete(result);
                    }
                });
                
                return future;
            };
            
            // Apply circuit breaker and retry patterns
            Supplier<CompletableFuture<PublishResult>> retryingSupplier = 
                    Retry.decorateSupplier(retry, publishSupplier);
            
            Supplier<CompletableFuture<PublishResult>> circuitBreakingSupplier = 
                    CircuitBreaker.decorateSupplier(circuitBreaker, retryingSupplier);
            
            // Execute the call (non-blocking)
            circuitBreakingSupplier.get().exceptionally(ex -> {
                LOGGER.error("All attempts to send SMS failed: {}", ex.getMessage());
                return null;
            });
            
        } finally {
            span.end();
        }
    }
    
    /**
     * Masks a phone number for logging purposes, showing only the last 4 digits.
     *
     * @param phoneNumber The phone number to mask
     * @return The masked phone number
     */
    private String maskPhoneNumber(String phoneNumber) {
        if (phoneNumber == null || phoneNumber.length() <= 4) {
            return "****";
        }
        return "*****" + phoneNumber.substring(phoneNumber.length() - 4);
    }
    
    @Override
    public boolean isHealthy() {
        return !circuitBreaker.getState().equals(CircuitBreaker.State.OPEN);
    }
    
    @Override
    public Map<String, Object> getHealthMetrics() {
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("circuitBreakerState", circuitBreaker.getState().toString());
        metrics.put("failureRate", circuitBreaker.getMetrics().getFailureRate());
        metrics.put("successfulCalls", circuitBreaker.getMetrics().getNumberOfSuccessfulCalls());
        metrics.put("failedCalls", circuitBreaker.getMetrics().getNumberOfFailedCalls());
        metrics.put("notPermittedCalls", circuitBreaker.getMetrics().getNumberOfNotPermittedCalls());
        return metrics;
    }
}