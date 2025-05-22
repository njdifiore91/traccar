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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import io.opentelemetry.api.GlobalOpenTelemetry;
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
import org.traccar.discovery.ServiceDiscoveryManager;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class SnsSmsClient implements SmsManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(SnsSmsClient.class);

    private final AmazonSNSAsync snsClient;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Counter smsSuccessCounter;
    private final Counter smsFailureCounter;
    private final Timer smsTimer;

    public SnsSmsClient(Config config, ServiceDiscoveryManager serviceDiscoveryManager, MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.smsSuccessCounter = Counter.builder("sms.sent")
                .tag("status", "success")
                .tag("provider", "aws-sns")
                .description("Number of successfully sent SMS messages")
                .register(meterRegistry);
        
        this.smsFailureCounter = Counter.builder("sms.sent")
                .tag("status", "failure")
                .tag("provider", "aws-sns")
                .description("Number of failed SMS messages")
                .register(meterRegistry);
        
        this.smsTimer = Timer.builder("sms.duration")
                .tag("provider", "aws-sns")
                .description("Time taken to send SMS messages")
                .register(meterRegistry);
        
        // Initialize OpenTelemetry tracer
        this.tracer = GlobalOpenTelemetry.getTracer("org.traccar.sms.SnsSmsClient");
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("sns-sms-client");
        
        // Register event listeners for circuit breaker state changes
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}", 
                        event.getStateTransition().getFromState(), 
                        event.getStateTransition().getToState()));
        
        // Get AWS configuration from service discovery or fallback to config
        String awsAccessKey;
        String awsSecretKey;
        String awsRegion;
        
        try {
            Map<String, String> awsConfig = serviceDiscoveryManager.getServiceConfig("aws-sns");
            awsAccessKey = awsConfig.getOrDefault("access-key", config.getString(Keys.SMS_AWS_ACCESS));
            awsSecretKey = awsConfig.getOrDefault("secret-key", config.getString(Keys.SMS_AWS_SECRET));
            awsRegion = awsConfig.getOrDefault("region", config.getString(Keys.SMS_AWS_REGION));
        } catch (Exception e) {
            LOGGER.warn("Failed to get AWS configuration from service discovery, using config values", e);
            awsAccessKey = config.getString(Keys.SMS_AWS_ACCESS);
            awsSecretKey = config.getString(Keys.SMS_AWS_SECRET);
            awsRegion = config.getString(Keys.SMS_AWS_REGION);
        }
        
        // Initialize AWS SNS client
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        snsClient = AmazonSNSAsyncClientBuilder.standard()
                .withRegion(awsRegion)
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .build();
    }

    @Override
    public void sendMessage(String phone, String message, boolean command) {
        // Generate correlation ID if not present
        String correlationId = MDC.get("correlationId");
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
            MDC.put("correlationId", correlationId);
        }
        
        final String finalCorrelationId = correlationId;
        
        // Create a span for the SMS sending operation
        Span span = tracer.spanBuilder("sns.sendSms")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("phone.number.masked", maskPhoneNumber(phone))
                .setAttribute("message.length", message.length())
                .setAttribute("correlation.id", finalCorrelationId)
                .setAttribute("is.command", command)
                .startSpan();
        
        // Use try-with-resources to ensure the span context is properly closed
        try (Scope scope = span.makeCurrent()) {
            // Execute the SMS sending with circuit breaker protection
            circuitBreaker.executeSupplier(() -> {
                Timer.Sample sample = Timer.start(meterRegistry);
                
                try {
                    // Prepare SMS attributes
                    Map<String, MessageAttributeValue> smsAttributes = new HashMap<>();
                    smsAttributes.put(
                            "AWS.SNS.SMS.SenderID",
                            new MessageAttributeValue().withStringValue("SNS").withDataType("String"));
                    smsAttributes.put(
                            "AWS.SNS.SMS.SMSType",
                            new MessageAttributeValue().withStringValue("Transactional").withDataType("String"));
                    
                    // Add correlation ID as a message attribute
                    smsAttributes.put(
                            "correlation-id",
                            new MessageAttributeValue().withStringValue(finalCorrelationId).withDataType("String"));

                    // Create publish request
                    PublishRequest publishRequest = new PublishRequest()
                            .withMessage(message)
                            .withPhoneNumber(phone)
                            .withMessageAttributes(smsAttributes);

                    // Send SMS asynchronously and handle response
                    CompletableFuture<PublishResult> future = new CompletableFuture<>();
                    
                    snsClient.publishAsync(publishRequest, new AsyncHandler<PublishRequest, PublishResult>() {
                        @Override
                        public void onError(Exception exception) {
                            LOGGER.error("SMS send failed - correlationId: {}", finalCorrelationId, exception);
                            span.setStatus(StatusCode.ERROR, exception.getMessage());
                            span.recordException(exception);
                            smsFailureCounter.increment();
                            sample.stop(smsTimer);
                            future.completeExceptionally(exception);
                        }

                        @Override
                        public void onSuccess(PublishRequest request, PublishResult result) {
                            LOGGER.debug("SMS sent successfully - correlationId: {}, messageId: {}", 
                                    finalCorrelationId, result.getMessageId());
                            span.setAttribute("aws.sns.message.id", result.getMessageId());
                            smsSuccessCounter.increment();
                            sample.stop(smsTimer);
                            future.complete(result);
                        }
                    });
                    
                    return future;
                } catch (Exception e) {
                    LOGGER.error("Error preparing SMS request - correlationId: {}", finalCorrelationId, e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    smsFailureCounter.increment();
                    sample.stop(smsTimer);
                    throw e;
                }
            });
        } catch (Exception e) {
            LOGGER.error("Circuit breaker prevented SMS sending - correlationId: {}", finalCorrelationId, e);
            span.setStatus(StatusCode.ERROR, "Circuit breaker prevented SMS sending: " + e.getMessage());
            span.recordException(e);
        } finally {
            span.end();
            MDC.remove("correlationId");
        }
    }
    
    /**
     * Masks a phone number for logging and metrics to protect PII
     * @param phoneNumber The phone number to mask
     * @return Masked phone number (e.g., +1234567890 becomes +1****7890)
     */
    private String maskPhoneNumber(String phoneNumber) {
        if (phoneNumber == null || phoneNumber.length() <= 6) {
            return phoneNumber;
        }
        
        int visibleDigits = Math.min(4, phoneNumber.length() / 3);
        int prefixLength = 2;
        
        StringBuilder masked = new StringBuilder(phoneNumber.length());
        masked.append(phoneNumber, 0, prefixLength);
        
        for (int i = prefixLength; i < phoneNumber.length() - visibleDigits; i++) {
            masked.append('*');
        }
        
        masked.append(phoneNumber.substring(phoneNumber.length() - visibleDigits));
        return masked.toString();
    }
}