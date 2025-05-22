/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.notificators;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.mail.MessagingException;

import org.traccar.mail.MailManager;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;
import org.traccar.notification.NotificationMetrics;
import org.traccar.resilience.CircuitBreakerManager;
import org.traccar.tracing.DistributedTracingContext;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Email notification handler with circuit breaker, rate limiting, and distributed tracing support.
 */
@Singleton
public class NotificatorMail extends Notificator {

    private final MailManager mailManager;
    private final CircuitBreakerManager circuitBreakerManager;
    private final DistributedTracingContext tracingContext;
    private final NotificationMetrics notificationMetrics;
    private final RateLimiter rateLimiter;

    private static final String CIRCUIT_BREAKER_NAME = "emailNotification";
    private static final String RATE_LIMITER_NAME = "emailRateLimiter";

    /**
     * Constructs a new email notificator with circuit breaker, rate limiting, and tracing support.
     *
     * @param mailManager           The mail manager for sending emails
     * @param notificationFormatter The formatter for notification messages
     * @param circuitBreakerManager The circuit breaker manager for SMTP connections
     * @param tracingContext        The distributed tracing context
     * @param notificationMetrics   The metrics collector for notifications
     */
    @Inject
    public NotificatorMail(MailManager mailManager, 
                          NotificationFormatter notificationFormatter,
                          CircuitBreakerManager circuitBreakerManager,
                          DistributedTracingContext tracingContext,
                          NotificationMetrics notificationMetrics) {
        super(notificationFormatter, "full");
        this.mailManager = mailManager;
        this.circuitBreakerManager = circuitBreakerManager;
        this.tracingContext = tracingContext;
        this.notificationMetrics = notificationMetrics;
        
        // Configure rate limiter for email sending
        RateLimiterConfig rateLimiterConfig = RateLimiterConfig.custom()
                .limitRefreshPeriod(Duration.ofMinutes(1))
                .limitForPeriod(100) // Allow 100 emails per minute
                .timeoutDuration(Duration.ofSeconds(5))
                .build();
        
        this.rateLimiter = RateLimiter.of(RATE_LIMITER_NAME, rateLimiterConfig);
    }

    @Override
    public void send(User user, NotificationMessage message, Event event, Position position) throws MessageException {
        // Create a span for distributed tracing
        Span span = tracingContext.startSpan("email.send");
        
        try {
            // Add relevant attributes to the span
            span.setAttribute("notification.type", "email");
            span.setAttribute("notification.recipient", user.getEmail());
            span.setAttribute("notification.subject", message.getSubject());
            if (event != null) {
                span.setAttribute("event.type", event.getType());
                span.setAttribute("event.id", event.getId());
            }
            
            // Get the circuit breaker for SMTP connections
            CircuitBreaker circuitBreaker = circuitBreakerManager.getCircuitBreaker(CIRCUIT_BREAKER_NAME);
            
            // Create a supplier that sends the email
            Supplier<Void> emailSupplier = () -> {
                try {
                    // Start timer for metrics
                    long startTime = System.currentTimeMillis();
                    
                    // Send the email
                    mailManager.sendMessage(user, false, message.getSubject(), message.getBody());
                    
                    // Record metrics
                    long duration = System.currentTimeMillis() - startTime;
                    notificationMetrics.recordEmailSent(duration);
                    
                    return null;
                } catch (MessagingException e) {
                    // Record failure metrics
                    notificationMetrics.recordEmailFailure(e.getClass().getSimpleName());
                    
                    // Add error details to the span
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    
                    throw new RuntimeException(e);
                }
            };
            
            // Apply rate limiting and circuit breaker to the email sending operation
            try {
                // First apply rate limiting
                RateLimiter.decorateSupplier(rateLimiter, () -> {
                    // Then apply circuit breaker
                    return CircuitBreaker.decorateSupplier(circuitBreaker, emailSupplier).get();
                }).get();
            } catch (Exception e) {
                if (e.getCause() instanceof MessagingException) {
                    throw new MessageException((MessagingException) e.getCause());
                }
                throw new MessageException(e);
            }
        } finally {
            // End the span
            span.end();
        }
    }
}