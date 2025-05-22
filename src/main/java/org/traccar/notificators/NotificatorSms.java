/*
 * Copyright 2017 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.traccar.database.StatisticsManager;
import org.traccar.metrics.NotificationMetrics;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;
import org.traccar.resilience.CircuitBreakerManager;
import org.traccar.sms.SmsManager;
import org.traccar.tracing.DistributedTracingContext;
import org.traccar.discovery.ServiceDiscovery;

import java.util.function.Supplier;

@Singleton
public class NotificatorSms extends Notificator {

    private final SmsManager smsManager;
    private final StatisticsManager statisticsManager;
    private final CircuitBreaker circuitBreaker;
    private final RateLimiter rateLimiter;
    private final NotificationMetrics notificationMetrics;
    private final Tracer tracer;
    private final ServiceDiscovery serviceDiscovery;

    /**
     * Constructor for NotificatorSms with enhanced resilience and observability features.
     *
     * @param smsManager              SMS manager for sending messages
     * @param notificationFormatter   Formatter for notification messages
     * @param statisticsManager       Legacy statistics manager
     * @param circuitBreakerManager   Circuit breaker manager for SMS gateway connections
     * @param notificationMetrics     Metrics collector for notifications
     * @param tracingContext          Distributed tracing context
     * @param serviceDiscovery        Service discovery for dynamic SMS gateway discovery
     */
    @Inject
    public NotificatorSms(
            SmsManager smsManager, 
            NotificationFormatter notificationFormatter, 
            StatisticsManager statisticsManager,
            CircuitBreakerManager circuitBreakerManager,
            NotificationMetrics notificationMetrics,
            DistributedTracingContext tracingContext,
            ServiceDiscovery serviceDiscovery) {
        super(notificationFormatter, "short");
        this.smsManager = smsManager;
        this.statisticsManager = statisticsManager;
        this.notificationMetrics = notificationMetrics;
        this.tracer = tracingContext.getTracer();
        this.serviceDiscovery = serviceDiscovery;
        
        // Initialize circuit breaker for SMS gateway connections
        this.circuitBreaker = circuitBreakerManager.createCircuitBreaker("sms-gateway");
        
        // Initialize rate limiter for SMS sending
        this.rateLimiter = circuitBreakerManager.createRateLimiter("sms-notifications");
    }

    /**
     * Sends an SMS notification to a user with enhanced resilience and observability.
     * 
     * @param user      User to send notification to
     * @param message   Notification message content
     * @param event     Event that triggered the notification
     * @param position  Position associated with the event
     * @throws MessageException if message sending fails
     */
    @Override
    public void send(User user, NotificationMessage message, Event event, Position position) throws MessageException {
        if (user.getPhone() != null) {
            // Create a span for distributed tracing
            Span span = tracer.spanBuilder("sms.send").startSpan();
            try (var scope = span.makeCurrent()) {
                // Add relevant attributes to the span
                span.setAttribute("notification.type", "sms");
                span.setAttribute("user.id", user.getId());
                span.setAttribute("event.type", event != null ? event.getType() : "unknown");
                
                // Discover SMS gateway if dynamic discovery is enabled
                updateSmsGatewayIfNeeded();
                
                // Register SMS in legacy statistics manager
                statisticsManager.registerSms();
                
                // Record metrics for SMS notification
                notificationMetrics.recordSmsSent(user.getId(), event != null ? event.getType() : "unknown");
                
                // Execute SMS sending with circuit breaker and rate limiter
                Supplier<Void> sendSmsWithResilience = () -> {
                    try {
                        smsManager.sendMessage(user.getPhone(), message.getBody(), false);
                    } catch (MessageException e) {
                        span.recordException(e);
                        throw new RuntimeException(e);
                    }
                    return null;
                };
                
                // Apply rate limiting and circuit breaker patterns
                RateLimiter.decorateSupplier(rateLimiter, 
                    CircuitBreaker.decorateSupplier(circuitBreaker, sendSmsWithResilience)).get();
                
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR);
                if (e instanceof MessageException) {
                    throw (MessageException) e;
                } else {
                    throw new MessageException(e);
                }
            } finally {
                span.end();
            }
        }
    }
    
    /**
     * Updates the SMS gateway configuration if dynamic discovery is enabled.
     * This allows for runtime discovery of SMS gateway services in containerized environments.
     */
    private void updateSmsGatewayIfNeeded() {
        if (serviceDiscovery.isEnabled()) {
            serviceDiscovery.discoverService("sms-gateway").ifPresent(serviceInfo -> {
                // Update SMS gateway configuration if available
                smsManager.updateGatewayConfig(serviceInfo);
            });
        }
    }
}