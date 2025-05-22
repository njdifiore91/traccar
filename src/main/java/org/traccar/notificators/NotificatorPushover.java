/*
 * Copyright 2020 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.retry.Retry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.metrics.NotificationMetrics;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;
import org.traccar.resilience.CircuitBreakerManager;
import org.traccar.tracing.DistributedTracingContext;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Notificator for Pushover service.
 * Implements resilient API calls with circuit breaker, rate limiting, and retry mechanisms.
 */
@Singleton
public class NotificatorPushover extends Notificator {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorPushover.class);
    
    private static final String PUSHOVER_API_URL = "https://api.pushover.net/1/messages.json";
    private static final String CIRCUIT_BREAKER_NAME = "pushover";
    private static final String RATE_LIMITER_NAME = "pushover";
    private static final String RETRY_NAME = "pushover";
    
    private final Client client;
    private final String token;
    private final String user;
    private final CircuitBreakerManager circuitBreakerManager;
    private final NotificationMetrics metrics;
    private final DistributedTracingContext tracingContext;

    /**
     * Message class for Pushover API requests.
     */
    public static class Message {
        @JsonProperty("token")
        private String token;
        @JsonProperty("user")
        private String user;
        @JsonProperty("device")
        private String device;
        @JsonProperty("title")
        private String title;
        @JsonProperty("message")
        private String message;
    }

    /**
     * Constructor for NotificatorPushover.
     * 
     * @param config Configuration provider
     * @param notificationFormatter Formatter for notifications
     * @param client HTTP client for API calls
     * @param circuitBreakerManager Circuit breaker manager for resilient API calls
     * @param metrics Metrics collector for notifications
     * @param tracingContext Distributed tracing context
     */
    @Inject
    public NotificatorPushover(
            Config config, 
            NotificationFormatter notificationFormatter, 
            Client client,
            CircuitBreakerManager circuitBreakerManager,
            NotificationMetrics metrics,
            DistributedTracingContext tracingContext) {
        super(notificationFormatter, "short");
        this.client = client;
        this.token = config.getString(Keys.NOTIFICATOR_PUSHOVER_TOKEN);
        this.user = config.getString(Keys.NOTIFICATOR_PUSHOVER_USER);
        this.circuitBreakerManager = circuitBreakerManager;
        this.metrics = metrics;
        this.tracingContext = tracingContext;
    }

    @Override
    public void send(User user, NotificationMessage shortMessage, Event event, Position position) {
        // Create a span for distributed tracing
        try (var span = tracingContext.startSpan("pushover.send")) {
            span.setAttribute("notification.type", "pushover");
            span.setAttribute("notification.recipient", user.getEmail());
            
            // Start metrics timer
            var timer = metrics.startPushoverTimer();
            
            try {
                Message message = new Message();
                message.token = token;

                message.user = user.getString("pushoverUserKey");
                if (message.user == null) {
                    message.user = this.user;
                }

                if (user.hasAttribute("pushoverDeviceNames")) {
                    message.device = user.getString("pushoverDeviceNames").replaceAll(" *, *", ",");
                }

                message.title = shortMessage.getSubject();
                message.message = shortMessage.getBody();
                
                span.setAttribute("pushover.message.title", message.title);
                
                // Get circuit breaker, rate limiter and retry instances
                CircuitBreaker circuitBreaker = circuitBreakerManager.getCircuitBreaker(CIRCUIT_BREAKER_NAME);
                RateLimiter rateLimiter = circuitBreakerManager.getRateLimiter(RATE_LIMITER_NAME);
                Retry retry = circuitBreakerManager.getRetry(RETRY_NAME);
                
                // Create a resilient supplier with circuit breaker, rate limiter and retry
                Supplier<Response> resilientSupplier = RateLimiter.decorateSupplier(
                    rateLimiter,
                    CircuitBreaker.decorateSupplier(
                        circuitBreaker,
                        Retry.decorateSupplier(
                            retry,
                            () -> sendRequest(message)
                        )
                    )
                );
                
                // Execute the call with all resilience patterns applied
                Response response = resilientSupplier.get();
                
                // Record metrics for successful call
                metrics.recordPushoverSuccess(timer);
                span.setAttribute("pushover.success", true);
                span.setAttribute("pushover.status_code", response.getStatus());
                
                // Close the response to release resources
                response.close();
                
            } catch (Exception e) {
                // Record metrics for failed call
                metrics.recordPushoverFailure(timer);
                span.setAttribute("pushover.success", false);
                span.setAttribute("pushover.error", e.getMessage());
                span.recordException(e);
                
                LOGGER.warn("Failed to send Pushover notification", e);
            }
        }
    }
    
    /**
     * Sends the actual HTTP request to Pushover API.
     * 
     * @param message The message to send
     * @return HTTP response from Pushover API
     */
    private Response sendRequest(Message message) {
        try (var span = tracingContext.startSpan("pushover.api.call")) {
            span.setAttribute("pushover.api.url", PUSHOVER_API_URL);
            
            Response response = client.target(PUSHOVER_API_URL)
                    .request()
                    .post(Entity.json(message));
            
            span.setAttribute("pushover.api.status", response.getStatus());
            return response;
        }
    }
}