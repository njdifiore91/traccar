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
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.ObjectOperation;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;
import org.traccar.metrics.NotificationMetrics;
import org.traccar.resilience.CircuitBreakerManager;
import org.traccar.tracing.DistributedTracingContext;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Singleton
public class NotificatorTraccar extends Notificator {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorTraccar.class);

    private final Client client;
    private final Storage storage;
    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final RateLimiter rateLimiter;
    private final NotificationMetrics metrics;
    private final Tracer tracer;

    private final String url;
    private final String key;

    public static class NotificationObject {
        @JsonProperty("title")
        private String title;
        @JsonProperty("body")
        private String body;
        @JsonProperty("sound")
        private String sound;
    }

    public static class Message {
        @JsonProperty("registration_ids")
        private String[] tokens;
        @JsonProperty("notification")
        private NotificationObject notification;
    }

    @Inject
    public NotificatorTraccar(
            Config config, NotificationFormatter notificationFormatter, Client client,
            Storage storage, CacheManager cacheManager, CircuitBreakerManager circuitBreakerManager,
            NotificationMetrics metrics, DistributedTracingContext tracingContext) {
        super(notificationFormatter, "short");
        this.client = client;
        this.storage = storage;
        this.cacheManager = cacheManager;
        this.metrics = metrics;
        this.tracer = tracingContext.getTracer();
        
        // Initialize circuit breaker for Traccar push API
        this.circuitBreaker = circuitBreakerManager.getCircuitBreaker("traccar-push-api");
        
        // Initialize retry mechanism with exponential backoff
        this.retry = circuitBreakerManager.getRetryWithExponentialBackoff("traccar-push-api");
        
        // Initialize rate limiter for Traccar push API
        this.rateLimiter = circuitBreakerManager.getRateLimiter("traccar-push-api");
        
        // Get configuration from environment or config file for containerized support
        String configuredUrl = System.getenv("TRACCAR_PUSH_URL");
        this.url = configuredUrl != null ? configuredUrl : config.getString(Keys.NOTIFICATOR_TRACCAR_URL, "https://www.traccar.org/push/");
        
        String configuredKey = System.getenv("TRACCAR_PUSH_KEY");
        this.key = configuredKey != null ? configuredKey : config.getString(Keys.NOTIFICATOR_TRACCAR_KEY);
    }

    @Override
    public void send(User user, NotificationMessage shortMessage, Event event, Position position) {
        if (user.hasAttribute("notificationTokens")) {
            // Create span for distributed tracing
            Span span = tracer.spanBuilder("NotificatorTraccar.send").startSpan();
            try {
                // Record metrics for notification attempt
                metrics.recordNotificationAttempt("traccar");
                
                NotificationObject item = new NotificationObject();
                item.title = shortMessage.getSubject();
                item.body = shortMessage.getBody();
                item.sound = "default";

                String[] tokenArray = user.getString("notificationTokens").split("[, ]");
                List<String> registrationTokens = new ArrayList<>(Arrays.asList(tokenArray));

                Message message = new Message();
                message.tokens = user.getString("notificationTokens").split("[, ]");
                message.notification = item;

                // Add tracing context to the span
                span.setAttribute("user.id", user.getId());
                span.setAttribute("notification.tokens.count", tokenArray.length);
                span.setAttribute("notification.type", "traccar");
                if (event != null) {
                    span.setAttribute("event.type", event.getType());
                }
                
                // Execute with circuit breaker, retry, and rate limiter
                Supplier<Response> apiCallSupplier = () -> {
                    // Apply rate limiting
                    rateLimiter.acquirePermission();
                    
                    // Make the API call with tracing context
                    var request = client.target(url).request().header("Authorization", "key=" + key);
                    return request.post(Entity.json(message));
                };
                
                // Combine circuit breaker, retry, and execute
                Response result = Retry.decorateSupplier(retry, 
                                    CircuitBreaker.decorateSupplier(circuitBreaker, apiCallSupplier))
                                    .get();
                
                try {
                    // Record successful notification
                    metrics.recordNotificationSuccess("traccar");
                    
                    var json = result.readEntity(JsonObject.class);
                    List<String> failedTokens = new LinkedList<>();
                    var responses = json.getJsonArray("responses");
                    for (int i = 0; i < responses.size(); i++) {
                        var response = responses.getJsonObject(i);
                        if (!response.getBoolean("success")) {
                            var error = response.getJsonObject("error");
                            String errorCode = error.getString("code");
                            if (errorCode.equals("messaging/invalid-argument")
                                    || errorCode.equals("messaging/registration-token-not-registered")) {
                                failedTokens.add(registrationTokens.get(i));
                            }
                            LOGGER.warn("Push user {} error - {}", user.getId(), error.getString("message"));
                            
                            // Record error in metrics and tracing
                            metrics.recordNotificationError("traccar", errorCode);
                            span.setAttribute("error", true);
                            span.setAttribute("error.code", errorCode);
                            span.setAttribute("error.message", error.getString("message"));
                        }
                    }
                    if (!failedTokens.isEmpty()) {
                        registrationTokens.removeAll(failedTokens);
                        if (registrationTokens.isEmpty()) {
                            user.removeAttribute("notificationTokens");
                        } else {
                            user.set("notificationTokens", String.join(",", registrationTokens));
                        }
                        storage.updateObject(user, new Request(
                                new Columns.Include("attributes"),
                                new Condition.Equals("id", user.getId())));
                        cacheManager.invalidateObject(true, User.class, user.getId(), ObjectOperation.UPDATE);
                    }
                } finally {
                    if (result != null) {
                        result.close();
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Push error", e);
                
                // Record error in metrics and tracing
                metrics.recordNotificationError("traccar", e.getClass().getSimpleName());
                span.setAttribute("error", true);
                span.setAttribute("error.type", e.getClass().getName());
                span.setAttribute("error.message", e.getMessage() != null ? e.getMessage() : "null");
            } finally {
                span.end();
            }
        }
    }
}