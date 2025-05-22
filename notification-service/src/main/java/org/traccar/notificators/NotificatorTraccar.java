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
import org.traccar.notificators.resilience.CircuitBreakerManager;
import org.traccar.tracing.DistributedTracingContext;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

@Singleton
public class NotificatorTraccar extends Notificator {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorTraccar.class);

    private final Client client;
    private final Storage storage;
    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final RateLimiter rateLimiter;
    private final Retry retry;
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
            MeterRegistry meterRegistry, Tracer tracer) {
        super(notificationFormatter, "short");
        this.client = client;
        this.storage = storage;
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        
        // Initialize circuit breaker for Traccar push API
        this.circuitBreaker = circuitBreakerManager.getCircuitBreaker("traccarPushApi");
        
        // Initialize rate limiter for Traccar push API
        this.rateLimiter = RateLimiter.of("traccarPushApi", RateLimiter.Config.custom()
                .limitRefreshPeriod(Duration.ofSeconds(1))
                .limitForPeriod(config.getInteger(Keys.NOTIFICATOR_TRACCAR_RATE_LIMIT, 10))
                .timeoutDuration(Duration.ofSeconds(5))
                .build());
        
        // Initialize retry with exponential backoff
        this.retry = Retry.of("traccarPushApi", Retry.Config.custom()
                .maxAttempts(config.getInteger(Keys.NOTIFICATOR_TRACCAR_MAX_RETRIES, 3))
                .waitDuration(Duration.ofMillis(config.getInteger(Keys.NOTIFICATOR_TRACCAR_RETRY_WAIT, 500)))
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(2.0)
                .build());
        
        // Initialize metrics
        this.metrics = new NotificationMetrics("traccar", meterRegistry);
        
        // Get configuration from environment or config file
        this.url = config.getString(Keys.NOTIFICATOR_TRACCAR_URL, "https://www.traccar.org/push/");
        this.key = config.getString(Keys.NOTIFICATOR_TRACCAR_KEY);
    }

    @Override
    public void send(User user, NotificationMessage shortMessage, Event event, Position position) {
        if (user.hasAttribute("notificationTokens")) {
            // Create span for distributed tracing
            Span span = tracer.spanBuilder("NotificatorTraccar.send").startSpan();
            try (var scope = DistributedTracingContext.activateSpan(span)) {
                span.setAttribute("notification.type", "traccar");
                span.setAttribute("notification.user.id", user.getId());
                span.setAttribute("notification.event.type", event != null ? event.getType() : "unknown");
                
                // Start metrics timer
                var timer = metrics.startSendTimer();
                
                NotificationObject item = new NotificationObject();
                item.title = shortMessage.getSubject();
                item.body = shortMessage.getBody();
                item.sound = "default";

                String[] tokenArray = user.getString("notificationTokens").split("[, ]");
                List<String> registrationTokens = new ArrayList<>(Arrays.asList(tokenArray));

                Message message = new Message();
                message.tokens = user.getString("notificationTokens").split("[, ]");
                message.notification = item;

                // Use circuit breaker, rate limiter and retry pattern with the API call
                Supplier<Response> decoratedSupplier = CircuitBreaker.decorateSupplier(
                        circuitBreaker,
                        RateLimiter.decorateSupplier(
                                rateLimiter,
                                Retry.decorateSupplier(
                                        retry,
                                        () -> client.target(url)
                                                .request()
                                                .header("Authorization", "key=" + key)
                                                .post(Entity.json(message))
                                )
                        )
                );

                try {
                    Response result = decoratedSupplier.get();
                    try {
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
                                span.setAttribute("notification.error", error.getString("message"));
                                metrics.recordError("traccar_push_error", errorCode);
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
                        
                        // Record success metrics
                        metrics.recordSuccess();
                        timer.stop();
                    } finally {
                        result.close();
                    }
                } catch (Exception e) {
                    LOGGER.warn("Push error", e);
                    span.recordException(e);
                    metrics.recordError("traccar_push_exception", e.getClass().getSimpleName());
                    timer.stop();
                }
            } finally {
                span.end();
            }
        }
    }
}