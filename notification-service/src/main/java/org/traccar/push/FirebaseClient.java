/*
 * Copyright 2022 - 2025 Anton Tananaev (anton@traccar.org)
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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.AndroidNotification;
import com.google.firebase.messaging.ApnsConfig;
import com.google.firebase.messaging.Aps;
import com.google.firebase.messaging.FirebaseMessaging;
import com.google.firebase.messaging.FirebaseMessagingException;
import com.google.firebase.messaging.Message;
import com.google.firebase.messaging.Notification;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@Service
public class FirebaseClient implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirebaseClient.class);

    private static final String CIRCUIT_BREAKER_NAME = "firebaseCircuitBreaker";
    private static final String RETRY_NAME = "firebaseRetry";
    private static final String FIREBASE_HEALTH_CHECK_TOKEN = "firebase-health-check";

    // OpenTelemetry metric keys
    private static final AttributeKey<String> NOTIFICATION_TYPE = AttributeKey.stringKey("notification.type");
    private static final AttributeKey<String> NOTIFICATION_STATUS = AttributeKey.stringKey("notification.status");
    private static final AttributeKey<String> ERROR_TYPE = AttributeKey.stringKey("error.type");

    @Value("${firebase.serviceAccountJson:}")
    private String serviceAccountJson;

    @Value("${firebase.databaseUrl:}")
    private String databaseUrl;

    @Value("${firebase.circuitBreaker.failureRateThreshold:50}")
    private float failureRateThreshold;

    @Value("${firebase.circuitBreaker.waitDurationInOpenState:30000}")
    private long waitDurationInOpenState;

    @Value("${firebase.circuitBreaker.permittedNumberOfCallsInHalfOpenState:10}")
    private int permittedNumberOfCallsInHalfOpenState;

    @Value("${firebase.circuitBreaker.slidingWindowSize:100}")
    private int slidingWindowSize;

    @Value("${firebase.retry.maxAttempts:3}")
    private int maxRetryAttempts;

    @Value("${firebase.retry.waitDuration:1000}")
    private long retryWaitDuration;

    private final AtomicBoolean initialized = new AtomicBoolean();
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    private CircuitBreaker circuitBreaker;
    private Retry retry;

    // OpenTelemetry components
    private final Meter meter;
    private final Tracer tracer;
    private LongCounter notificationCounter;
    private LongCounter errorCounter;

    private final DiscoveryClient discoveryClient;

    @Autowired
    public FirebaseClient(Meter meter, Tracer tracer, DiscoveryClient discoveryClient) {
        this.meter = meter;
        this.tracer = tracer;
        this.discoveryClient = discoveryClient;
    }

    @PostConstruct
    public void init() {
        if (serviceAccountJson != null && !serviceAccountJson.isEmpty()) {
            try {
                // Initialize OpenTelemetry metrics
                initializeMetrics();

                // Initialize circuit breaker
                initializeCircuitBreaker();

                // Initialize retry mechanism
                initializeRetry();

                // Initialize Firebase
                initializeFirebase();

                // Register with service discovery
                registerService();

                // Perform health check
                checkHealth();

                initialized.set(true);
                LOGGER.info("Firebase client initialized successfully");
            } catch (Exception e) {
                LOGGER.error("Failed to initialize Firebase client", e);
                healthy.set(false);
            }
        } else {
            LOGGER.info("Firebase client not enabled");
        }
    }

    private void initializeMetrics() {
        notificationCounter = meter
                .counterBuilder("firebase.notifications")
                .setDescription("Number of Firebase notifications sent")
                .build();

        errorCounter = meter
                .counterBuilder("firebase.errors")
                .setDescription("Number of Firebase notification errors")
                .build();
    }

    private void initializeCircuitBreaker() {
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(failureRateThreshold)
                .waitDurationInOpenState(Duration.ofMillis(waitDurationInOpenState))
                .permittedNumberOfCallsInHalfOpenState(permittedNumberOfCallsInHalfOpenState)
                .slidingWindowSize(slidingWindowSize)
                .recordExceptions(FirebaseMessagingException.class, IOException.class)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);

        // Register event listeners for monitoring
        circuitBreaker.getEventPublisher().onStateTransition(event -> {
            LOGGER.info("Circuit breaker state changed from {} to {}", 
                    event.getStateTransition().getFromState(),
                    event.getStateTransition().getToState());
            if (event.getStateTransition().getToState() == CircuitBreaker.State.OPEN) {
                healthy.set(false);
            } else if (event.getStateTransition().getToState() == CircuitBreaker.State.CLOSED) {
                healthy.set(true);
            }
        });
    }

    private void initializeRetry() {
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxRetryAttempts)
                .waitDuration(Duration.ofMillis(retryWaitDuration))
                .retryExceptions(FirebaseMessagingException.class, IOException.class)
                .build();

        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry(RETRY_NAME);
    }

    private void initializeFirebase() throws IOException {
        InputStream serviceAccount = new ByteArrayInputStream(serviceAccountJson.getBytes(StandardCharsets.UTF_8));
        FirebaseOptions.Builder optionsBuilder = FirebaseOptions.builder()
                .setCredentials(GoogleCredentials.fromStream(serviceAccount));

        if (databaseUrl != null && !databaseUrl.isEmpty()) {
            optionsBuilder.setDatabaseUrl(databaseUrl);
        }

        FirebaseApp.initializeApp(optionsBuilder.build());
    }

    private void registerService() {
        // Service is automatically registered with Spring Cloud Discovery Client
        LOGGER.info("Firebase client registered with service discovery. Available services: {}", 
                discoveryClient.getServices());
    }

    private void checkHealth() {
        try {
            // Send a test message to verify Firebase connectivity
            Message message = Message.builder()
                    .setToken(FIREBASE_HEALTH_CHECK_TOKEN)
                    .build();

            // This will fail with an expected error since the token is invalid,
            // but it will verify that we can connect to Firebase
            FirebaseMessaging.getInstance().send(message);
        } catch (FirebaseMessagingException e) {
            // Expected error for invalid token, but connection is working
            if (e.getMessagingErrorCode().name().equals("INVALID_ARGUMENT") || 
                e.getMessagingErrorCode().name().equals("UNREGISTERED")) {
                LOGGER.debug("Firebase health check completed successfully");
                healthy.set(true);
            } else {
                LOGGER.error("Firebase health check failed", e);
                healthy.set(false);
            }
        } catch (Exception e) {
            LOGGER.error("Firebase health check failed with unexpected error", e);
            healthy.set(false);
        }
    }

    @PreDestroy
    public void cleanup() {
        try {
            if (FirebaseApp.getApps() != null && !FirebaseApp.getApps().isEmpty()) {
                FirebaseApp.getInstance().delete();
                LOGGER.info("Firebase client shut down successfully");
            }
        } catch (Exception e) {
            LOGGER.warn("Error shutting down Firebase client", e);
        }
    }

    public CompletableFuture<String> sendMessage(String token, Map<String, String> data, String title, String body) {
        if (!initialized.get()) {
            return CompletableFuture.failedFuture(new IllegalStateException("Firebase client not initialized"));
        }

        Span span = tracer.spanBuilder("firebase.send").startSpan();
        Context context = Context.current().with(span);

        try {
            span.setAttribute("token.length", token.length());
            span.setAttribute("notification.title", title != null ? title : "null");
            span.setAttribute("notification.has_body", body != null);
            span.setAttribute("notification.data_fields", data != null ? data.size() : 0);

            return context.wrap(CompletableFuture.supplyAsync(() -> {
                try {
                    return Retry.decorateSupplier(retry, () -> 
                        CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                            try {
                                Message.Builder builder = Message.builder().setToken(token);
                                if (data != null) {
                                    builder.putAllData(data);
                                }
                                if (title != null) {
                                    builder.setNotification(Notification.builder().setTitle(title).setBody(body).build());
                                    builder.setAndroidConfig(AndroidConfig.builder()
                                            .setNotification(AndroidNotification.builder().setSound("default").build())
                                            .build());
                                    builder.setApnsConfig(ApnsConfig.builder()
                                            .setAps(Aps.builder().setSound("default").build())
                                            .build());
                                }

                                String messageId = FirebaseMessaging.getInstance().send(builder.build());
                                notificationCounter.add(1, Attributes.of(
                                    NOTIFICATION_TYPE, title != null ? "notification" : "data",
                                    NOTIFICATION_STATUS, "success"
                                ));
                                span.setStatus(StatusCode.OK);
                                return messageId;
                            } catch (FirebaseMessagingException e) {
                                errorCounter.add(1, Attributes.of(
                                    NOTIFICATION_TYPE, title != null ? "notification" : "data",
                                    NOTIFICATION_STATUS, "failure",
                                    ERROR_TYPE, e.getMessagingErrorCode().name()
                                ));
                                span.recordException(e);
                                span.setStatus(StatusCode.ERROR, e.getMessage());
                                LOGGER.warn("Firebase messaging error: {} - {}", 
                                        e.getMessagingErrorCode(), e.getMessage());
                                throw e;
                            }
                        }).get();
                    ).get();
                } catch (Exception e) {
                    if (e instanceof FirebaseMessagingException) {
                        throw new CompletionException("Firebase messaging error: " + 
                                ((FirebaseMessagingException) e).getMessagingErrorCode(), e);
                    } else {
                        throw new CompletionException("Firebase error: " + e.getMessage(), e);
                    }
                } finally {
                    span.end();
                }
            }));
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.end();
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public Health health() {
        if (!initialized.get()) {
            return Health.unknown().withDetail("message", "Firebase client not initialized").build();
        }

        if (healthy.get() && circuitBreaker.getState() == CircuitBreaker.State.CLOSED) {
            return Health.up()
                    .withDetail("circuitBreakerState", circuitBreaker.getState())
                    .withDetail("failureRate", circuitBreaker.getMetrics().getFailureRate())
                    .withDetail("successfulCalls", circuitBreaker.getMetrics().getNumberOfSuccessfulCalls())
                    .withDetail("failedCalls", circuitBreaker.getMetrics().getNumberOfFailedCalls())
                    .build();
        } else {
            return Health.down()
                    .withDetail("circuitBreakerState", circuitBreaker.getState())
                    .withDetail("failureRate", circuitBreaker.getMetrics().getFailureRate())
                    .withDetail("successfulCalls", circuitBreaker.getMetrics().getNumberOfSuccessfulCalls())
                    .withDetail("failedCalls", circuitBreaker.getMetrics().getNumberOfFailedCalls())
                    .build();
        }
    }

    public boolean isInitialized() {
        return initialized.get();
    }

    public CircuitBreaker.State getCircuitBreakerState() {
        return circuitBreaker != null ? circuitBreaker.getState() : null;
    }
}