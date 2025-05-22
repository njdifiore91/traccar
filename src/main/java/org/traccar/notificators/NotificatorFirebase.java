/*
 * Copyright 2018 - 2025 Anton Tananaev (anton@traccar.org)
 * Copyright 2018 Andrey Kunitsyn (andrey@traccar.org)
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

import com.google.firebase.messaging.AndroidConfig;
import com.google.firebase.messaging.AndroidNotification;
import com.google.firebase.messaging.ApnsConfig;
import com.google.firebase.messaging.Aps;
import com.google.firebase.messaging.MessagingErrorCode;
import com.google.firebase.messaging.MulticastMessage;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Event;
import org.traccar.model.ObjectOperation;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.CircuitBreakerManager;
import org.traccar.notification.DistributedTracingContext;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;
import org.traccar.notification.NotificationMetrics;
import org.traccar.push.FirebaseClient;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Firebase Cloud Messaging notificator.
 * Sends push notifications to mobile devices.
 */
@Singleton
public class NotificatorFirebase extends Notificator {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorFirebase.class);

    private final FirebaseClient firebaseClient;
    private final Storage storage;
    private final CacheManager cacheManager;
    private final CircuitBreakerManager circuitBreakerManager;
    private final DistributedTracingContext tracingContext;
    private final NotificationMetrics metrics;
    private final NotificatorRateLimiter rateLimiter;

    /**
     * Initialize the Firebase notificator.
     *
     * @param firebaseClient Firebase client for sending messages
     * @param notificationFormatter Formatter for notification messages
     * @param storage Storage for updating user tokens
     * @param cacheManager Cache manager for invalidating user objects
     * @param circuitBreakerManager Circuit breaker for Firebase API calls
     * @param tracingContext Distributed tracing context
     * @param metrics Metrics collector for notifications
     * @param rateLimiter Rate limiter for Firebase API
     * @throws IOException If initialization fails
     */
    @Inject
    public NotificatorFirebase(
            FirebaseClient firebaseClient, 
            NotificationFormatter notificationFormatter,
            Storage storage, 
            CacheManager cacheManager,
            CircuitBreakerManager circuitBreakerManager,
            DistributedTracingContext tracingContext,
            NotificationMetrics metrics,
            NotificatorRateLimiter rateLimiter) throws IOException {

        super(notificationFormatter, "short");
        this.firebaseClient = firebaseClient;
        this.storage = storage;
        this.cacheManager = cacheManager;
        this.circuitBreakerManager = circuitBreakerManager;
        this.tracingContext = tracingContext;
        this.metrics = metrics;
        this.rateLimiter = rateLimiter;
    }

    @Override
    public void send(User user, NotificationMessage message, Event event, Position position) throws MessageException {
        // Create a span for this notification operation
        try (var span = tracingContext.startSpan("firebase.notification")) {
            // Add user and event information to the span for tracing
            tracingContext.addSpanAttribute(span, "user.id", String.valueOf(user.getId()));
            if (event != null) {
                tracingContext.addSpanAttribute(span, "event.id", String.valueOf(event.getId()));
                tracingContext.addSpanAttribute(span, "event.type", event.getType());
            }

            // Check if user has notification tokens
            if (user.hasAttribute("notificationTokens")) {
                // Check rate limits before proceeding
                if (!rateLimiter.allowRequest("firebase", user.getId())) {
                    metrics.recordRateLimited("firebase");
                    tracingContext.addSpanAttribute(span, "rate_limited", "true");
                    LOGGER.warn("Firebase notification rate limited for user {}", user.getId());
                    return;
                }

                // Start metrics timer for Firebase operations
                metrics.startFirebaseOperationTimer();

                List<String> registrationTokens = new ArrayList<>(
                        Arrays.asList(user.getString("notificationTokens").split("[, ]")));

                // Add token count to span for observability
                tracingContext.addSpanAttribute(span, "token.count", String.valueOf(registrationTokens.size()));

                var messageBuilder = MulticastMessage.builder()
                        .setNotification(com.google.firebase.messaging.Notification.builder()
                                .setTitle(message.getSubject())
                                .setBody(message.getBody())
                                .build())
                        .setAndroidConfig(AndroidConfig.builder()
                                .setNotification(AndroidNotification.builder()
                                        .setSound("default")
                                        .build())
                                .build())
                        .setApnsConfig(ApnsConfig.builder()
                                .setAps(Aps.builder()
                                        .setSound("default")
                                        .build())
                                .build())
                        .addAllTokens(registrationTokens);

                // Add correlation ID for distributed tracing
                messageBuilder.putData("correlationId", tracingContext.getCorrelationId());

                if (event != null) {
                    messageBuilder.putData("eventId", String.valueOf(event.getId()));
                }

                try {
                    // Use circuit breaker to protect against Firebase API failures
                    var result = circuitBreakerManager.executeWithCircuitBreaker(
                            "firebase",
                            () -> firebaseClient.getInstance().sendEachForMulticast(messageBuilder.build()));

                    // Record successful operation in metrics
                    metrics.recordFirebaseOperationSuccess();

                    List<String> failedTokens = new LinkedList<>();
                    var iterator = result.getResponses().listIterator();
                    int successCount = 0;
                    int failureCount = 0;

                    while (iterator.hasNext()) {
                        int index = iterator.nextIndex();
                        var response = iterator.next();
                        if (!response.isSuccessful()) {
                            MessagingErrorCode error = response.getException().getMessagingErrorCode();
                            if (error == MessagingErrorCode.INVALID_ARGUMENT || error == MessagingErrorCode.UNREGISTERED) {
                                failedTokens.add(registrationTokens.get(index));
                            }
                            LOGGER.warn("Firebase user {} error", user.getId(), response.getException());
                            failureCount++;
                            // Record specific error type in metrics
                            metrics.recordFirebaseError(error.name());
                        } else {
                            successCount++;
                        }
                    }

                    // Add success/failure counts to span
                    tracingContext.addSpanAttribute(span, "success.count", String.valueOf(successCount));
                    tracingContext.addSpanAttribute(span, "failure.count", String.valueOf(failureCount));

                    if (!failedTokens.isEmpty()) {
                        // Record token cleanup in metrics
                        metrics.recordTokenCleanup(failedTokens.size());
                        tracingContext.addSpanAttribute(span, "tokens.removed", String.valueOf(failedTokens.size()));

                        registrationTokens.removeAll(failedTokens);
                        if (registrationTokens.isEmpty()) {
                            user.removeAttribute("notificationTokens");
                        } else {
                            user.set("notificationTokens", String.join(",", registrationTokens));
                        }

                        // Update user in storage and invalidate cache
                        storage.updateObject(user, new Request(
                                new Columns.Include("attributes"),
                                new Condition.Equals("id", user.getId())));
                        cacheManager.invalidateObject(true, User.class, user.getId(), ObjectOperation.UPDATE);
                    }
                } catch (Exception e) {
                    // Record failure in metrics
                    metrics.recordFirebaseOperationFailure(e.getClass().getSimpleName());
                    tracingContext.recordException(span, e);
                    LOGGER.warn("Firebase error", e);
                } finally {
                    // Stop metrics timer regardless of outcome
                    metrics.stopFirebaseOperationTimer();
                }
            } else {
                // Record skipped notification due to missing tokens
                metrics.recordSkippedNotification("firebase", "no_tokens");
                tracingContext.addSpanAttribute(span, "skipped", "no_tokens");
            }
        }
    }
}