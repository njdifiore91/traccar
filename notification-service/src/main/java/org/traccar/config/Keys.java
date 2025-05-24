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
package org.traccar.config;

import java.util.List;

public final class Keys {

    private Keys() {
    }

    /**
     * Enabled notification options. Comma-separated string is expected.
     * Example: web,mail,sms
     */
    public static final ConfigKey<String> NOTIFICATOR_TYPES = new StringConfigKey(
            "notificator.types",
            List.of(KeyType.CONFIG),
            "web,mail,command");

    /**
     * If the event time is too old, we should not send notifications. This parameter is the threshold value in
     * milliseconds. Default value is 15 minutes.
     */
    public static final ConfigKey<Long> NOTIFICATOR_TIME_THRESHOLD = new LongConfigKey(
            "notificator.timeThreshold",
            List.of(KeyType.CONFIG),
            15 * 60 * 1000L);

    /**
     * Traccar notification API key.
     */
    public static final ConfigKey<String> NOTIFICATOR_TRACCAR_KEY = new StringConfigKey(
            "notificator.traccar.key",
            List.of(KeyType.CONFIG));

    /**
     * Firebase service account JSON.
     */
    public static final ConfigKey<String> NOTIFICATOR_FIREBASE_SERVICE_ACCOUNT = new StringConfigKey(
            "notificator.firebase.serviceAccount",
            List.of(KeyType.CONFIG));

    /**
     * Pushover notification user name.
     */
    public static final ConfigKey<String> NOTIFICATOR_PUSHOVER_USER = new StringConfigKey(
            "notificator.pushover.user",
            List.of(KeyType.CONFIG));

    /**
     * Pushover notification user token.
     */
    public static final ConfigKey<String> NOTIFICATOR_PUSHOVER_TOKEN = new StringConfigKey(
            "notificator.pushover.token",
            List.of(KeyType.CONFIG));

    /**
     * Telegram notification API key.
     */
    public static final ConfigKey<String> NOTIFICATOR_TELEGRAM_KEY = new StringConfigKey(
            "notificator.telegram.key",
            List.of(KeyType.CONFIG));

    /**
     * Telegram notification chat id to post messages to.
     */
    public static final ConfigKey<String> NOTIFICATOR_TELEGRAM_CHAT_ID = new StringConfigKey(
            "notificator.telegram.chatId",
            List.of(KeyType.CONFIG));

    /**
     * Telegram notification send location message.
     */
    public static final ConfigKey<Boolean> NOTIFICATOR_TELEGRAM_SEND_LOCATION = new BooleanConfigKey(
            "notificator.telegram.sendLocation",
            List.of(KeyType.CONFIG));

    /**
     * Enable user expiration email notification.
     */
    public static final ConfigKey<Boolean> NOTIFICATION_EXPIRATION_USER = new BooleanConfigKey(
            "notification.expiration.user",
            List.of(KeyType.CONFIG));

    /**
     * User expiration reminder. Value in milliseconds.
     */
    public static final ConfigKey<Long> NOTIFICATION_EXPIRATION_USER_REMINDER = new LongConfigKey(
            "notification.expiration.user.reminder",
            List.of(KeyType.CONFIG));

    /**
     * Enable device expiration email notification.
     */
    public static final ConfigKey<Boolean> NOTIFICATION_EXPIRATION_DEVICE = new BooleanConfigKey(
            "notification.expiration.device",
            List.of(KeyType.CONFIG));

    /**
     * Device expiration reminder. Value in milliseconds.
     */
    public static final ConfigKey<Long> NOTIFICATION_EXPIRATION_DEVICE_REMINDER = new LongConfigKey(
            "notification.expiration.device.reminder",
            List.of(KeyType.CONFIG));

    /**
     * Block notifications for specific users. The value should be a comma-separated list of internal user ids.
     */
    public static final ConfigKey<String> NOTIFICATION_BLOCK_USERS = new StringConfigKey(
            "notification.block.users",
            List.of(KeyType.CONFIG));

    /**
     * Circuit breaker failure rate threshold. Default value is 50%.
     */
    public static final ConfigKey<Float> NOTIFICATION_CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD = new FloatConfigKey(
            "notification.circuitBreaker.failureRateThreshold",
            List.of(KeyType.CONFIG),
            50.0f);

    /**
     * Circuit breaker slow call rate threshold. Default value is 50%.
     */
    public static final ConfigKey<Float> NOTIFICATION_CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD = new FloatConfigKey(
            "notification.circuitBreaker.slowCallRateThreshold",
            List.of(KeyType.CONFIG),
            50.0f);

    /**
     * Circuit breaker slow call duration threshold in milliseconds. Default value is 2000ms.
     */
    public static final ConfigKey<Long> NOTIFICATION_CIRCUIT_BREAKER_SLOW_CALL_DURATION_THRESHOLD = new LongConfigKey(
            "notification.circuitBreaker.slowCallDurationThreshold",
            List.of(KeyType.CONFIG),
            2000L);

    /**
     * Circuit breaker permitted number of calls in half-open state. Default value is 10.
     */
    public static final ConfigKey<Integer> NOTIFICATION_CIRCUIT_BREAKER_PERMITTED_CALLS_IN_HALF_OPEN = new IntegerConfigKey(
            "notification.circuitBreaker.permittedCallsInHalfOpen",
            List.of(KeyType.CONFIG),
            10);

    /**
     * Circuit breaker minimum number of calls before calculating failure rate. Default value is 10.
     */
    public static final ConfigKey<Integer> NOTIFICATION_CIRCUIT_BREAKER_MINIMUM_CALLS = new IntegerConfigKey(
            "notification.circuitBreaker.minimumCalls",
            List.of(KeyType.CONFIG),
            10);

    /**
     * Circuit breaker sliding window size. Default value is 100.
     */
    public static final ConfigKey<Integer> NOTIFICATION_CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE = new IntegerConfigKey(
            "notification.circuitBreaker.slidingWindowSize",
            List.of(KeyType.CONFIG),
            100);

    /**
     * Circuit breaker wait duration in open state in seconds. Default value is 60 seconds.
     */
    public static final ConfigKey<Long> NOTIFICATION_CIRCUIT_BREAKER_WAIT_DURATION_IN_OPEN_STATE = new LongConfigKey(
            "notification.circuitBreaker.waitDurationInOpenState",
            List.of(KeyType.CONFIG),
            60L);

    /**
     * Firebase-specific circuit breaker slow call threshold in milliseconds. Default value is 3000ms.
     */
    public static final ConfigKey<Long> NOTIFICATION_CIRCUIT_BREAKER_FIREBASE_SLOW_CALL_THRESHOLD = new LongConfigKey(
            "notification.circuitBreaker.firebase.slowCallThreshold",
            List.of(KeyType.CONFIG),
            3000L);

    /**
     * Mail-specific circuit breaker slow call threshold in milliseconds. Default value is 5000ms.
     */
    public static final ConfigKey<Long> NOTIFICATION_CIRCUIT_BREAKER_MAIL_SLOW_CALL_THRESHOLD = new LongConfigKey(
            "notification.circuitBreaker.mail.slowCallThreshold",
            List.of(KeyType.CONFIG),
            5000L);

    /**
     * Mail-specific circuit breaker failure rate threshold. Default value is 70%.
     */
    public static final ConfigKey<Float> NOTIFICATION_CIRCUIT_BREAKER_MAIL_FAILURE_RATE_THRESHOLD = new FloatConfigKey(
            "notification.circuitBreaker.mail.failureRateThreshold",
            List.of(KeyType.CONFIG),
            70.0f);

    /**
     * SMS-specific circuit breaker slow call threshold in milliseconds. Default value is 3000ms.
     */
    public static final ConfigKey<Long> NOTIFICATION_CIRCUIT_BREAKER_SMS_SLOW_CALL_THRESHOLD = new LongConfigKey(
            "notification.circuitBreaker.sms.slowCallThreshold",
            List.of(KeyType.CONFIG),
            3000L);

    /**
     * API-specific circuit breaker slow call threshold in milliseconds. Default value is 2000ms.
     */
    public static final ConfigKey<Long> NOTIFICATION_CIRCUIT_BREAKER_API_SLOW_CALL_THRESHOLD = new LongConfigKey(
            "notification.circuitBreaker.api.slowCallThreshold",
            List.of(KeyType.CONFIG),
            2000L);

    /**
     * Store notifications for retry when circuit breaker is open. Default value is true.
     */
    public static final ConfigKey<Boolean> NOTIFICATION_CIRCUIT_BREAKER_STORE_FOR_RETRY = new BooleanConfigKey(
            "notification.circuitBreaker.storeForRetry",
            List.of(KeyType.CONFIG),
            true);

    /**
     * Use alternative channel when circuit breaker is open. Default value is false.
     */
    public static final ConfigKey<Boolean> NOTIFICATION_CIRCUIT_BREAKER_USE_ALTERNATIVE_CHANNEL = new BooleanConfigKey(
            "notification.circuitBreaker.useAlternativeChannel",
            List.of(KeyType.CONFIG),
            false);

    /**
     * Alternative channel to use when circuit breaker is open.
     */
    public static final ConfigKey<String> NOTIFICATION_CIRCUIT_BREAKER_ALTERNATIVE_CHANNEL = new StringConfigKey(
            "notification.circuitBreaker.alternativeChannel",
            List.of(KeyType.CONFIG));
}