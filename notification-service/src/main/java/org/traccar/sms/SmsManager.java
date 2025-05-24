/*
 * Copyright 2018 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.sms;

import org.traccar.notification.MessageException;

import java.util.concurrent.CompletableFuture;

/**
 * SmsManager interface defines the contract for SMS delivery services within the Notification Service.
 * It provides both synchronous and asynchronous methods for sending SMS messages through various providers.
 */
public interface SmsManager {

    /**
     * Sends an SMS message synchronously.
     * This method is maintained for backward compatibility with existing implementations.
     *
     * @param phone   The recipient's phone number in international format
     * @param message The text message to send
     * @param command Flag indicating if this is a command message (may affect delivery priority)
     * @throws MessageException If the message cannot be sent
     */
    void sendMessage(String phone, String message, boolean command) throws MessageException;

    /**
     * Sends an SMS message asynchronously with support for distributed tracing.
     *
     * @param phone        The recipient's phone number in international format
     * @param message      The text message to send
     * @param command      Flag indicating if this is a command message (may affect delivery priority)
     * @param correlationId Unique identifier for tracing this request across services
     * @return A CompletableFuture that completes when the message is sent or fails with a MessageException
     */
    default CompletableFuture<Void> sendMessageAsync(String phone, String message, boolean command, String correlationId) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        try {
            sendMessage(phone, message, command);
            future.complete(null);
        } catch (MessageException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    /**
     * Sends an SMS message asynchronously without correlation ID.
     * This is a convenience method that generates a random correlation ID.
     *
     * @param phone   The recipient's phone number in international format
     * @param message The text message to send
     * @param command Flag indicating if this is a command message (may affect delivery priority)
     * @return A CompletableFuture that completes when the message is sent or fails with a MessageException
     */
    default CompletableFuture<Void> sendMessageAsync(String phone, String message, boolean command) {
        // Generate a random correlation ID for tracing
        String correlationId = java.util.UUID.randomUUID().toString();
        return sendMessageAsync(phone, message, command, correlationId);
    }

    /**
     * Checks if the SMS provider is properly configured and available.
     *
     * @return true if the SMS provider is available, false otherwise
     */
    default boolean isConfigured() {
        return true; // Default implementation assumes configured
    }
}