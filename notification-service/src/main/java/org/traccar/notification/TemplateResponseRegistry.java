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
package org.traccar.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for tracking template response futures.
 * This class manages the CompletableFuture objects used for asynchronous template retrieval.
 */
public class TemplateResponseRegistry {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateResponseRegistry.class);
    private static final Map<String, CompletableFuture<String>> FUTURES = new ConcurrentHashMap<>();

    private TemplateResponseRegistry() {
        // Utility class, prevent instantiation
    }

    /**
     * Creates a new CompletableFuture for the given request ID and stores it in the registry.
     *
     * @param requestId The request ID to associate with the future
     * @return The created CompletableFuture
     */
    public static CompletableFuture<String> createFuture(String requestId) {
        CompletableFuture<String> future = new CompletableFuture<>();
        FUTURES.put(requestId, future);
        return future;
    }

    /**
     * Completes the future associated with the given request ID with the provided content.
     *
     * @param requestId The request ID associated with the future
     * @param content The content to complete the future with
     */
    public static void completeFuture(String requestId, String content) {
        CompletableFuture<String> future = FUTURES.remove(requestId);
        if (future != null) {
            future.complete(content);
        } else {
            LOGGER.warn("No future found for request ID: {}", requestId);
        }
    }

    /**
     * Removes the future associated with the given request ID from the registry.
     *
     * @param requestId The request ID associated with the future to remove
     */
    public static void removeFuture(String requestId) {
        CompletableFuture<String> future = FUTURES.remove(requestId);
        if (future != null && !future.isDone()) {
            future.completeExceptionally(new RuntimeException("Future removed from registry"));
        }
    }

    /**
     * Completes the future associated with the given request ID exceptionally with the provided exception.
     *
     * @param requestId The request ID associated with the future
     * @param exception The exception to complete the future with
     */
    public static void completeExceptionally(String requestId, Throwable exception) {
        CompletableFuture<String> future = FUTURES.remove(requestId);
        if (future != null) {
            future.completeExceptionally(exception);
        } else {
            LOGGER.warn("No future found for request ID: {}", requestId);
        }
    }

    /**
     * Gets the number of pending futures in the registry.
     *
     * @return The number of pending futures
     */
    public static int getPendingCount() {
        return FUTURES.size();
    }

    /**
     * Clears all futures from the registry, completing them exceptionally.
     */
    public static void clearAll() {
        FUTURES.forEach((requestId, future) -> {
            if (!future.isDone()) {
                future.completeExceptionally(new RuntimeException("Registry cleared"));
            }
        });
        FUTURES.clear();
    }
}