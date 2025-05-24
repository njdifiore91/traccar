/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.service;

/**
 * Service interface for managing WebSocket sessions and delivering notifications
 * to connected web clients.
 */
public interface WebSocketService {

    /**
     * Checks if a user has an active WebSocket session.
     *
     * @param userId The ID of the user to check
     * @return true if the user has at least one active session, false otherwise
     */
    boolean hasActiveSession(long userId);

    /**
     * Sends a notification to a specific user through their WebSocket connection(s).
     *
     * @param userId The ID of the user to send the notification to
     * @param notification The formatted notification message as a JSON string
     * @return true if the notification was sent successfully to at least one session, false otherwise
     */
    boolean sendNotification(long userId, String notification);

    /**
     * Gets the count of active WebSocket sessions for a specific user.
     *
     * @param userId The ID of the user to check
     * @return The number of active sessions for the user
     */
    int getActiveSessionCount(long userId);

    /**
     * Gets the total count of active WebSocket sessions across all users.
     *
     * @return The total number of active WebSocket sessions
     */
    int getTotalActiveSessionCount();

    /**
     * Broadcasts a notification to all connected WebSocket sessions.
     *
     * @param notification The formatted notification message as a JSON string
     * @return The number of sessions the notification was sent to
     */
    int broadcastNotification(String notification);
}