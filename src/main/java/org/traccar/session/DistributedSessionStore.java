/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.session;

/**
 * Interface for distributed session storage implementations.
 * Provides methods to store, retrieve, and remove device sessions across multiple instances.
 */
public interface DistributedSessionStore {

    /**
     * Store a device session in the distributed store.
     *
     * @param deviceId The device ID associated with the session
     * @param session The device session to store
     * @param ttlSeconds Time-to-live in seconds for the session
     */
    void putSession(long deviceId, DeviceSession session, long ttlSeconds);

    /**
     * Retrieve a device session from the distributed store.
     *
     * @param deviceId The device ID associated with the session
     * @return The device session, or null if not found
     */
    DeviceSession getSession(long deviceId);

    /**
     * Remove a device session from the distributed store.
     *
     * @param deviceId The device ID associated with the session
     */
    void removeSession(long deviceId);

    /**
     * Check if a session exists in the distributed store.
     *
     * @param deviceId The device ID to check
     * @return true if the session exists, false otherwise
     */
    boolean containsSession(long deviceId);

    /**
     * Update the expiration time for a session.
     *
     * @param deviceId The device ID associated with the session
     * @param ttlSeconds New time-to-live in seconds for the session
     * @return true if the session was found and updated, false otherwise
     */
    boolean updateSessionExpiration(long deviceId, long ttlSeconds);

    /**
     * Close the session store and release any resources.
     */
    void close();
}