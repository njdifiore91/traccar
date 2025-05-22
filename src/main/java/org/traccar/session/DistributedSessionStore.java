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

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Interface for distributed session storage operations.
 * Provides methods for storing and retrieving device sessions in a distributed environment,
 * abstracting the underlying storage mechanism (Redis) and providing a consistent API
 * for session management across multiple service instances.
 */
public interface DistributedSessionStore {

    /**
     * Retrieves a device session by device ID.
     *
     * @param deviceId The unique identifier of the device
     * @return The device session if found, or null if not found
     */
    DeviceSession getDeviceSession(long deviceId);

    /**
     * Stores a device session with the specified device ID.
     *
     * @param deviceId The unique identifier of the device
     * @param session The device session to store
     */
    void setDeviceSession(long deviceId, DeviceSession session);

    /**
     * Removes a device session with the specified device ID.
     *
     * @param deviceId The unique identifier of the device
     * @return true if the session was removed, false if it didn't exist
     */
    boolean removeDeviceSession(long deviceId);

    /**
     * Retrieves all device sessions associated with a specific connection key.
     *
     * @param connectionKey The connection key (channel and remote address)
     * @return A map of unique IDs to device sessions for the specified connection key
     */
    Map<String, DeviceSession> getDeviceSessionsByConnectionKey(ConnectionKey connectionKey);

    /**
     * Stores a device session with the specified connection key and unique ID.
     *
     * @param connectionKey The connection key (channel and remote address)
     * @param uniqueId The unique identifier of the device
     * @param session The device session to store
     */
    void setDeviceSessionByConnectionKey(ConnectionKey connectionKey, String uniqueId, DeviceSession session);

    /**
     * Removes a device session with the specified connection key and unique ID.
     *
     * @param connectionKey The connection key (channel and remote address)
     * @param uniqueId The unique identifier of the device
     * @return true if the session was removed, false if it didn't exist
     */
    boolean removeDeviceSessionByConnectionKey(ConnectionKey connectionKey, String uniqueId);

    /**
     * Removes all device sessions associated with a specific connection key.
     *
     * @param connectionKey The connection key (channel and remote address)
     * @return true if any sessions were removed, false otherwise
     */
    boolean removeAllDeviceSessionsByConnectionKey(ConnectionKey connectionKey);

    /**
     * Sets the time-to-live (TTL) for a device session.
     *
     * @param deviceId The unique identifier of the device
     * @param ttl The time-to-live value
     * @param unit The time unit for the TTL value
     */
    void setSessionTTL(long deviceId, long ttl, TimeUnit unit);

    /**
     * Gets the remaining time-to-live (TTL) for a device session.
     *
     * @param deviceId The unique identifier of the device
     * @return The remaining TTL in milliseconds, or -1 if the session doesn't exist or has no TTL
     */
    long getSessionTTL(long deviceId);

    /**
     * Atomically updates a device session using the provided update function.
     * This ensures that updates to the session are performed atomically in a distributed environment.
     *
     * @param deviceId The unique identifier of the device
     * @param updateFunction A function that takes the current session and returns the updated session
     * @return The updated device session, or null if the session doesn't exist
     */
    DeviceSession atomicUpdateDeviceSession(long deviceId, Function<DeviceSession, DeviceSession> updateFunction);

    /**
     * Retrieves all device sessions currently stored in the distributed session store.
     *
     * @return A map of device IDs to device sessions
     */
    Map<Long, DeviceSession> getAllDeviceSessions();

    /**
     * Bulk updates multiple device sessions at once.
     * This is more efficient than updating sessions individually when many sessions need to be updated.
     *
     * @param sessions A map of device IDs to device sessions to update
     */
    void bulkUpdateDeviceSessions(Map<Long, DeviceSession> sessions);

    /**
     * Checks if a device session exists for the specified device ID.
     *
     * @param deviceId The unique identifier of the device
     * @return true if a session exists, false otherwise
     */
    boolean hasDeviceSession(long deviceId);

    /**
     * Gets the total number of device sessions currently stored.
     *
     * @return The count of device sessions
     */
    long getSessionCount();

    /**
     * Clears all device sessions from the store.
     * This should be used with caution, typically only during testing or system reset.
     */
    void clearAllSessions();
}