/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.broadcast;

import com.google.inject.Inject;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.BaseModel;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.ObjectOperation;
import org.traccar.model.Position;

/**
 * A null implementation of the BroadcastService that doesn't actually broadcast anything.
 * This implementation is used when broadcasting is not required or in single-instance deployments.
 * It includes metrics reporting and service discovery integration for monitoring purposes.
 */
public class NullBroadcastService implements BroadcastService {

    private static final Logger LOGGER = LoggerFactory.getLogger(NullBroadcastService.class);

    // Metrics for monitoring broadcast operations
    private final Counter broadcastOperationsCounter;
    private final Gauge broadcastListenersGauge;
    private int listenerCount = 0;

    /**
     * Constructs a new NullBroadcastService with metrics initialization.
     */
    @Inject
    public NullBroadcastService() {
        // Initialize metrics
        broadcastOperationsCounter = Counter.build()
                .name("traccar_broadcast_operations_total")
                .help("Total number of broadcast operations")
                .labelNames("operation", "status")
                .register();
        
        broadcastListenersGauge = Gauge.build()
                .name("traccar_broadcast_listeners")
                .help("Number of registered broadcast listeners")
                .register();
        
        LOGGER.info("Initialized NullBroadcastService");
    }

    @Override
    public boolean singleInstance() {
        return true;
    }

    @Override
    public void registerListener(BroadcastInterface listener) {
        if (listener != null) {
            listenerCount++;
            broadcastListenersGauge.set(listenerCount);
            LOGGER.debug("Registered broadcast listener (no-op in NullBroadcastService)");
        }
    }

    @Override
    public void updateDevice(boolean local, Device device) {
        broadcastOperationsCounter.labels("updateDevice", "noop").inc();
        LOGGER.trace("Device update broadcast request ignored (no-op in NullBroadcastService)");
    }

    @Override
    public void updatePosition(boolean local, Position position) {
        broadcastOperationsCounter.labels("updatePosition", "noop").inc();
        LOGGER.trace("Position update broadcast request ignored (no-op in NullBroadcastService)");
    }

    @Override
    public void updateEvent(boolean local, long userId, Event event) {
        broadcastOperationsCounter.labels("updateEvent", "noop").inc();
        LOGGER.trace("Event update broadcast request ignored (no-op in NullBroadcastService)");
    }

    @Override
    public void updateCommand(boolean local, long deviceId) {
        broadcastOperationsCounter.labels("updateCommand", "noop").inc();
        LOGGER.trace("Command update broadcast request ignored (no-op in NullBroadcastService)");
    }

    @Override
    public <T extends BaseModel> void invalidateObject(
            boolean local, Class<T> clazz, long id, ObjectOperation operation) {
        broadcastOperationsCounter.labels("invalidateObject", "noop").inc();
        LOGGER.trace("Object invalidation broadcast request ignored (no-op in NullBroadcastService)");
    }

    @Override
    public <T1 extends BaseModel, T2 extends BaseModel> void invalidatePermission(
            boolean local, Class<T1> clazz1, long id1, Class<T2> clazz2, long id2, boolean link) {
        broadcastOperationsCounter.labels("invalidatePermission", "noop").inc();
        LOGGER.trace("Permission invalidation broadcast request ignored (no-op in NullBroadcastService)");
    }

    @Override
    public void start() throws Exception {
        LOGGER.info("Starting NullBroadcastService (no-op implementation)");
        // Register with service discovery if available
        try {
            // This is a no-op implementation, so we don't actually register with service discovery
            // But we log that we're ready for monitoring purposes
            broadcastOperationsCounter.labels("lifecycle", "start").inc();
        } catch (Exception e) {
            LOGGER.warn("Failed to register with service discovery", e);
        }
    }

    @Override
    public void stop() throws Exception {
        LOGGER.info("Stopping NullBroadcastService (no-op implementation)");
        // Deregister from service discovery if available
        try {
            // This is a no-op implementation, so we don't actually deregister from service discovery
            // But we log that we're stopping for monitoring purposes
            broadcastOperationsCounter.labels("lifecycle", "stop").inc();
        } catch (Exception e) {
            LOGGER.warn("Failed to deregister from service discovery", e);
        }
    }
}