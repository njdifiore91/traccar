/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.session.state;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Device;
import org.traccar.model.Event;

import java.io.Serializable;
import java.util.Date;

/**
 * Represents the overspeed state of a device.
 * This class is serializable for Redis compatibility in a distributed environment.
 */
public class OverspeedState implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private transient MessagePublisher messagePublisher;
    private transient MeterRegistry meterRegistry;
    
    /**
     * Creates an OverspeedState from a Device.
     *
     * @param device The device to extract overspeed state from
     * @return A new OverspeedState instance
     */
    public static OverspeedState fromDevice(Device device) {
        OverspeedState state = new OverspeedState();
        state.overspeedState = device.getOverspeedState();
        state.overspeedTime = device.getOverspeedTime();
        state.overspeedGeofenceId = device.getOverspeedGeofenceId();
        return state;
    }

    /**
     * Applies this state to a Device.
     *
     * @param device The device to update with this state
     */
    public void toDevice(Device device) {
        device.setOverspeedState(overspeedState);
        device.setOverspeedTime(overspeedTime);
        device.setOverspeedGeofenceId(overspeedGeofenceId);
    }

    /**
     * Sets the message publisher for state change events.
     *
     * @param messagePublisher The message publisher to use
     */
    public void setMessagePublisher(MessagePublisher messagePublisher) {
        this.messagePublisher = messagePublisher;
    }

    /**
     * Sets the meter registry for metrics collection.
     *
     * @param meterRegistry The meter registry to use
     */
    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    private boolean changed;

    /**
     * Checks if the state has changed.
     *
     * @return true if the state has changed, false otherwise
     */
    public boolean isChanged() {
        return changed;
    }

    private boolean overspeedState;

    /**
     * Gets the overspeed state.
     *
     * @return The overspeed state
     */
    public boolean getOverspeedState() {
        return overspeedState;
    }

    /**
     * Sets the overspeed state and marks the state as changed.
     * Also publishes a state change event and records metrics.
     *
     * @param overspeedState The new overspeed state
     */
    public void setOverspeedState(boolean overspeedState) {
        Timer.Sample sample = Timer.start();
        try {
            this.overspeedState = overspeedState;
            changed = true;
            version++;
            publishStateChange("overspeedState", overspeedState);
        } finally {
            if (meterRegistry != null) {
                sample.stop(meterRegistry.timer("overspeed.state.update"));
                meterRegistry.counter("overspeed.state.changes").increment();
            }
        }
    }

    private Date overspeedTime;

    /**
     * Gets the overspeed time.
     *
     * @return The overspeed time
     */
    public Date getOverspeedTime() {
        return overspeedTime;
    }

    /**
     * Sets the overspeed time and marks the state as changed.
     * Also publishes a state change event and records metrics.
     *
     * @param overspeedTime The new overspeed time
     */
    public void setOverspeedTime(Date overspeedTime) {
        Timer.Sample sample = Timer.start();
        try {
            this.overspeedTime = overspeedTime;
            changed = true;
            version++;
            publishStateChange("overspeedTime", overspeedTime);
        } finally {
            if (meterRegistry != null) {
                sample.stop(meterRegistry.timer("overspeed.time.update"));
                meterRegistry.counter("overspeed.time.changes").increment();
            }
        }
    }

    private long overspeedGeofenceId;

    /**
     * Gets the overspeed geofence ID.
     *
     * @return The overspeed geofence ID
     */
    public long getOverspeedGeofenceId() {
        return overspeedGeofenceId;
    }

    /**
     * Sets the overspeed geofence ID and marks the state as changed.
     * Also publishes a state change event and records metrics.
     *
     * @param overspeedGeofenceId The new overspeed geofence ID
     */
    public void setOverspeedGeofenceId(long overspeedGeofenceId) {
        Timer.Sample sample = Timer.start();
        try {
            this.overspeedGeofenceId = overspeedGeofenceId;
            changed = true;
            version++;
            publishStateChange("overspeedGeofenceId", overspeedGeofenceId);
        } finally {
            if (meterRegistry != null) {
                sample.stop(meterRegistry.timer("overspeed.geofence.update"));
                meterRegistry.counter("overspeed.geofence.changes").increment();
            }
        }
    }

    private transient Event event;

    /**
     * Gets the associated event.
     *
     * @return The event
     */
    public Event getEvent() {
        return event;
    }

    /**
     * Sets the associated event.
     *
     * @param event The new event
     */
    public void setEvent(Event event) {
        this.event = event;
    }
    
    private long version = 0;
    
    /**
     * Gets the current version of this state.
     * The version is incremented on each state change for conflict resolution.
     *
     * @return The current version
     */
    public long getVersion() {
        return version;
    }
    
    /**
     * Publishes a state change event to the message broker.
     *
     * @param property The property that changed
     * @param value The new value of the property
     */
    private void publishStateChange(String property, Object value) {
        if (messagePublisher != null) {
            try {
                StateChangeEvent stateChangeEvent = new StateChangeEvent(
                    "overspeedState", property, value, version);
                messagePublisher.publish("state.changes", stateChangeEvent);
            } catch (Exception e) {
                // Log the exception but don't rethrow to avoid disrupting the main flow
                System.err.println("Failed to publish state change: " + e.getMessage());
            }
        }
    }
    
    /**
     * Inner class representing a state change event to be published to the message broker.
     */
    public static class StateChangeEvent implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private final String stateType;
        private final String property;
        private final Object value;
        private final long version;
        
        public StateChangeEvent(String stateType, String property, Object value, long version) {
            this.stateType = stateType;
            this.property = property;
            this.value = value;
            this.version = version;
        }
        
        public String getStateType() {
            return stateType;
        }
        
        public String getProperty() {
            return property;
        }
        
        public Object getValue() {
            return value;
        }
        
        public long getVersion() {
            return version;
        }
    }
}