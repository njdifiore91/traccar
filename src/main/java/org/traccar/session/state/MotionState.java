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

import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.messaging.MessageProducer;
import org.traccar.metrics.MetricsRegistry;

import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents the motion state of a device.
 * This class is serializable for Redis compatibility and includes versioning
 * for conflict resolution in distributed environments.
 */
public class MotionState implements Serializable {

    private static final long serialVersionUID = 1L;
    
    // Transient fields are not serialized
    private transient MessageProducer messageProducer;
    private transient MetricsRegistry metricsRegistry;
    
    // Version for optimistic locking and conflict resolution
    private long version = 0;
    
    // Metrics counters
    private static final AtomicLong stateChangeCounter = new AtomicLong(0);
    
    /**
     * Creates a MotionState from a Device object.
     *
     * @param device The device to extract motion state from
     * @return A new MotionState object
     */
    public static MotionState fromDevice(Device device) {
        MotionState state = new MotionState();
        state.motionStreak = device.getMotionStreak();
        state.motionState = device.getMotionState();
        state.motionTime = device.getMotionTime();
        state.motionDistance = device.getMotionDistance();
        return state;
    }

    /**
     * Updates a Device object with this motion state.
     *
     * @param device The device to update
     */
    public void toDevice(Device device) {
        device.setMotionStreak(motionStreak);
        device.setMotionState(motionState);
        device.setMotionTime(motionTime);
        device.setMotionDistance(motionDistance);
    }
    
    /**
     * Sets the message producer for publishing state changes.
     *
     * @param messageProducer The message producer to use
     * @return This MotionState instance for method chaining
     */
    public MotionState withMessageProducer(MessageProducer messageProducer) {
        this.messageProducer = messageProducer;
        return this;
    }
    
    /**
     * Sets the metrics registry for collecting metrics.
     *
     * @param metricsRegistry The metrics registry to use
     * @return This MotionState instance for method chaining
     */
    public MotionState withMetricsRegistry(MetricsRegistry metricsRegistry) {
        this.metricsRegistry = metricsRegistry;
        return this;
    }
    
    /**
     * Gets the current version of this state object.
     * Used for optimistic locking in distributed environments.
     *
     * @return The current version
     */
    public long getVersion() {
        return version;
    }
    
    /**
     * Sets the version of this state object.
     * Used during deserialization and conflict resolution.
     *
     * @param version The new version
     */
    public void setVersion(long version) {
        this.version = version;
    }
    
    /**
     * Increments the version of this state object.
     * Called internally when state changes occur.
     */
    private void incrementVersion() {
        this.version++;
    }
    
    /**
     * Publishes a state change event to the message broker.
     * 
     * @param property The property that changed
     * @param value The new value
     */
    private void publishStateChange(String property, Object value) {
        if (messageProducer != null) {
            try {
                messageProducer.publishStateChange("motion", property, value, version);
                
                // Record metric for successful state change publication
                if (metricsRegistry != null) {
                    metricsRegistry.incrementCounter("motion.state.changes");
                }
                
                // Global counter for all state changes (static for all instances)
                stateChangeCounter.incrementAndGet();
                
            } catch (Exception e) {
                // Log the exception but don't rethrow to avoid disrupting application flow
                if (metricsRegistry != null) {
                    metricsRegistry.incrementCounter("motion.state.changes.failed");
                }
            }
        }
    }

    private boolean changed;

    /**
     * Checks if the state has been changed since last reset.
     *
     * @return true if the state has changed, false otherwise
     */
    public boolean isChanged() {
        return changed;
    }
    
    /**
     * Resets the changed flag.
     * Useful after state has been persisted or synchronized.
     */
    public void resetChanged() {
        changed = false;
    }

    private boolean motionStreak;

    /**
     * Gets the motion streak state.
     *
     * @return The current motion streak state
     */
    public boolean getMotionStreak() {
        return motionStreak;
    }

    /**
     * Sets the motion streak state and publishes the change.
     *
     * @param motionStreak The new motion streak state
     */
    public void setMotionStreak(boolean motionStreak) {
        if (this.motionStreak != motionStreak) {
            this.motionStreak = motionStreak;
            changed = true;
            incrementVersion();
            publishStateChange("motionStreak", motionStreak);
            
            // Record metric for specific property change
            if (metricsRegistry != null) {
                metricsRegistry.incrementCounter("motion.state.streak.changes");
            }
        }
    }

    private boolean motionState;

    /**
     * Gets the motion state.
     *
     * @return The current motion state
     */
    public boolean getMotionState() {
        return motionState;
    }

    /**
     * Sets the motion state and publishes the change.
     *
     * @param motionState The new motion state
     */
    public void setMotionState(boolean motionState) {
        if (this.motionState != motionState) {
            this.motionState = motionState;
            changed = true;
            incrementVersion();
            publishStateChange("motionState", motionState);
            
            // Record metric for specific property change
            if (metricsRegistry != null) {
                metricsRegistry.incrementCounter("motion.state.state.changes");
                if (motionState) {
                    metricsRegistry.incrementCounter("motion.state.active");
                } else {
                    metricsRegistry.incrementCounter("motion.state.inactive");
                }
            }
        }
    }

    private Date motionTime;

    /**
     * Gets the motion time.
     *
     * @return The current motion time
     */
    public Date getMotionTime() {
        return motionTime;
    }

    /**
     * Sets the motion time and publishes the change.
     *
     * @param motionTime The new motion time
     */
    public void setMotionTime(Date motionTime) {
        // For Date objects, we need to check for null and equality
        if ((this.motionTime == null && motionTime != null) ||
            (this.motionTime != null && !this.motionTime.equals(motionTime))) {
            this.motionTime = motionTime;
            changed = true;
            incrementVersion();
            publishStateChange("motionTime", motionTime);
            
            // Record metric for specific property change
            if (metricsRegistry != null) {
                metricsRegistry.incrementCounter("motion.state.time.changes");
            }
        }
    }

    private double motionDistance;

    /**
     * Gets the motion distance.
     *
     * @return The current motion distance
     */
    public double getMotionDistance() {
        return motionDistance;
    }

    /**
     * Sets the motion distance and publishes the change.
     *
     * @param motionDistance The new motion distance
     */
    public void setMotionDistance(double motionDistance) {
        if (this.motionDistance != motionDistance) {
            this.motionDistance = motionDistance;
            changed = true;
            incrementVersion();
            publishStateChange("motionDistance", motionDistance);
            
            // Record metric for specific property change
            if (metricsRegistry != null) {
                metricsRegistry.incrementCounter("motion.state.distance.changes");
                metricsRegistry.recordValue("motion.state.distance.value", motionDistance);
            }
        }
    }

    private Event event;

    /**
     * Gets the associated event.
     *
     * @return The current event
     */
    public Event getEvent() {
        return event;
    }

    /**
     * Sets the associated event.
     * Note: This does not trigger state change notification as it's considered internal.
     *
     * @param event The new event
     */
    public void setEvent(Event event) {
        this.event = event;
    }

    /**
     * Custom serialization method to handle transient fields and optimize serialization.
     * This method is called during serialization to Redis or other storage.
     *
     * @param out The object output stream
     * @throws java.io.IOException If an I/O error occurs
     */
    private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
        // Record metric for serialization operations
        if (metricsRegistry != null) {
            metricsRegistry.incrementCounter("motion.state.serializations");
        }
        
        // Default serialization for most fields
        out.defaultWriteObject();
    }
    
    /**
     * Custom deserialization method to handle transient fields and versioning.
     * This method is called during deserialization from Redis or other storage.
     *
     * @param in The object input stream
     * @throws java.io.IOException If an I/O error occurs
     * @throws ClassNotFoundException If the class of a serialized object cannot be found
     */
    private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
        // Default deserialization for most fields
        in.defaultReadObject();
        
        // Record metric for deserialization operations
        if (metricsRegistry != null) {
            metricsRegistry.incrementCounter("motion.state.deserializations");
        }
    }
    
    /**
     * Gets the total number of state changes across all instances.
     * This is useful for monitoring and metrics collection.
     *
     * @return The total number of state changes
     */
    public static long getTotalStateChanges() {
        return stateChangeCounter.get();
    }
}