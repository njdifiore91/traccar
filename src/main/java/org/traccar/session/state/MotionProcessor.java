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
package org.traccar.session.state;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.reports.common.TripsConfig;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * MotionProcessor handles motion state updates for devices in a distributed environment.
 * It uses Redis for state synchronization across multiple instances and implements
 * circuit breaker pattern for resilience against Redis failures.
 */
@Singleton
public class MotionProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MotionProcessor.class);

    private final StateSynchronizer stateSynchronizer;
    private final CircuitBreaker circuitBreaker;
    private final MeterRegistry meterRegistry;

    /**
     * Constructs a new MotionProcessor with the required dependencies.
     *
     * @param stateSynchronizer For synchronizing state across instances
     * @param circuitBreakerRegistry For creating circuit breakers
     * @param meterRegistry For metrics collection
     */
    @Inject
    public MotionProcessor(
            StateSynchronizer stateSynchronizer,
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry) {
        this.stateSynchronizer = stateSynchronizer;
        this.meterRegistry = meterRegistry;

        // Configure circuit breaker for Redis operations
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider the last 10 calls
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .slowCallRateThreshold(50) // Open circuit when 50% of calls are slow
                .slowCallDurationThreshold(Duration.ofSeconds(1)) // Call is considered slow if it takes more than 1 second
                .build();

        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("motionProcessor", circuitBreakerConfig);
    }

    /**
     * Updates the motion state of a device based on the current position.
     * This method is resilient to Redis failures through circuit breaker pattern.
     *
     * @param state The current motion state
     * @param position The new position
     * @param newState The new motion state to set
     * @param tripsConfig Configuration for trip detection
     */
    public void updateState(
            MotionState state, Position position, boolean newState, TripsConfig tripsConfig) {

        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            // Execute the state update with circuit breaker protection
            Supplier<Void> stateUpdateSupplier = () -> {
                doUpdateState(state, position, newState, tripsConfig);
                return null;
            };

            circuitBreaker.executeSupplier(stateUpdateSupplier);

            // Record successful execution metrics
            sample.stop(meterRegistry.timer("motion.processor.update", 
                    "result", "success",
                    "deviceId", String.valueOf(position.getDeviceId())));

        } catch (Exception e) {
            // Record failure metrics
            sample.stop(meterRegistry.timer("motion.processor.update", 
                    "result", "failure",
                    "deviceId", String.valueOf(position.getDeviceId()),
                    "error", e.getClass().getSimpleName()));

            LOGGER.error("Error updating motion state for device {}: {}", 
                    position.getDeviceId(), e.getMessage());
            
            // Set the event to null to prevent further processing of this update
            state.setEvent(null);
        }
    }

    /**
     * Internal method that performs the actual state update logic.
     * This method is protected by the circuit breaker in the public method.
     */
    private void doUpdateState(
            MotionState state, Position position, boolean newState, TripsConfig tripsConfig) {

        state.setEvent(null);

        boolean oldState = state.getMotionState();
        if (oldState == newState) {
            if (state.getMotionTime() != null) {
                long oldTime = state.getMotionTime().getTime();
                long newTime = position.getFixTime().getTime();

                double distance = position.getDouble(Position.KEY_TOTAL_DISTANCE) - state.getMotionDistance();
                Boolean ignition = null;
                if (tripsConfig.getUseIgnition() && position.hasAttribute(Position.KEY_IGNITION)) {
                    ignition = position.getBoolean(Position.KEY_IGNITION);
                }

                boolean generateEvent = false;
                if (newState) {
                    if (newTime - oldTime >= tripsConfig.getMinimalTripDuration()
                            || distance >= tripsConfig.getMinimalTripDistance()) {
                        generateEvent = true;
                    }
                } else {
                    if (newTime - oldTime >= tripsConfig.getMinimalParkingDuration()
                            || ignition != null && !ignition) {
                        generateEvent = true;
                    }
                }

                if (generateEvent) {
                    String eventType = newState ? Event.TYPE_DEVICE_MOVING : Event.TYPE_DEVICE_STOPPED;
                    Event event = new Event(eventType, position);

                    state.setMotionStreak(newState);
                    state.setMotionTime(null);
                    state.setMotionDistance(0);
                    state.setEvent(event);

                    // Synchronize the state change across instances
                    synchronizeState(state, position.getDeviceId());
                }
            }
        } else {
            state.setMotionState(newState);
            if (state.getMotionStreak() == newState) {
                state.setMotionTime(null);
                state.setMotionDistance(0);
            } else {
                state.setMotionTime(position.getFixTime());
                state.setMotionDistance(position.getDouble(Position.KEY_TOTAL_DISTANCE));
            }

            // Synchronize the state change across instances
            synchronizeState(state, position.getDeviceId());
        }
    }

    /**
     * Synchronizes the motion state across multiple instances using Redis.
     * This method is protected by the circuit breaker in the public method.
     *
     * @param state The state to synchronize
     * @param deviceId The device ID
     */
    private void synchronizeState(MotionState state, long deviceId) {
        try {
            stateSynchronizer.synchronizeMotionState(state, deviceId);
            
            // Record successful synchronization
            meterRegistry.counter("motion.processor.sync", 
                    "result", "success",
                    "deviceId", String.valueOf(deviceId)).increment();
            
        } catch (Exception e) {
            // Record failed synchronization
            meterRegistry.counter("motion.processor.sync", 
                    "result", "failure",
                    "deviceId", String.valueOf(deviceId),
                    "error", e.getClass().getSimpleName()).increment();
            
            LOGGER.warn("Failed to synchronize motion state for device {}: {}", 
                    deviceId, e.getMessage());
            // We don't rethrow the exception as we want to continue processing even if sync fails
        }
    }
}