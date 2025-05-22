/*
 * Copyright 2017 - 2023 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.reports.common;

import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;

/**
 * Configuration class for trip detection parameters.
 * Supports dynamic updates, validation, and metrics collection.
 */
public class TripsConfig {

    private static final Logger LOGGER = Logger.getLogger(TripsConfig.class.getName());

    private static final double MIN_TRIP_DISTANCE = 0.01; // Minimum valid trip distance in km
    private static final long MIN_TRIP_DURATION = 1000; // Minimum valid trip duration in ms
    private static final long MIN_PARKING_DURATION = 1000; // Minimum valid parking duration in ms
    private static final long MIN_NO_DATA_DURATION = 0; // Minimum valid no data duration in ms

    private final AttributeUtil.Provider attributeProvider;
    private final ConcurrentHashMap<String, Consumer<TripsConfig>> updateListeners = new ConcurrentHashMap<>();
    
    // Cache for configuration values
    private final AtomicReference<ConfigCache> configCache = new AtomicReference<>(null);
    
    // Metrics
    private LongCounter tripDetectionCounter;
    private LongCounter invalidConfigCounter;
    
    /**
     * Constructs a new TripsConfig with the given attribute provider.
     * 
     * @param attributeProvider The provider for configuration attributes
     */
    public TripsConfig(AttributeUtil.Provider attributeProvider) {
        this.attributeProvider = attributeProvider;
        initMetrics();
        refreshCache();
    }

    /**
     * Constructs a new TripsConfig with explicit parameter values.
     * 
     * @param minimalTripDistance Minimum distance for a trip in kilometers
     * @param minimalTripDuration Minimum duration for a trip in milliseconds
     * @param minimalParkingDuration Minimum duration for parking in milliseconds
     * @param minimalNoDataDuration Minimum duration for no data in milliseconds
     * @param useIgnition Whether to use ignition for trip detection
     * @param ignoreOdometer Whether to ignore odometer for trip detection
     */
    public TripsConfig(
            double minimalTripDistance, long minimalTripDuration, long minimalParkingDuration,
            long minimalNoDataDuration, boolean useIgnition, boolean ignoreOdometer) {
        this.attributeProvider = null;
        validateParameters(minimalTripDistance, minimalTripDuration, minimalParkingDuration, minimalNoDataDuration);
        initMetrics();
        
        ConfigCache cache = new ConfigCache();
        cache.minimalTripDistance = minimalTripDistance;
        cache.minimalTripDuration = minimalTripDuration;
        cache.minimalParkingDuration = minimalParkingDuration;
        cache.minimalNoDataDuration = minimalNoDataDuration;
        cache.useIgnition = useIgnition;
        cache.ignoreOdometer = ignoreOdometer;
        cache.timestamp = System.currentTimeMillis();
        configCache.set(cache);
    }
    
    /**
     * Initialize metrics for trip detection performance monitoring.
     */
    private void initMetrics() {
        try {
            MeterProvider meterProvider = io.opentelemetry.api.GlobalOpenTelemetry.getMeterProvider();
            Meter meter = meterProvider.get("org.traccar.reports");
            
            tripDetectionCounter = meter.counterBuilder("trip_detection_count")
                    .setDescription("Number of trip detections performed")
                    .build();
            
            invalidConfigCounter = meter.counterBuilder("trip_config_invalid_count")
                    .setDescription("Number of invalid trip configuration attempts")
                    .build();
        } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Failed to initialize metrics for TripsConfig", e);
        }
    }
    
    /**
     * Validates the trip configuration parameters.
     * 
     * @param minimalTripDistance Minimum distance for a trip
     * @param minimalTripDuration Minimum duration for a trip
     * @param minimalParkingDuration Minimum duration for parking
     * @param minimalNoDataDuration Minimum duration for no data
     * @throws IllegalArgumentException if any parameter is invalid
     */
    private void validateParameters(
            double minimalTripDistance, long minimalTripDuration, 
            long minimalParkingDuration, long minimalNoDataDuration) {
        
        StringBuilder errors = new StringBuilder();
        
        if (minimalTripDistance < MIN_TRIP_DISTANCE) {
            errors.append("Minimal trip distance must be at least ").append(MIN_TRIP_DISTANCE).append(" km. ");
        }
        
        if (minimalTripDuration < MIN_TRIP_DURATION) {
            errors.append("Minimal trip duration must be at least ").append(MIN_TRIP_DURATION).append(" ms. ");
        }
        
        if (minimalParkingDuration < MIN_PARKING_DURATION) {
            errors.append("Minimal parking duration must be at least ").append(MIN_PARKING_DURATION).append(" ms. ");
        }
        
        if (minimalNoDataDuration < MIN_NO_DATA_DURATION) {
            errors.append("Minimal no data duration must be at least ").append(MIN_NO_DATA_DURATION).append(" ms. ");
        }
        
        if (errors.length() > 0) {
            if (invalidConfigCounter != null) {
                invalidConfigCounter.add(1);
            }
            throw new IllegalArgumentException(errors.toString());
        }
    }
    
    /**
     * Refreshes the configuration cache with values from the attribute provider.
     * This method is called automatically when configuration values are accessed
     * and the cache is expired or not initialized.
     */
    public void refreshCache() {
        if (attributeProvider == null) {
            return; // No attribute provider to refresh from
        }
        
        try {
            // Get values from environment variables first, then fall back to attribute provider
            double minimalTripDistance = getDoubleFromEnv("TRACCAR_TRIP_MINIMAL_DISTANCE", 
                    AttributeUtil.lookup(attributeProvider, Keys.REPORT_TRIP_MINIMAL_TRIP_DISTANCE));
            
            long minimalTripDuration = getLongFromEnv("TRACCAR_TRIP_MINIMAL_DURATION", 
                    AttributeUtil.lookup(attributeProvider, Keys.REPORT_TRIP_MINIMAL_TRIP_DURATION)) * 1000;
            
            long minimalParkingDuration = getLongFromEnv("TRACCAR_TRIP_MINIMAL_PARKING_DURATION", 
                    AttributeUtil.lookup(attributeProvider, Keys.REPORT_TRIP_MINIMAL_PARKING_DURATION)) * 1000;
            
            long minimalNoDataDuration = getLongFromEnv("TRACCAR_TRIP_MINIMAL_NO_DATA_DURATION", 
                    AttributeUtil.lookup(attributeProvider, Keys.REPORT_TRIP_MINIMAL_NO_DATA_DURATION)) * 1000;
            
            boolean useIgnition = getBooleanFromEnv("TRACCAR_TRIP_USE_IGNITION", 
                    AttributeUtil.lookup(attributeProvider, Keys.REPORT_TRIP_USE_IGNITION));
            
            boolean ignoreOdometer = getBooleanFromEnv("TRACCAR_TRIP_IGNORE_ODOMETER", 
                    AttributeUtil.lookup(attributeProvider, Keys.REPORT_IGNORE_ODOMETER));
            
            validateParameters(minimalTripDistance, minimalTripDuration, minimalParkingDuration, minimalNoDataDuration);
            
            ConfigCache oldCache = configCache.get();
            ConfigCache newCache = new ConfigCache();
            newCache.minimalTripDistance = minimalTripDistance;
            newCache.minimalTripDuration = minimalTripDuration;
            newCache.minimalParkingDuration = minimalParkingDuration;
            newCache.minimalNoDataDuration = minimalNoDataDuration;
            newCache.useIgnition = useIgnition;
            newCache.ignoreOdometer = ignoreOdometer;
            newCache.timestamp = System.currentTimeMillis();
            
            configCache.set(newCache);
            
            // Notify listeners if values have changed
            if (oldCache != null && !oldCache.equals(newCache)) {
                notifyUpdateListeners();
            }
            
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to refresh trips configuration cache", e);
            if (invalidConfigCounter != null) {
                invalidConfigCounter.add(1);
            }
            // Keep using the old cache if refresh fails
        }
    }
    
    /**
     * Helper method to get a double value from environment variable or default value.
     */
    private double getDoubleFromEnv(String envName, double defaultValue) {
        String envValue = System.getenv(envName);
        if (envValue != null && !envValue.isEmpty()) {
            try {
                return Double.parseDouble(envValue);
            } catch (NumberFormatException e) {
                LOGGER.log(Level.WARNING, "Invalid value for " + envName + ": " + envValue, e);
            }
        }
        return defaultValue;
    }
    
    /**
     * Helper method to get a long value from environment variable or default value.
     */
    private long getLongFromEnv(String envName, long defaultValue) {
        String envValue = System.getenv(envName);
        if (envValue != null && !envValue.isEmpty()) {
            try {
                return Long.parseLong(envValue);
            } catch (NumberFormatException e) {
                LOGGER.log(Level.WARNING, "Invalid value for " + envName + ": " + envValue, e);
            }
        }
        return defaultValue;
    }
    
    /**
     * Helper method to get a boolean value from environment variable or default value.
     */
    private boolean getBooleanFromEnv(String envName, boolean defaultValue) {
        String envValue = System.getenv(envName);
        if (envValue != null && !envValue.isEmpty()) {
            return Boolean.parseBoolean(envValue);
        }
        return defaultValue;
    }
    
    /**
     * Gets the current configuration cache, refreshing if necessary.
     * 
     * @return The current configuration cache
     */
    private ConfigCache getConfigCache() {
        ConfigCache cache = configCache.get();
        
        // If cache is null or expired (older than 5 minutes), refresh it
        if (cache == null || System.currentTimeMillis() - cache.timestamp > TimeUnit.MINUTES.toMillis(5)) {
            refreshCache();
            cache = configCache.get();
        }
        
        return cache;
    }
    
    /**
     * Adds a listener that will be notified when configuration is updated.
     * 
     * @param id A unique identifier for the listener
     * @param listener The listener to be notified
     */
    public void addUpdateListener(String id, Consumer<TripsConfig> listener) {
        updateListeners.put(id, listener);
    }
    
    /**
     * Removes an update listener.
     * 
     * @param id The identifier of the listener to remove
     */
    public void removeUpdateListener(String id) {
        updateListeners.remove(id);
    }
    
    /**
     * Notifies all registered listeners about configuration updates.
     */
    private void notifyUpdateListeners() {
        updateListeners.values().forEach(listener -> {
            try {
                listener.accept(this);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "Error notifying trip config listener", e);
            }
        });
    }

    /**
     * Gets the minimum distance required for a trip.
     * 
     * @return The minimum trip distance in kilometers
     */
    public double getMinimalTripDistance() {
        if (tripDetectionCounter != null) {
            tripDetectionCounter.add(1);
        }
        return getConfigCache().minimalTripDistance;
    }

    /**
     * Gets the minimum duration required for a trip.
     * 
     * @return The minimum trip duration in milliseconds
     */
    public long getMinimalTripDuration() {
        return getConfigCache().minimalTripDuration;
    }

    /**
     * Gets the minimum duration required for parking.
     * 
     * @return The minimum parking duration in milliseconds
     */
    public long getMinimalParkingDuration() {
        return getConfigCache().minimalParkingDuration;
    }

    /**
     * Gets the minimum duration for no data.
     * 
     * @return The minimum no data duration in milliseconds
     */
    public long getMinimalNoDataDuration() {
        return getConfigCache().minimalNoDataDuration;
    }

    /**
     * Checks if ignition should be used for trip detection.
     * 
     * @return True if ignition should be used, false otherwise
     */
    public boolean getUseIgnition() {
        return getConfigCache().useIgnition;
    }

    /**
     * Checks if odometer should be ignored for trip detection.
     * 
     * @return True if odometer should be ignored, false otherwise
     */
    public boolean getIgnoreOdometer() {
        return getConfigCache().ignoreOdometer;
    }
    
    /**
     * Inner class to cache configuration values.
     */
    private static class ConfigCache {
        private double minimalTripDistance;
        private long minimalTripDuration;
        private long minimalParkingDuration;
        private long minimalNoDataDuration;
        private boolean useIgnition;
        private boolean ignoreOdometer;
        private long timestamp;
        
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            ConfigCache other = (ConfigCache) obj;
            return Double.compare(minimalTripDistance, other.minimalTripDistance) == 0
                    && minimalTripDuration == other.minimalTripDuration
                    && minimalParkingDuration == other.minimalParkingDuration
                    && minimalNoDataDuration == other.minimalNoDataDuration
                    && useIgnition == other.useIgnition
                    && ignoreOdometer == other.ignoreOdometer;
        }
        
        @Override
        public int hashCode() {
            int result = 17;
            result = 31 * result + Double.hashCode(minimalTripDistance);
            result = 31 * result + Long.hashCode(minimalTripDuration);
            result = 31 * result + Long.hashCode(minimalParkingDuration);
            result = 31 * result + Long.hashCode(minimalNoDataDuration);
            result = 31 * result + Boolean.hashCode(useIgnition);
            result = 31 * result + Boolean.hashCode(ignoreOdometer);
            return result;
        }
    }
}