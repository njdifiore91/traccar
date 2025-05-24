/*
 * Copyright 2014 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.handler;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.UnitsConverter;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.metrics.MetricsService;
import org.traccar.model.Calendar;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.position.PositionCacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Order;
import org.traccar.storage.query.Request;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * FilterHandler applies a comprehensive and highly configurable sequence of filters 
 * to every incoming Position event before it proceeds further in the processing pipeline.
 * 
 * This implementation is adapted for the position-service microservice architecture with
 * OpenTelemetry instrumentation for monitoring filter performance.
 */
public class FilterHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterHandler.class);

    private final boolean filterInvalid;
    private final boolean filterZero;
    private final boolean filterDuplicate;
    private final boolean filterOutdated;
    private final long filterFuture;
    private final long filterPast;
    private final boolean filterApproximate;
    private final int filterAccuracy;
    private final boolean filterStatic;
    private final int filterDistance;
    private final int filterMaxSpeed;
    private final long filterMinPeriod;
    private final int filterDailyLimit;
    private final long filterDailyLimitInterval;
    private final boolean filterRelative;
    private final long skipLimit;
    private final boolean skipAttributes;

    private final PositionCacheManager positionCacheManager;
    private final Storage storage;
    private final MetricsService metricsService;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    // Thread-safe cache for daily message counts
    private final ConcurrentMap<Long, Integer> deviceMessageCounters = new ConcurrentHashMap<>();
    
    // Metrics
    private final Counter totalPositionsCounter;
    private final Counter filteredPositionsCounter;
    private final Counter invalidPositionsCounter;
    private final Counter zeroPositionsCounter;
    private final Counter duplicatePositionsCounter;
    private final Counter outdatedPositionsCounter;
    private final Counter futurePositionsCounter;
    private final Counter pastPositionsCounter;
    private final Counter accuracyPositionsCounter;
    private final Counter approximatePositionsCounter;
    private final Counter staticPositionsCounter;
    private final Counter distancePositionsCounter;
    private final Counter maxSpeedPositionsCounter;
    private final Counter minPeriodPositionsCounter;
    private final Counter dailyLimitPositionsCounter;
    private final Counter calendarPositionsCounter;
    private final Timer filterProcessingTimer;

    @Inject
    public FilterHandler(
            Config config, 
            PositionCacheManager positionCacheManager, 
            Storage storage, 
            MetricsService metricsService,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        filterInvalid = config.getBoolean(Keys.FILTER_INVALID);
        filterZero = config.getBoolean(Keys.FILTER_ZERO);
        filterDuplicate = config.getBoolean(Keys.FILTER_DUPLICATE);
        filterOutdated = config.getBoolean(Keys.FILTER_OUTDATED);
        filterFuture = config.getLong(Keys.FILTER_FUTURE) * 1000;
        filterPast = config.getLong(Keys.FILTER_PAST) * 1000;
        filterAccuracy = config.getInteger(Keys.FILTER_ACCURACY);
        filterApproximate = config.getBoolean(Keys.FILTER_APPROXIMATE);
        filterStatic = config.getBoolean(Keys.FILTER_STATIC);
        filterDistance = config.getInteger(Keys.FILTER_DISTANCE);
        filterMaxSpeed = config.getInteger(Keys.FILTER_MAX_SPEED);
        filterMinPeriod = config.getInteger(Keys.FILTER_MIN_PERIOD) * 1000L;
        filterDailyLimit = config.getInteger(Keys.FILTER_DAILY_LIMIT);
        filterDailyLimitInterval = config.getInteger(Keys.FILTER_DAILY_LIMIT_INTERVAL) * 1000L;
        filterRelative = config.getBoolean(Keys.FILTER_RELATIVE);
        skipLimit = config.getLong(Keys.FILTER_SKIP_LIMIT) * 1000;
        skipAttributes = config.getBoolean(Keys.FILTER_SKIP_ATTRIBUTES_ENABLE);
        
        this.positionCacheManager = positionCacheManager;
        this.storage = storage;
        this.metricsService = metricsService;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        totalPositionsCounter = Counter.builder("position.total")
                .description("Total number of positions processed")
                .register(meterRegistry);
        
        filteredPositionsCounter = Counter.builder("position.filtered")
                .description("Total number of positions filtered out")
                .register(meterRegistry);
        
        invalidPositionsCounter = Counter.builder("position.filtered.invalid")
                .description("Number of invalid positions filtered out")
                .register(meterRegistry);
        
        zeroPositionsCounter = Counter.builder("position.filtered.zero")
                .description("Number of zero coordinate positions filtered out")
                .register(meterRegistry);
        
        duplicatePositionsCounter = Counter.builder("position.filtered.duplicate")
                .description("Number of duplicate positions filtered out")
                .register(meterRegistry);
        
        outdatedPositionsCounter = Counter.builder("position.filtered.outdated")
                .description("Number of outdated positions filtered out")
                .register(meterRegistry);
        
        futurePositionsCounter = Counter.builder("position.filtered.future")
                .description("Number of future positions filtered out")
                .register(meterRegistry);
        
        pastPositionsCounter = Counter.builder("position.filtered.past")
                .description("Number of past positions filtered out")
                .register(meterRegistry);
        
        accuracyPositionsCounter = Counter.builder("position.filtered.accuracy")
                .description("Number of positions filtered out due to accuracy")
                .register(meterRegistry);
        
        approximatePositionsCounter = Counter.builder("position.filtered.approximate")
                .description("Number of approximate positions filtered out")
                .register(meterRegistry);
        
        staticPositionsCounter = Counter.builder("position.filtered.static")
                .description("Number of static positions filtered out")
                .register(meterRegistry);
        
        distancePositionsCounter = Counter.builder("position.filtered.distance")
                .description("Number of positions filtered out due to distance")
                .register(meterRegistry);
        
        maxSpeedPositionsCounter = Counter.builder("position.filtered.maxspeed")
                .description("Number of positions filtered out due to max speed")
                .register(meterRegistry);
        
        minPeriodPositionsCounter = Counter.builder("position.filtered.minperiod")
                .description("Number of positions filtered out due to min period")
                .register(meterRegistry);
        
        dailyLimitPositionsCounter = Counter.builder("position.filtered.dailylimit")
                .description("Number of positions filtered out due to daily limit")
                .register(meterRegistry);
        
        calendarPositionsCounter = Counter.builder("position.filtered.calendar")
                .description("Number of positions filtered out due to calendar")
                .register(meterRegistry);
        
        filterProcessingTimer = Timer.builder("position.filter.processing.time")
                .description("Time taken to process position filters")
                .register(meterRegistry);
    }

    private Position getPrecedingPosition(long deviceId, Date date) throws StorageException {
        Span span = tracer.spanBuilder("getPrecedingPosition").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("date", date.toString());
            
            Position position = storage.getObject(Position.class, new Request(
                    new Columns.All(),
                    new Condition.And(
                            new Condition.Equals("deviceId", deviceId),
                            new Condition.Compare("fixTime", "<=", "time", date)),
                    new Order("fixTime", true, 1)));
            
            span.setStatus(StatusCode.OK);
            return position;
        } catch (StorageException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to get preceding position");
            throw e;
        } finally {
            span.end();
        }
    }

    @WithSpan("filterInvalid")
    private boolean filterInvalid(Position position) {
        boolean filtered = filterInvalid && (!position.getValid()
                || position.getLatitude() > 90 || position.getLongitude() > 180
                || position.getLatitude() < -90 || position.getLongitude() < -180);
        
        if (filtered) {
            invalidPositionsCounter.increment();
        }
        
        return filtered;
    }

    @WithSpan("filterZero")
    private boolean filterZero(Position position) {
        boolean filtered = filterZero && position.getLatitude() == 0.0 && position.getLongitude() == 0.0;
        
        if (filtered) {
            zeroPositionsCounter.increment();
        }
        
        return filtered;
    }

    @WithSpan("filterDuplicate")
    private boolean filterDuplicate(Position position, Position last) {
        if (filterDuplicate && last != null && position.getFixTime().equals(last.getFixTime())) {
            for (String key : position.getAttributes().keySet()) {
                if (!last.hasAttribute(key)) {
                    return false;
                }
            }
            duplicatePositionsCounter.increment();
            return true;
        }
        return false;
    }

    @WithSpan("filterOutdated")
    private boolean filterOutdated(Position position) {
        boolean filtered = filterOutdated && position.getOutdated();
        
        if (filtered) {
            outdatedPositionsCounter.increment();
        }
        
        return filtered;
    }

    @WithSpan("filterFuture")
    private boolean filterFuture(Position position) {
        boolean filtered = filterFuture != 0 && position.getFixTime().getTime() > System.currentTimeMillis() + filterFuture;
        
        if (filtered) {
            futurePositionsCounter.increment();
        }
        
        return filtered;
    }

    @WithSpan("filterPast")
    private boolean filterPast(Position position) {
        boolean filtered = filterPast != 0 && position.getFixTime().getTime() < System.currentTimeMillis() - filterPast;
        
        if (filtered) {
            pastPositionsCounter.increment();
        }
        
        return filtered;
    }

    @WithSpan("filterAccuracy")
    private boolean filterAccuracy(Position position) {
        boolean filtered = filterAccuracy != 0 && position.getAccuracy() > filterAccuracy;
        
        if (filtered) {
            accuracyPositionsCounter.increment();
        }
        
        return filtered;
    }

    @WithSpan("filterApproximate")
    private boolean filterApproximate(Position position) {
        boolean filtered = filterApproximate && position.getBoolean(Position.KEY_APPROXIMATE);
        
        if (filtered) {
            approximatePositionsCounter.increment();
        }
        
        return filtered;
    }

    @WithSpan("filterStatic")
    private boolean filterStatic(Position position) {
        boolean filtered = filterStatic && position.getSpeed() == 0.0;
        
        if (filtered) {
            staticPositionsCounter.increment();
        }
        
        return filtered;
    }

    @WithSpan("filterDistance")
    private boolean filterDistance(Position position, Position last) {
        if (filterDistance != 0 && last != null) {
            boolean filtered = position.getDouble(Position.KEY_DISTANCE) < filterDistance;
            
            if (filtered) {
                distancePositionsCounter.increment();
            }
            
            return filtered;
        }
        return false;
    }

    @WithSpan("filterMaxSpeed")
    private boolean filterMaxSpeed(Position position, Position last) {
        if (filterMaxSpeed != 0 && last != null) {
            double distance = position.getDouble(Position.KEY_DISTANCE);
            double time = position.getFixTime().getTime() - last.getFixTime().getTime();
            boolean filtered = time > 0 && UnitsConverter.knotsFromMps(distance / (time / 1000)) > filterMaxSpeed;
            
            if (filtered) {
                maxSpeedPositionsCounter.increment();
            }
            
            return filtered;
        }
        return false;
    }

    @WithSpan("filterMinPeriod")
    private boolean filterMinPeriod(Position position, Position last) {
        if (filterMinPeriod != 0 && last != null) {
            long time = position.getFixTime().getTime() - last.getFixTime().getTime();
            boolean filtered = time > 0 && time < filterMinPeriod;
            
            if (filtered) {
                minPeriodPositionsCounter.increment();
            }
            
            return filtered;
        }
        return false;
    }

    @WithSpan("filterDailyLimit")
    private boolean filterDailyLimit(Position position, Position last) {
        if (filterDailyLimit != 0) {
            long deviceId = position.getDeviceId();
            int messageCount = getDeviceMessageCount(deviceId);
            
            if (messageCount >= filterDailyLimit) {
                long lastTime = last != null ? last.getFixTime().getTime() : 0;
                long interval = position.getFixTime().getTime() - lastTime;
                boolean filtered = filterDailyLimitInterval <= 0 || interval < filterDailyLimitInterval;
                
                if (filtered) {
                    dailyLimitPositionsCounter.increment();
                }
                
                return filtered;
            }
        }
        return false;
    }

    private int getDeviceMessageCount(long deviceId) {
        return deviceMessageCounters.getOrDefault(deviceId, metricsService.getMessageCount(deviceId));
    }
    
    private void incrementDeviceMessageCount(long deviceId) {
        deviceMessageCounters.compute(deviceId, (key, count) -> count == null ? 1 : count + 1);
    }

    @WithSpan("skipLimit")
    private boolean skipLimit(Position position, Position last) {
        if (skipLimit != 0 && last != null) {
            return (position.getServerTime().getTime() - last.getServerTime().getTime()) > skipLimit;
        }
        return false;
    }

    @WithSpan("skipAttributes")
    private boolean skipAttributes(Position position) {
        if (skipAttributes) {
            String string = AttributeUtil.lookup(positionCacheManager, Keys.FILTER_SKIP_ATTRIBUTES, position.getDeviceId());
            for (String attribute : string.split("[ ,]")) {
                if (position.hasAttribute(attribute)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Applies all configured filters to the position
     * 
     * @param position Position to filter
     * @return true if position should be filtered out, false otherwise
     */
    @WithSpan("filter")
    protected boolean filter(Position position) {
        totalPositionsCounter.increment();
        
        return filterProcessingTimer.record(() -> {
            StringBuilder filterType = new StringBuilder();
            long deviceId = position.getDeviceId();
            
            // filter out invalid data
            if (filterInvalid(position)) {
                filterType.append("Invalid ");
            }
            if (filterZero(position)) {
                filterType.append("Zero ");
            }
            if (filterOutdated(position)) {
                filterType.append("Outdated ");
            }
            if (filterFuture(position)) {
                filterType.append("Future ");
            }
            if (filterPast(position)) {
                filterType.append("Past ");
            }
            if (filterAccuracy(position)) {
                filterType.append("Accuracy ");
            }
            if (filterApproximate(position)) {
                filterType.append("Approximate ");
            }

            // filter out excessive data
            if (filterDuplicate || filterStatic
                    || filterDistance > 0 || filterMaxSpeed > 0 || filterMinPeriod > 0 || filterDailyLimit > 0) {
                Position preceding;
                if (filterRelative) {
                    try {
                        Date newFixTime = position.getFixTime();
                        preceding = getPrecedingPosition(deviceId, newFixTime);
                    } catch (StorageException e) {
                        LOGGER.warn("Error retrieving preceding position; fall backing to last received position.", e);
                        preceding = positionCacheManager.getLastPosition(deviceId);
                    }
                } else {
                    preceding = positionCacheManager.getLastPosition(deviceId);
                }
                if (filterDuplicate(position, preceding) && !skipLimit(position, preceding) && !skipAttributes(position)) {
                    filterType.append("Duplicate ");
                }
                if (filterStatic(position) && !skipLimit(position, preceding) && !skipAttributes(position)) {
                    filterType.append("Static ");
                }
                if (filterDistance(position, preceding) && !skipLimit(position, preceding) && !skipAttributes(position)) {
                    filterType.append("Distance ");
                }
                if (filterMaxSpeed(position, preceding)) {
                    filterType.append("MaxSpeed ");
                }
                if (filterMinPeriod(position, preceding)) {
                    filterType.append("MinPeriod ");
                }
                if (filterDailyLimit(position, preceding)) {
                    filterType.append("DailyLimit ");
                }
            }

            Device device = positionCacheManager.getDevice(deviceId);
            if (device.getCalendarId() > 0) {
                Calendar calendar = positionCacheManager.getCalendar(device.getCalendarId());
                if (!calendar.checkMoment(position.getFixTime())) {
                    filterType.append("Calendar ");
                    calendarPositionsCounter.increment();
                }
            }

            if (!filterType.isEmpty()) {
                LOGGER.info("Position filtered by {}filters from device: {}", filterType, device.getUniqueId());
                filteredPositionsCounter.increment();
                return true;
            }
            
            // If position is not filtered, increment the device message count
            incrementDeviceMessageCount(deviceId);
            return false;
        });
    }

    @Override
    @WithSpan("onPosition")
    public void onPosition(Position position, Callback callback) {
        Span span = Span.current();
        span.setAttribute("deviceId", position.getDeviceId());
        span.setAttribute("fixTime", position.getFixTime().toString());
        
        boolean filtered = filter(position);
        span.setAttribute("filtered", filtered);
        
        callback.processed(filtered);
    }
}