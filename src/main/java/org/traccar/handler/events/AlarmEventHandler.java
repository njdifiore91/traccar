/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.handler.events;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Alarm event handler that consumes position data from the message broker,
 * detects alarms, and publishes alarm events back to the message broker.
 * Includes distributed tracing, metrics collection, and graceful degradation.
 */
@Singleton
public class AlarmEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlarmEventHandler.class);

    private final CacheManager cacheManager;
    private final boolean ignoreDuplicates;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    // Metrics
    private final Counter alarmsDetectedCounter;
    private final Counter alarmsDuplicatedCounter;
    private final Timer alarmProcessingTimer;

    /**
     * Constructs the AlarmEventHandler with required dependencies.
     *
     * @param config Configuration for alarm event handling
     * @param cacheManager Cache manager for position history
     * @param messageProducer Message producer for publishing events
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     */
    @Inject
    public AlarmEventHandler(
            Config config,
            CacheManager cacheManager,
            MessageProducer messageProducer,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.ignoreDuplicates = config.getBoolean(Keys.EVENT_IGNORE_DUPLICATE_ALERTS);
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.alarmsDetectedCounter = Counter.builder("traccar.alarms.detected")
                .description("Number of alarms detected")
                .register(meterRegistry);
        this.alarmsDuplicatedCounter = Counter.builder("traccar.alarms.duplicated")
                .description("Number of duplicate alarms filtered")
                .register(meterRegistry);
        this.alarmProcessingTimer = Timer.builder("traccar.alarms.processing.time")
                .description("Time taken to process alarm events")
                .register(meterRegistry);
        
        LOGGER.info("AlarmEventHandler initialized with ignoreDuplicates={}", ignoreDuplicates);
    }

    /**
     * Processes a position message from the message broker.
     * Detects alarms and publishes alarm events.
     *
     * @param position The position to process
     * @param correlationId Correlation ID for distributed tracing
     * @return CompletableFuture that completes when processing is done
     */
    public CompletableFuture<Void> processPosition(Position position, String correlationId) {
        // Create a span for this operation
        Span span = tracer.spanBuilder("AlarmEventHandler.processPosition")
                .setSpanKind(SpanKind.CONSUMER)
                .setParent(Context.current().with(Span.current()))
                .setAttribute("position.id", position.getId())
                .setAttribute("device.id", position.getDeviceId())
                .setAttribute("correlation.id", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            return alarmProcessingTimer.record(() -> {
                String alarmString = position.getString(Position.KEY_ALARM);
                if (alarmString != null) {
                    LOGGER.debug("Processing alarms: {} for device: {}", alarmString, position.getDeviceId());
                    Set<String> alarms = new HashSet<>(Arrays.asList(alarmString.split(",")));
                    int initialAlarmCount = alarms.size();
                    
                    // Filter out duplicate alarms if configured
                    if (ignoreDuplicates) {
                        try {
                            Position lastPosition = cacheManager.getPosition(position.getDeviceId());
                            if (lastPosition != null) {
                                String lastAlarmString = lastPosition.getString(Position.KEY_ALARM);
                                if (lastAlarmString != null) {
                                    Set<String> lastAlarms = new HashSet<>(Arrays.asList(lastAlarmString.split(",")));
                                    int beforeSize = alarms.size();
                                    alarms.removeAll(lastAlarms);
                                    int duplicatesRemoved = beforeSize - alarms.size();
                                    if (duplicatesRemoved > 0) {
                                        alarmsDuplicatedCounter.increment(duplicatesRemoved);
                                        span.setAttribute("alarms.duplicated", duplicatesRemoved);
                                        LOGGER.debug("Filtered {} duplicate alarms for device: {}", 
                                                duplicatesRemoved, position.getDeviceId());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            // Graceful degradation - log error but continue processing alarms
                            LOGGER.warn("Error accessing position cache, processing all alarms: {}", e.getMessage());
                            span.recordException(e);
                        }
                    }
                    
                    // Process and publish each alarm
                    CompletableFuture<?>[] futures = alarms.stream()
                            .map(alarm -> createAndPublishAlarmEvent(position, alarm, correlationId, span))
                            .toArray(CompletableFuture[]::new);
                    
                    // Record metrics
                    alarmsDetectedCounter.increment(alarms.size());
                    span.setAttribute("alarms.detected", alarms.size());
                    span.setAttribute("alarms.total", initialAlarmCount);
                    
                    // Wait for all alarm events to be published
                    return CompletableFuture.allOf(futures);
                }
                
                // No alarms to process
                return CompletableFuture.completedFuture(null);
            });
        } catch (Exception e) {
            LOGGER.error("Error processing position for alarms: {}", e.getMessage(), e);
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            return CompletableFuture.failedFuture(e);
        } finally {
            span.end();
        }
    }

    /**
     * Creates and publishes an alarm event to the message broker.
     *
     * @param position The position associated with the alarm
     * @param alarm The alarm type
     * @param correlationId Correlation ID for distributed tracing
     * @param parentSpan Parent span for tracing context
     * @return CompletableFuture that completes when the event is published
     */
    private CompletableFuture<Void> createAndPublishAlarmEvent(
            Position position, String alarm, String correlationId, Span parentSpan) {
        
        Span span = tracer.spanBuilder("AlarmEventHandler.createAndPublishAlarmEvent")
                .setSpanKind(SpanKind.PRODUCER)
                .setParent(Context.current().with(parentSpan))
                .setAttribute("alarm.type", alarm)
                .setAttribute("device.id", position.getDeviceId())
                .setAttribute("correlation.id", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Create the alarm event
            Event event = new Event(Event.TYPE_ALARM, position);
            event.set(Position.KEY_ALARM, alarm);
            event.set("correlationId", correlationId);
            
            LOGGER.debug("Publishing alarm event: {} for device: {}", alarm, position.getDeviceId());
            
            // Publish the event to the message broker
            return messageProducer.publishEvent(event, correlationId)
                    .exceptionally(e -> {
                        LOGGER.error("Failed to publish alarm event: {}", e.getMessage(), e);
                        span.recordException(e);
                        span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                        throw new RuntimeException("Failed to publish alarm event", e);
                    });
        } finally {
            span.end();
        }
    }
}