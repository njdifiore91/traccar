/*
 * Copyright 2019 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Handler that adjusts position time based on configuration settings.
 * Supports both direct processing and asynchronous processing via message broker.
 */
public class TimeHandler extends BasePositionHandler {

    private final boolean useServerTime;
    private final Set<String> protocols;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final MessageProducer messageProducer;
    
    private final Counter serverTimeAdjustments;
    private final Counter deviceTimeAdjustments;

    /**
     * Constructs a new TimeHandler with the specified dependencies.
     *
     * @param config Configuration settings
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for collecting operational metrics
     * @param messageProducer Message producer for asynchronous processing
     */
    @Inject
    public TimeHandler(Config config, Tracer tracer, MeterRegistry meterRegistry, MessageProducer messageProducer) {
        useServerTime = config.getString(Keys.TIME_OVERRIDE).equalsIgnoreCase("serverTime");
        String protocolList = config.getString(Keys.TIME_PROTOCOLS);
        if (protocolList != null) {
            protocols = new HashSet<>(Arrays.asList(protocolList.split("[, ]")));
        } else {
            protocols = null;
        }
        
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.messageProducer = messageProducer;
        
        // Initialize metrics counters
        this.serverTimeAdjustments = Counter.builder("traccar.time.adjustments")
                .tag("type", "server")
                .description("Number of positions adjusted to server time")
                .register(meterRegistry);
        
        this.deviceTimeAdjustments = Counter.builder("traccar.time.adjustments")
                .tag("type", "device")
                .description("Number of positions adjusted to device time")
                .register(meterRegistry);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("TimeHandler.onPosition").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("position.deviceId", position.getDeviceId());
            span.setAttribute("position.protocol", position.getProtocol());
            
            // Apply time adjustment logic
            adjustTime(position);
            
            // Continue processing
            callback.processed(false);
        } finally {
            span.end();
        }
    }
    
    /**
     * Processes a position asynchronously via message broker.
     * This method is called when processing positions from the message broker.
     *
     * @param position The position to process
     * @return The processed position
     */
    public Position processAsync(Position position) {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("TimeHandler.processAsync").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("position.deviceId", position.getDeviceId());
            span.setAttribute("position.protocol", position.getProtocol());
            
            // Apply time adjustment logic
            adjustTime(position);
            
            return position;
        } finally {
            span.end();
        }
    }
    
    /**
     * Adjusts the time of a position based on configuration settings.
     *
     * @param position The position to adjust
     */
    private void adjustTime(Position position) {
        if (protocols == null || protocols.contains(position.getProtocol())) {
            if (useServerTime) {
                position.setDeviceTime(position.getServerTime());
                position.setFixTime(position.getServerTime());
                serverTimeAdjustments.increment();
            } else {
                position.setFixTime(position.getDeviceTime());
                deviceTimeAdjustments.increment();
            }
        }
    }
}