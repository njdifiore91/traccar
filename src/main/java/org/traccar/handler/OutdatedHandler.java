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
package org.traccar.handler;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.messaging.MessageProducer;

import java.util.Date;

public class OutdatedHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutdatedHandler.class);

    private final CacheManager cacheManager;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final MessageProducer messageProducer;
    private final Counter restoredPositionsCounter;

    @Inject
    public OutdatedHandler(CacheManager cacheManager, Tracer tracer, 
                          MeterRegistry meterRegistry, MessageProducer messageProducer) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.messageProducer = messageProducer;
        this.restoredPositionsCounter = meterRegistry.counter("traccar.positions.outdated.restored");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        Span span = tracer.spanBuilder("outdated.position.process")
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("outdated", String.valueOf(position.getOutdated()))
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            if (position.getOutdated()) {
                span.addEvent("Restoring outdated position");
                
                // Check if we should process directly or asynchronously
                if (messageProducer != null && messageProducer.isAvailable()) {
                    // Asynchronous processing via message broker
                    span.addEvent("Sending to message broker for processing");
                    messageProducer.sendOutdatedPosition(position, Context.current());
                    span.addEvent("Position sent to message broker");
                    callback.processed(false);
                } else {
                    // Direct processing
                    span.addEvent("Processing directly");
                    processOutdatedPosition(position, span);
                    span.addEvent("Position processed directly");
                    callback.processed(false);
                }
            } else {
                callback.processed(false);
            }
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.warn("Error processing outdated position", e);
            callback.processed(false);
        } finally {
            span.end();
        }
    }

    /**
     * Process an outdated position by restoring data from the last known position
     * 
     * @param position The outdated position to process
     * @param span The current tracing span
     */
    public void processOutdatedPosition(Position position, Span span) {
        Position last = cacheManager.getPosition(position.getDeviceId());
        if (last != null) {
            span.addEvent("Restoring from cached position");
            position.setFixTime(last.getFixTime());
            position.setValid(last.getValid());
            position.setLatitude(last.getLatitude());
            position.setLongitude(last.getLongitude());
            position.setAltitude(last.getAltitude());
            position.setSpeed(last.getSpeed());
            position.setCourse(last.getCourse());
            position.setAccuracy(last.getAccuracy());
            
            // Increment metrics counter for restored positions
            restoredPositionsCounter.increment();
        } else {
            span.addEvent("No cached position found, using default values");
            position.setFixTime(new Date(315964819000L)); // gps epoch 1980-01-06
        }
        
        if (position.getDeviceTime() == null) {
            position.setDeviceTime(position.getServerTime());
        }
    }

    /**
     * Process an outdated position from a message broker
     * This method is called when a position is received from the message broker
     * 
     * @param position The outdated position to process
     * @param context The OpenTelemetry context for distributed tracing
     */
    public void processFromMessageBroker(Position position, Context context) {
        Span span = tracer.spanBuilder("outdated.position.process.async")
                .setParent(context)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("processingType", "async")
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.addEvent("Processing position from message broker");
            processOutdatedPosition(position, span);
            span.addEvent("Position processed from message broker");
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.warn("Error processing outdated position from message broker", e);
        } finally {
            span.end();
        }
    }
}