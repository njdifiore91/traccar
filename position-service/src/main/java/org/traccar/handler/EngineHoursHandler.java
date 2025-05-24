/*
 * Copyright 2018 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2018 Andrey Kunitsyn (andrey@traccar.org)
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

import jakarta.inject.Inject;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

// OpenTelemetry imports
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

/**
 * Handler that calculates and populates the engine operating hours (Position.KEY_HOURS)
 * attribute on incoming Position objects when that attribute is absent.
 */
public class EngineHoursHandler extends BasePositionHandler {

    private final CacheManager cacheManager;
    private final Tracer tracer;

    /**
     * Constructs the EngineHoursHandler with required dependencies.
     * 
     * @param cacheManager The cache manager for retrieving previous positions
     * @param tracer The OpenTelemetry tracer for performance monitoring
     */
    @Inject
    public EngineHoursHandler(CacheManager cacheManager, Tracer tracer) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for monitoring the engine hours calculation performance
        Span span = tracer.spanBuilder("EngineHoursHandler.onPosition").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", position.getDeviceId());
            
            if (!position.hasAttribute(Position.KEY_HOURS)) {
                // Add attribute to indicate we're calculating engine hours
                span.setAttribute("calculating_hours", true);
                
                // Get the last position from cache for this device
                Position last = cacheManager.getPosition(position.getDeviceId());
                if (last != null) {
                    // Record that we found a previous position
                    span.setAttribute("previous_position_found", true);
                    
                    long hours = last.getLong(Position.KEY_HOURS);
                    // Only add time if ignition was on for both positions
                    if (last.getBoolean(Position.KEY_IGNITION) && position.getBoolean(Position.KEY_IGNITION)) {
                        // Calculate time difference between positions
                        long timeDifference = position.getDeviceTime().getTime() - last.getDeviceTime().getTime();
                        span.setAttribute("time_difference_ms", timeDifference);
                        
                        // Add the time difference to the accumulated hours
                        hours += timeDifference;
                    }
                    
                    if (hours != 0) {
                        position.set(Position.KEY_HOURS, hours);
                        span.setAttribute("hours_set", true);
                    }
                } else {
                    span.setAttribute("previous_position_found", false);
                }
            } else {
                span.setAttribute("calculating_hours", false);
            }
            
            // Mark the span as successful
            span.setAttribute("success", true);
        } catch (Exception e) {
            // Record the error in the span
            span.recordException(e);
            span.setAttribute("success", false);
            // Re-throw the exception to maintain the error handling flow
            throw e;
        } finally {
            // End the span regardless of success or failure
            span.end();
            // Continue processing the position
            callback.processed(false);
        }
    }
}