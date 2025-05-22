/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 - 2017 Andrey Kunitsyn (andrey@traccar.org)
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

/**
 * Handler that copies attributes from the last position to the current position if they don't exist.
 * Updated to support both direct and asynchronous processing via message broker.
 */
public class CopyAttributesHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CopyAttributesHandler.class);
    
    private final CacheManager cacheManager;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Timer processingTimer;

    /**
     * Initialize the handler with required dependencies.
     *
     * @param config Configuration provider
     * @param cacheManager Cache manager for accessing device positions
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Metrics registry for performance monitoring
     */
    @Inject
    public CopyAttributesHandler(
            Config config, 
            CacheManager cacheManager,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.processingTimer = Timer.builder("traccar.handler.copy_attributes")
                .description("Time spent processing copy attributes operations")
                .register(meterRegistry);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("CopyAttributesHandler.onPosition").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Add relevant attributes to the span for better tracing context
            span.setAttribute("deviceId", String.valueOf(position.getDeviceId()));
            span.setAttribute("positionId", String.valueOf(position.getId()));
            
            // Use timer to measure performance
            processingTimer.record(() -> {
                processPosition(position, callback, span);
            });
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.warn("Error in CopyAttributesHandler", e);
            callback.processed(false);
        } finally {
            span.end();
        }
    }
    
    /**
     * Process the position by copying attributes from the last position.
     * 
     * @param position Current position
     * @param callback Callback to notify when processing is complete
     * @param span Current tracing span for adding context
     */
    private void processPosition(Position position, Callback callback, Span span) {
        String attributesString = AttributeUtil.lookup(
                cacheManager, Keys.PROCESSING_COPY_ATTRIBUTES, position.getDeviceId());
        
        if (attributesString != null) {
            span.setAttribute("copyAttributes", attributesString);
            
            Position last = cacheManager.getPosition(position.getDeviceId());
            if (last != null) {
                span.setAttribute("lastPositionId", String.valueOf(last.getId()));
                int copiedCount = 0;
                
                for (String attribute : attributesString.split("[ ,]")) {
                    if (last.hasAttribute(attribute) && !position.hasAttribute(attribute)) {
                        position.getAttributes().put(attribute, last.getAttributes().get(attribute));
                        copiedCount++;
                    }
                }
                
                span.setAttribute("attributesCopied", copiedCount);
                meterRegistry.counter("traccar.handler.copy_attributes.count").increment(copiedCount);
            } else {
                span.setAttribute("lastPositionFound", false);
            }
        } else {
            span.setAttribute("copyAttributesConfigured", false);
        }
        
        callback.processed(false);
    }
}