/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 Andrey Kunitsyn (andrey@traccar.org)
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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.extension.annotations.WithSpan;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class IgnitionEventHandler extends BaseEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(IgnitionEventHandler.class);
    
    private final CacheManager cacheManager;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final Timer ignitionEventTimer;
    private final Timer ignitionOnEventTimer;
    private final Timer ignitionOffEventTimer;

    @Inject
    public IgnitionEventHandler(
            CacheManager cacheManager, 
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            TextMapPropagator propagator,
            MeterRegistry meterRegistry) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
        this.propagator = propagator;
        
        // Configure circuit breaker for CacheManager interactions
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(TimeoutException.class, RuntimeException.class)
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(
                "ignitionEventHandler", circuitBreakerConfig);
        
        // Initialize metrics
        this.ignitionEventTimer = Timer.builder("traccar.event.ignition.detection.duration")
                .description("Time taken to detect ignition events")
                .register(meterRegistry);
        
        this.ignitionOnEventTimer = Timer.builder("traccar.event.ignition.on.detection.duration")
                .description("Time taken to detect ignition ON events")
                .register(meterRegistry);
        
        this.ignitionOffEventTimer = Timer.builder("traccar.event.ignition.off.detection.duration")
                .description("Time taken to detect ignition OFF events")
                .register(meterRegistry);
    }

    @Override
    @WithSpan(value = "IgnitionEventHandler.onPosition", kind = SpanKind.CONSUMER)
    public void onPosition(Position position, Callback callback) {
        // Extract trace context from position attributes if available
        Context context = extractContext(position);
        Span span = tracer.spanBuilder("IgnitionEventHandler.processPosition")
                .setParent(context)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("position.id", position.getId())
                .setAttribute("position.deviceId", position.getDeviceId())
                .setAttribute("position.protocol", position.getProtocol());
            
            // Use circuit breaker for CacheManager interactions
            Timer.Sample sample = Timer.start();
            
            Supplier<Device> deviceSupplier = CircuitBreaker.decorateSupplier(
                    circuitBreaker, 
                    () -> cacheManager.getObject(Device.class, position.getDeviceId()));
            
            Device device = deviceSupplier.get();
            
            if (device == null) {
                span.setStatus(StatusCode.ERROR, "Device not found in cache");
                span.end();
                return;
            }
            
            // Check if this is the latest position
            Supplier<Boolean> isLatestSupplier = CircuitBreaker.decorateSupplier(
                    circuitBreaker,
                    () -> PositionUtil.isLatest(cacheManager, position));
            
            if (!isLatestSupplier.get()) {
                span.addEvent("Position is not latest");
                span.end();
                return;
            }

            if (position.hasAttribute(Position.KEY_IGNITION)) {
                boolean ignition = position.getBoolean(Position.KEY_IGNITION);
                span.setAttribute("position.ignition", ignition);

                // Get last position with circuit breaker
                Supplier<Position> lastPositionSupplier = CircuitBreaker.decorateSupplier(
                        circuitBreaker,
                        () -> cacheManager.getPosition(position.getDeviceId()));
                
                Position lastPosition = lastPositionSupplier.get();
                
                if (lastPosition != null && lastPosition.hasAttribute(Position.KEY_IGNITION)) {
                    boolean oldIgnition = lastPosition.getBoolean(Position.KEY_IGNITION);
                    span.setAttribute("position.previous.ignition", oldIgnition);

                    if (ignition && !oldIgnition) {
                        Timer.Sample onSample = Timer.start();
                        span.addEvent("Ignition ON event detected");
                        
                        Event event = new Event(Event.TYPE_IGNITION_ON, position);
                        // Propagate trace context to the event
                        injectContext(event, span.getSpanContext());
                        
                        callback.eventDetected(event);
                        onSample.stop(ignitionOnEventTimer);
                    } else if (!ignition && oldIgnition) {
                        Timer.Sample offSample = Timer.start();
                        span.addEvent("Ignition OFF event detected");
                        
                        Event event = new Event(Event.TYPE_IGNITION_OFF, position);
                        // Propagate trace context to the event
                        injectContext(event, span.getSpanContext());
                        
                        callback.eventDetected(event);
                        offSample.stop(ignitionOffEventTimer);
                    }
                }
            }
            
            sample.stop(ignitionEventTimer);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Error processing ignition event", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
    
    // Helper method to extract trace context from position attributes
    private Context extractContext(Position position) {
        Map<String, Object> attributes = position.getAttributes();
        return propagator.extract(Context.current(), attributes, new TextMapGetter<Map<String, Object>>() {
            @Override
            public Iterable<String> keys(Map<String, Object> carrier) {
                return carrier.keySet();
            }

            @Override
            public String get(Map<String, Object> carrier, String key) {
                Object value = carrier.get(key);
                return value != null ? value.toString() : null;
            }
        });
    }
    
    // Helper method to inject trace context into event attributes
    private void injectContext(Event event, io.opentelemetry.api.trace.SpanContext spanContext) {
        propagator.inject(Context.current().with(Span.wrap(spanContext)), event.getAttributes(), (carrier, key, value) -> {
            carrier.put(key, value);
        });
    }
}