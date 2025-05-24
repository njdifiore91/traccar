/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.TaskScheduler;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.forward.PositionData;
import org.traccar.forward.PositionForwarder;
import org.traccar.forward.ResultHandler;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

public class PositionForwardingHandler extends BasePositionHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionForwardingHandler.class);

    private final CacheManager cacheManager;
    private final TaskScheduler taskScheduler;
    private final PositionForwarder positionForwarder;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    private final boolean retryEnabled;
    private final int retryDelay;
    private final int retryCount;
    private final int retryLimit;

    private final AtomicInteger deliveryPending;
    
    private final Counter forwardingAttempts;
    private final Counter forwardingSuccesses;
    private final Counter forwardingFailures;
    private final Timer forwardingDuration;

    @Inject
    public PositionForwardingHandler(
            Config config, 
            CacheManager cacheManager, 
            TaskScheduler taskScheduler, 
            @Nullable PositionForwarder positionForwarder,
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            MeterRegistry meterRegistry) {

        this.cacheManager = cacheManager;
        this.taskScheduler = taskScheduler;
        this.positionForwarder = positionForwarder;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        this.retryEnabled = config.getBoolean(Keys.FORWARD_RETRY_ENABLE);
        this.retryDelay = config.getInteger(Keys.FORWARD_RETRY_DELAY);
        this.retryCount = config.getInteger(Keys.FORWARD_RETRY_COUNT);
        this.retryLimit = config.getInteger(Keys.FORWARD_RETRY_LIMIT);

        this.deliveryPending = new AtomicInteger();
        
        // Initialize metrics
        this.forwardingAttempts = meterRegistry.counter("position.forwarding.attempts");
        this.forwardingSuccesses = meterRegistry.counter("position.forwarding.successes");
        this.forwardingFailures = meterRegistry.counter("position.forwarding.failures");
        this.forwardingDuration = meterRegistry.timer("position.forwarding.duration");
        
        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("positionForwarder", circuitBreakerConfig);
    }

    class AsyncRequestAndCallback implements ResultHandler, Runnable {

        private final PositionData positionData;
        private final Span parentSpan;
        private int retries = 0;

        AsyncRequestAndCallback(PositionData positionData, Span parentSpan) {
            this.positionData = positionData;
            this.parentSpan = parentSpan;
            deliveryPending.incrementAndGet();
        }

        private void send() {
            forwardingAttempts.increment();
            Span span = tracer.spanBuilder("position.forward")
                    .setParent(Context.current().with(parentSpan))
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                span.setAttribute("deviceId", positionData.getDevice().getId());
                span.setAttribute("retry.count", retries);
                
                Timer.Sample sample = Timer.start(meterRegistry);
                
                circuitBreaker.executeRunnable(() -> {
                    positionForwarder.forward(positionData, this);
                });
                
                sample.stop(forwardingDuration);
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                retry(e);
            } finally {
                span.end();
            }
        }

        private void retry(Throwable throwable) {
            boolean scheduled = false;
            try {
                if (retryEnabled && deliveryPending.get() <= retryLimit && retries < retryCount) {
                    schedule();
                    scheduled = true;
                }
            } finally {
                int pending = scheduled ? deliveryPending.get() : deliveryPending.decrementAndGet();
                LOGGER.warn("Position forwarding failed: " + pending + " pending", throwable);
                forwardingFailures.increment();
            }
        }

        private void schedule() {
            long delay = retryDelay * (long) Math.pow(2, retries++);
            taskScheduler.schedule(this, Instant.now().plusMillis(delay));
        }

        @Override
        public void onResult(boolean success, Throwable throwable) {
            if (success) {
                deliveryPending.decrementAndGet();
                forwardingSuccesses.increment();
            } else {
                retry(throwable);
            }
        }

        @Override
        public void run() {
            boolean sent = false;
            try {
                send();
                sent = true;
            } finally {
                if (!sent) {
                    deliveryPending.decrementAndGet();
                    forwardingFailures.increment();
                }
            }
        }
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        if (positionForwarder != null && circuitBreaker.getState() != CircuitBreaker.State.OPEN) {
            Span span = tracer.spanBuilder("position.prepare_forward").startSpan();
            try (Scope scope = span.makeCurrent()) {
                span.setAttribute("deviceId", position.getDeviceId());
                
                PositionData positionData = new PositionData();
                positionData.setPosition(position);
                positionData.setDevice(cacheManager.getObject(Device.class, position.getDeviceId()));
                
                new AsyncRequestAndCallback(positionData, span).send();
            } finally {
                span.end();
            }
        }
        callback.processed(false);
    }
}