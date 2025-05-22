/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.config;

import com.google.inject.AbstractModule;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import io.github.resilience4j.micrometer.tagged.TaggedCircuitBreakerMetrics;

import io.micrometer.core.instrument.MeterRegistry;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Configuration module for Resilience4j circuit breaker integration.
 * This module configures the circuit breaker registry and registers metrics for monitoring.
 */
public class CircuitBreakerConfig extends AbstractModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreakerConfig.class);

    @Override
    protected void configure() {
        bind(CircuitBreakerRegistry.class).toProvider(CircuitBreakerRegistryProvider.class).in(Singleton.class);
    }

    /**
     * Provider for CircuitBreakerRegistry with configured circuit breakers.
     */
    public static class CircuitBreakerRegistryProvider implements Provider<CircuitBreakerRegistry> {

        private final Config config;
        private final MeterRegistry meterRegistry;

        @Inject
        public CircuitBreakerRegistryProvider(Config config, MeterRegistry meterRegistry) {
            this.config = config;
            this.meterRegistry = meterRegistry;
        }

        @Override
        public CircuitBreakerRegistry get() {
            // Create default circuit breaker configuration
            io.github.resilience4j.circuitbreaker.CircuitBreakerConfig circuitBreakerConfig = 
                io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                    .failureRateThreshold(config.getFloat("circuitbreaker.failureRateThreshold", 50.0f))
                    .slowCallRateThreshold(config.getFloat("circuitbreaker.slowCallRateThreshold", 50.0f))
                    .slowCallDurationThreshold(Duration.ofMillis(
                        config.getLong("circuitbreaker.slowCallDurationThreshold", 1000L)))
                    .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger("circuitbreaker.permittedNumberOfCallsInHalfOpenState", 10))
                    .slidingWindowType(SlidingWindowType.COUNT_BASED)
                    .slidingWindowSize(config.getInteger("circuitbreaker.slidingWindowSize", 100))
                    .minimumNumberOfCalls(config.getInteger("circuitbreaker.minimumNumberOfCalls", 10))
                    .waitDurationInOpenState(Duration.ofMillis(
                        config.getLong("circuitbreaker.waitDurationInOpenState", 60000L)))
                    .automaticTransitionFromOpenToHalfOpenEnabled(true)
                    .build();

            // Create registry with default configuration
            CircuitBreakerRegistry circuitBreakerRegistry = 
                CircuitBreakerRegistry.of(circuitBreakerConfig);

            // Create and configure specific circuit breakers
            CircuitBreaker positionServiceCircuitBreaker = circuitBreakerRegistry.circuitBreaker("positionService");
            LOGGER.info("Configured circuit breaker for positionService");

            // Register circuit breaker metrics with Micrometer
            TaggedCircuitBreakerMetrics.ofCircuitBreakerRegistry(circuitBreakerRegistry)
                .bindTo(meterRegistry);

            return circuitBreakerRegistry;
        }
    }
}