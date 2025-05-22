/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.speedlimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Metrics collection for speed limit providers.
 * Collects and exposes metrics for monitoring and alerting.
 */
@Singleton
public class SpeedLimitMetrics {

    private static final Logger LOGGER = Logger.getLogger(SpeedLimitMetrics.class.getName());

    private final MeterRegistry registry;

    /**
     * Constructs a new SpeedLimitMetrics with the given registry.
     *
     * @param registry The meter registry to use
     */
    @Inject
    public SpeedLimitMetrics(MeterRegistry registry) {
        this.registry = registry;
        LOGGER.info("Initialized SpeedLimitMetrics");
    }

    /**
     * Records a successful speed limit lookup.
     *
     * @param providerName The name of the speed limit provider
     * @param durationMs The duration of the lookup in milliseconds
     */
    public void recordSuccess(String providerName, long durationMs) {
        List<Tag> tags = Arrays.asList(
                Tag.of("provider", providerName),
                Tag.of("outcome", "success")
        );

        Counter.builder("speedlimit.lookup.total")
                .tags(tags)
                .description("Total number of speed limit lookups")
                .register(registry)
                .increment();

        Timer.builder("speedlimit.lookup.duration")
                .tags(tags)
                .description("Speed limit lookup duration")
                .register(registry)
                .record(durationMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Records a failed speed limit lookup.
     *
     * @param providerName The name of the speed limit provider
     * @param errorType The type of error that occurred
     * @param durationMs The duration of the lookup in milliseconds
     */
    public void recordFailure(String providerName, String errorType, long durationMs) {
        List<Tag> tags = Arrays.asList(
                Tag.of("provider", providerName),
                Tag.of("outcome", "failure"),
                Tag.of("error", errorType)
        );

        Counter.builder("speedlimit.lookup.total")
                .tags(tags)
                .description("Total number of speed limit lookups")
                .register(registry)
                .increment();

        Timer.builder("speedlimit.lookup.duration")
                .tags(tags)
                .description("Speed limit lookup duration")
                .register(registry)
                .record(durationMs, TimeUnit.MILLISECONDS);

        Counter.builder("speedlimit.lookup.errors")
                .tags(tags)
                .description("Speed limit lookup errors")
                .register(registry)
                .increment();
    }

    /**
     * Records the current state of a circuit breaker.
     *
     * @param providerName The name of the speed limit provider
     * @param state The state of the circuit breaker
     */
    public void recordCircuitBreakerState(String providerName, String state) {
        List<Tag> tags = Arrays.asList(
                Tag.of("provider", providerName),
                Tag.of("state", state)
        );

        registry.gauge("speedlimit.circuitbreaker.state", tags, 1.0);
    }

    /**
     * Records circuit breaker metrics.
     *
     * @param providerName The name of the speed limit provider
     * @param failureRate The failure rate of the circuit breaker
     * @param slowCallRate The slow call rate of the circuit breaker
     * @param numberOfBufferedCalls The number of buffered calls in the circuit breaker
     * @param numberOfFailedCalls The number of failed calls in the circuit breaker
     */
    public void recordCircuitBreakerMetrics(
            String providerName,
            float failureRate,
            float slowCallRate,
            int numberOfBufferedCalls,
            int numberOfFailedCalls) {

        List<Tag> tags = Arrays.asList(Tag.of("provider", providerName));

        registry.gauge("speedlimit.circuitbreaker.failure.rate", tags, failureRate);
        registry.gauge("speedlimit.circuitbreaker.slow.rate", tags, slowCallRate);
        registry.gauge("speedlimit.circuitbreaker.buffered.calls", tags, numberOfBufferedCalls);
        registry.gauge("speedlimit.circuitbreaker.failed.calls", tags, numberOfFailedCalls);
    }

    /**
     * Records the health status of a speed limit provider.
     *
     * @param providerName The name of the speed limit provider
     * @param status The health status of the provider
     */
    public void recordHealthStatus(String providerName, SpeedLimitProvider.HealthStatus status) {
        List<Tag> tags = Arrays.asList(
                Tag.of("provider", providerName),
                Tag.of("status", status.name())
        );

        registry.gauge("speedlimit.health.status", tags, status == SpeedLimitProvider.HealthStatus.HEALTHY ? 1.0 :
                status == SpeedLimitProvider.HealthStatus.DEGRADED ? 0.5 : 0.0);
    }
}