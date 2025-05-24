/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.metrics;

import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Histogram;
import io.prometheus.metrics.model.registry.PrometheusRegistry;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Metrics related to position processing in the Position Service.
 * Tracks position throughput, processing times, and success/failure rates using Prometheus metrics.
 */
@Singleton
public class PositionMetrics {

    private final PrometheusRegistry registry;

    // Position throughput counters
    private final Counter positionsReceivedTotal;
    private final Counter positionsProcessedTotal;
    private final Counter positionsErrorTotal;

    // Position processing time histogram
    private final Histogram positionProcessingDurationSeconds;

    /**
     * Creates a new instance of PositionMetrics with the specified Prometheus registry.
     *
     * @param registry The Prometheus registry to register metrics with
     */
    @Inject
    public PositionMetrics(PrometheusRegistry registry) {
        this.registry = registry;

        // Initialize position throughput counters
        positionsReceivedTotal = Counter.builder()
                .name("position_messages_received_total")
                .help("Total number of position messages received")
                .labelNames("protocol", "source")
                .register(registry);

        positionsProcessedTotal = Counter.builder()
                .name("position_messages_processed_total")
                .help("Total number of position messages successfully processed")
                .labelNames("protocol", "stage")
                .register(registry);

        positionsErrorTotal = Counter.builder()
                .name("position_messages_error_total")
                .help("Total number of position messages that failed processing")
                .labelNames("protocol", "stage", "error_type")
                .register(registry);

        // Initialize position processing time histogram
        positionProcessingDurationSeconds = Histogram.builder()
                .name("position_processing_duration_seconds")
                .help("Time taken to process position messages in seconds")
                .labelNames("protocol", "stage")
                .register(registry);
    }

    /**
     * Records a position message being received.
     *
     * @param protocol The protocol used to receive the position
     * @param source The source of the position (e.g., "tcp", "udp")
     */
    public void recordPositionReceived(String protocol, String source) {
        positionsReceivedTotal.labelValues(protocol, source).inc();
    }

    /**
     * Records a position message being successfully processed at a specific stage.
     *
     * @param protocol The protocol used to receive the position
     * @param stage The processing stage (e.g., "decode", "validate", "store")
     */
    public void recordPositionProcessed(String protocol, String stage) {
        positionsProcessedTotal.labelValues(protocol, stage).inc();
    }

    /**
     * Records a position message processing error.
     *
     * @param protocol The protocol used to receive the position
     * @param stage The processing stage where the error occurred
     * @param errorType The type of error that occurred
     */
    public void recordPositionError(String protocol, String stage, String errorType) {
        positionsErrorTotal.labelValues(protocol, stage, errorType).inc();
    }

    /**
     * Starts timing the processing of a position message.
     * Returns a timer that should be used with {@link #recordProcessingTime}.
     *
     * @return A timer object to be used with recordProcessingTime
     */
    public Histogram.Timer startPositionProcessingTimer() {
        return positionProcessingDurationSeconds.startTimer();
    }

    /**
     * Records the time taken to process a position message at a specific stage.
     *
     * @param timer The timer returned by startPositionProcessingTimer
     * @param protocol The protocol used to receive the position
     * @param stage The processing stage being timed
     */
    public void recordProcessingTime(Histogram.Timer timer, String protocol, String stage) {
        timer.observeDuration(positionProcessingDurationSeconds.labelValues(protocol, stage));
    }

    /**
     * Measures and records the time taken to execute a specific processing stage.
     * This is a convenience method that combines starting a timer and observing its duration.
     *
     * @param protocol The protocol used to receive the position
     * @param stage The processing stage being timed
     * @param runnable The code to execute and time
     */
    public void timeProcessingStage(String protocol, String stage, Runnable runnable) {
        Histogram.Timer timer = positionProcessingDurationSeconds.labelValues(protocol, stage).startTimer();
        try {
            runnable.run();
        } finally {
            timer.observeDuration();
        }
    }
}