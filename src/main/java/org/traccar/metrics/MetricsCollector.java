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
package org.traccar.metrics;

import org.traccar.config.Config;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and exposes metrics for monitoring system performance.
 * Integrates with Prometheus for metrics collection in Kubernetes.
 */
@Singleton
public class MetricsCollector {

    private final Config config;
    
    // Maps to store metric values
    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> metricDescriptions = new ConcurrentHashMap<>();

    /**
     * Creates a new metrics collector with required dependencies.
     *
     * @param config Configuration for metrics collection
     */
    @Inject
    public MetricsCollector(Config config) {
        this.config = config;
    }

    /**
     * Registers a counter metric with a description.
     *
     * @param name The name of the counter
     * @param description The description of the counter
     */
    public void registerCounter(String name, String description) {
        counters.putIfAbsent(name, new AtomicLong(0));
        metricDescriptions.put(name, description);
    }

    /**
     * Registers a gauge metric with a description.
     *
     * @param name The name of the gauge
     * @param description The description of the gauge
     */
    public void registerGauge(String name, String description) {
        gauges.putIfAbsent(name, new AtomicLong(0));
        metricDescriptions.put(name, description);
    }

    /**
     * Registers a histogram metric with a description.
     * Note: In this simplified implementation, histograms are treated as counters.
     * In a real implementation, this would use Prometheus histograms.
     *
     * @param name The name of the histogram
     * @param description The description of the histogram
     */
    public void registerHistogram(String name, String description) {
        // In a real implementation, this would register a Prometheus histogram
        // For now, we'll just register a counter for simplicity
        registerCounter(name, description);
    }

    /**
     * Increments a counter by 1.
     *
     * @param name The name of the counter to increment
     */
    public void incrementCounter(String name) {
        AtomicLong counter = counters.get(name);
        if (counter != null) {
            counter.incrementAndGet();
        }
    }

    /**
     * Increments a counter by a specified amount.
     *
     * @param name The name of the counter to increment
     * @param amount The amount to increment by
     */
    public void incrementCounter(String name, long amount) {
        AtomicLong counter = counters.get(name);
        if (counter != null) {
            counter.addAndGet(amount);
        }
    }

    /**
     * Sets a gauge to a specified value.
     *
     * @param name The name of the gauge to set
     * @param value The value to set the gauge to
     */
    public void setGauge(String name, long value) {
        AtomicLong gauge = gauges.get(name);
        if (gauge != null) {
            gauge.set(value);
        }
    }

    /**
     * Increments a gauge by 1.
     *
     * @param name The name of the gauge to increment
     */
    public void incrementGauge(String name) {
        AtomicLong gauge = gauges.get(name);
        if (gauge != null) {
            gauge.incrementAndGet();
        }
    }

    /**
     * Decrements a gauge by 1.
     *
     * @param name The name of the gauge to decrement
     */
    public void decrementGauge(String name) {
        AtomicLong gauge = gauges.get(name);
        if (gauge != null) {
            gauge.decrementAndGet();
        }
    }

    /**
     * Records a value in a histogram.
     * Note: In this simplified implementation, histograms are treated as counters.
     * In a real implementation, this would use Prometheus histograms.
     *
     * @param name The name of the histogram
     * @param value The value to record
     */
    public void recordHistogramValue(String name, long value) {
        // In a real implementation, this would record a value in a Prometheus histogram
        // For now, we'll just increment a counter for simplicity
        incrementCounter(name, value);
    }

    /**
     * Gets the current value of a counter.
     *
     * @param name The name of the counter
     * @return The current value of the counter, or 0 if not found
     */
    public long getCounterValue(String name) {
        AtomicLong counter = counters.get(name);
        return counter != null ? counter.get() : 0;
    }

    /**
     * Gets the current value of a gauge.
     *
     * @param name The name of the gauge
     * @return The current value of the gauge, or 0 if not found
     */
    public long getGaugeValue(String name) {
        AtomicLong gauge = gauges.get(name);
        return gauge != null ? gauge.get() : 0;
    }

    /**
     * Gets the description of a metric.
     *
     * @param name The name of the metric
     * @return The description of the metric, or null if not found
     */
    public String getMetricDescription(String name) {
        return metricDescriptions.get(name);
    }

    /**
     * Generates a Prometheus-compatible metrics response.
     * This would be exposed via an HTTP endpoint for Prometheus scraping.
     *
     * @return A string containing all metrics in Prometheus format
     */
    public String getPrometheusMetrics() {
        StringBuilder sb = new StringBuilder();
        
        // Add counters
        for (String name : counters.keySet()) {
            String description = metricDescriptions.getOrDefault(name, "");
            sb.append("# HELP ").append(name).append(" ").append(description).append("\n");
            sb.append("# TYPE ").append(name).append(" counter\n");
            sb.append(name).append(" ").append(getCounterValue(name)).append("\n");
        }
        
        // Add gauges
        for (String name : gauges.keySet()) {
            String description = metricDescriptions.getOrDefault(name, "");
            sb.append("# HELP ").append(name).append(" ").append(description).append("\n");
            sb.append("# TYPE ").append(name).append(" gauge\n");
            sb.append(name).append(" ").append(getGaugeValue(name)).append("\n");
        }
        
        return sb.toString();
    }
}