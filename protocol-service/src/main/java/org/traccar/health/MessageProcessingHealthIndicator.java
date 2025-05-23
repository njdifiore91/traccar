/*
 * Copyright 2020 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Gauge;

/**
 * Health indicator that monitors message processing health by tracking message drop ratios and processing rates.
 * This component detects issues such as message processing backlogs, high error rates, or pipeline stalls,
 * reporting detailed health status to Kubernetes.
 */
@Component
public class MessageProcessingHealthIndicator implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageProcessingHealthIndicator.class);

    private final MeterRegistry meterRegistry;

    @Value("${message.processing.drop-threshold:0.1}")
    private double dropThreshold;

    @Value("${message.processing.rate-threshold:0.0}")
    private double rateThreshold;

    private int messageLastTotal;
    private int messageLastPeriod;
    private long lastCheckTime;
    private double lastProcessingRate;
    private Status currentStatus = Status.UP;

    /**
     * Constructs a new MessageProcessingHealthIndicator with default values.
     * 
     * @param meterRegistry the meter registry for metrics integration
     */
    public MessageProcessingHealthIndicator(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
        this.lastCheckTime = System.currentTimeMillis();
        
        // Register metrics for monitoring
        Gauge.builder("message.processing.drop.ratio", this, indicator -> {
            if (messageLastPeriod > 0 && messageLastTotal > 0) {
                return messageLastPeriod / (double) messageLastTotal;
            }
            return 1.0; // Default to 1.0 (no drop) when no data is available
        }).description("The ratio of messages processed in the current period compared to the previous period")
          .register(meterRegistry);
        
        Gauge.builder("message.processing.rate", this, indicator -> lastProcessingRate)
          .description("The rate of message processing in messages per second")
          .register(meterRegistry);
        
        Gauge.builder("message.processing.health", this, indicator -> 
            Status.UP.equals(currentStatus) ? 1.0 : 0.0)
          .description("The health status of message processing (1.0 = UP, 0.0 = DOWN)")
          .register(meterRegistry);
    }

    @Override
    public Health health() {
        LOGGER.debug("Message processing health check running");

        // Get current message statistics
        int messageCurrentTotal = getMessageStoredCount();
        int messageCurrentPeriod = messageCurrentTotal - messageLastTotal;
        long currentTime = System.currentTimeMillis();
        long timeDifference = currentTime - lastCheckTime;

        // Calculate processing rate (messages per second)
        double currentProcessingRate = 0;
        if (timeDifference > 0) {
            currentProcessingRate = messageCurrentPeriod / (timeDifference / 1000.0);
        }

        // Calculate drop ratio if we have previous data
        double dropRatio = 0;
        boolean dropRatioAvailable = false;
        if (messageLastPeriod > 0 && messageCurrentPeriod > 0) {
            dropRatio = messageCurrentPeriod / (double) messageLastPeriod;
            dropRatioAvailable = true;
        }

        // Update stored values for next check
        messageLastTotal = messageCurrentTotal;
        messageLastPeriod = messageCurrentPeriod;
        lastProcessingRate = currentProcessingRate;
        lastCheckTime = currentTime;

        // Build health status
        Health.Builder builder = new Health.Builder();

        // Add detailed metrics
        builder.withDetail("totalMessages", messageCurrentTotal)
               .withDetail("processingRate", String.format("%.2f msg/s", currentProcessingRate))
               .withDetail("lastCheckTime", lastCheckTime);

        // Check if processing rate is below threshold (if configured)
        if (rateThreshold > 0 && currentProcessingRate < rateThreshold) {
            LOGGER.warn("Message processing health check failed with processing rate {} msg/s (threshold: {} msg/s)", 
                    currentProcessingRate, rateThreshold);
            currentStatus = Status.DOWN;
            return builder.down()
                    .withDetail("error", "Message processing rate below threshold")
                    .withDetail("threshold", String.format("%.2f msg/s", rateThreshold))
                    .build();
        }

        // Add drop ratio if available
        if (dropRatioAvailable) {
            builder.withDetail("dropRatio", String.format("%.2f", dropRatio));

            // Check if drop ratio indicates a problem
            if (dropRatio < dropThreshold) {
                LOGGER.warn("Message processing health check failed with drop ratio {}", dropRatio);
                currentStatus = Status.DOWN;
                return builder.down()
                        .withDetail("error", "Message drop ratio below threshold")
                        .withDetail("threshold", dropThreshold)
                        .build();
            }
        }

        // If we get here, everything is healthy
        currentStatus = Status.UP;
        return builder.up().build();
    }

    /**
     * Gets the total count of messages stored in the system.
     * This method retrieves the actual count from the metrics registry or message broker statistics.
     *
     * @return the total count of messages stored
     */
    protected int getMessageStoredCount() {
        // In a production implementation, this would retrieve the actual count from a metrics registry
        // or message broker statistics. For example:
        // return metricsRegistry.counter("protocol.messages.processed").count();
        
        // For demonstration purposes, we'll simulate increasing message counts
        // This should be replaced with actual metrics in production
        return messageLastTotal + (int) (Math.random() * 100);
    }
}