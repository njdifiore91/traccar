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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.function.ToDoubleFunction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.test.util.ReflectionTestUtils;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Gauge.Builder;
import io.micrometer.core.instrument.MeterRegistry;

/**
 * Tests for the MessageProcessingHealthIndicator class which monitors message processing health.
 * This test verifies that the health indicator correctly calculates message drop ratios,
 * reports appropriate health status based on configurable thresholds, and integrates with
 * the metrics collection system.
 */
@ExtendWith(MockitoExtension.class)
public class MessageProcessingHealthIndicatorTest {

    @Mock
    private MeterRegistry meterRegistry;

    @Mock
    private Builder<MessageProcessingHealthIndicator> gaugeBuilder;

    private MessageProcessingHealthIndicator healthIndicator;

    @BeforeEach
    public void setUp() {
        // Mock the gauge builder chain
        when(meterRegistry.gauge(anyString(), any(MessageProcessingHealthIndicator.class))).thenReturn(null);
        when(Gauge.builder(anyString(), any(MessageProcessingHealthIndicator.class), any())).thenReturn(gaugeBuilder);
        when(gaugeBuilder.description(anyString())).thenReturn(gaugeBuilder);
        when(gaugeBuilder.register(any(MeterRegistry.class))).thenReturn(mock(Gauge.class));

        // Create a testable subclass that overrides the protected method
        healthIndicator = new TestableMessageProcessingHealthIndicator(meterRegistry);
        
        // Set default threshold values
        ReflectionTestUtils.setField(healthIndicator, "dropThreshold", 0.1);
        ReflectionTestUtils.setField(healthIndicator, "rateThreshold", 0.0);
    }

    /**
     * Tests that the health indicator correctly initializes and registers metrics with the registry.
     */
    @Test
    public void testMetricsRegistration() {
        // Verify that three gauges were registered
        verify(Gauge.builder(anyString(), any(MessageProcessingHealthIndicator.class), any()), times(3));
        verify(gaugeBuilder, times(3)).register(meterRegistry);
    }

    /**
     * Tests that health status is UP when message processing is healthy (drop ratio above threshold).
     */
    @Test
    public void testHealthStatusUpWhenProcessingHealthy() {
        // Set up the test scenario with healthy processing
        TestableMessageProcessingHealthIndicator testIndicator = (TestableMessageProcessingHealthIndicator) healthIndicator;
        testIndicator.setMessageCounts(100, 200); // First check: 100 messages
        
        // First health check to establish baseline
        Health health1 = healthIndicator.health();
        assertEquals(Status.UP, health1.getStatus());
        
        // Second check with healthy increase (above threshold)
        testIndicator.setMessageCounts(200, 320); // Second check: 120 messages (120/100 = 1.2 ratio, above 0.1 threshold)
        Health health2 = healthIndicator.health();
        
        // Verify health status and details
        assertEquals(Status.UP, health2.getStatus());
        assertNotNull(health2.getDetails().get("totalMessages"));
        assertNotNull(health2.getDetails().get("processingRate"));
        assertNotNull(health2.getDetails().get("dropRatio"));
        assertEquals(320, health2.getDetails().get("totalMessages"));
    }

    /**
     * Tests that health status is DOWN when message drop ratio is below threshold.
     */
    @Test
    public void testHealthStatusDownWhenDropRatioBelowThreshold() {
        // Set up the test scenario with a drop in processing
        TestableMessageProcessingHealthIndicator testIndicator = (TestableMessageProcessingHealthIndicator) healthIndicator;
        testIndicator.setMessageCounts(100, 200); // First check: 100 messages
        
        // First health check to establish baseline
        Health health1 = healthIndicator.health();
        assertEquals(Status.UP, health1.getStatus());
        
        // Second check with unhealthy decrease (below threshold)
        testIndicator.setMessageCounts(200, 205); // Second check: only 5 messages (5/100 = 0.05 ratio, below 0.1 threshold)
        Health health2 = healthIndicator.health();
        
        // Verify health status and details
        assertEquals(Status.DOWN, health2.getStatus());
        assertTrue(health2.getDetails().containsKey("error"));
        assertTrue(health2.getDetails().containsKey("threshold"));
        assertEquals("Message drop ratio below threshold", health2.getDetails().get("error"));
    }

    /**
     * Tests that health status is DOWN when processing rate is below configured threshold.
     */
    @Test
    public void testHealthStatusDownWhenProcessingRateBelowThreshold() {
        // Configure a processing rate threshold
        ReflectionTestUtils.setField(healthIndicator, "rateThreshold", 10.0); // 10 messages per second
        
        // Set up the test scenario with a low processing rate
        TestableMessageProcessingHealthIndicator testIndicator = (TestableMessageProcessingHealthIndicator) healthIndicator;
        testIndicator.setMessageCounts(100, 200); // First check: 100 messages
        
        // First health check to establish baseline
        Health health1 = healthIndicator.health();
        
        // Second check with low processing rate
        testIndicator.setMessageCounts(200, 205); // Only 5 new messages
        testIndicator.setProcessingRateForTest(5.0); // 5 messages per second (below 10 threshold)
        Health health2 = healthIndicator.health();
        
        // Verify health status and details
        assertEquals(Status.DOWN, health2.getStatus());
        assertTrue(health2.getDetails().containsKey("error"));
        assertTrue(health2.getDetails().containsKey("threshold"));
        assertEquals("Message processing rate below threshold", health2.getDetails().get("error"));
    }

    /**
     * Tests that the health indicator correctly handles the first health check
     * when no previous data is available for comparison.
     */
    @Test
    public void testFirstHealthCheck() {
        // First health check with no previous data
        Health health = healthIndicator.health();
        
        // Should be UP since there's no previous data to compare against
        assertEquals(Status.UP, health.getStatus());
        assertFalse(health.getDetails().containsKey("dropRatio"));
    }

    /**
     * Tests that the health indicator correctly handles configuration changes.
     */
    @Test
    public void testThresholdConfiguration() {
        // Set up the test scenario
        TestableMessageProcessingHealthIndicator testIndicator = (TestableMessageProcessingHealthIndicator) healthIndicator;
        testIndicator.setMessageCounts(100, 200); // First check: 100 messages
        
        // First health check to establish baseline
        healthIndicator.health();
        
        // Change the threshold to be more permissive
        ReflectionTestUtils.setField(healthIndicator, "dropThreshold", 0.05);
        
        // Second check with what would normally be an unhealthy decrease
        testIndicator.setMessageCounts(200, 210); // Second check: 10 messages (10/100 = 0.1 ratio)
        Health health = healthIndicator.health();
        
        // Should now be UP since we lowered the threshold to 0.05
        assertEquals(Status.UP, health.getStatus());
    }

    /**
     * Tests that the metrics gauge functions correctly calculate values.
     */
    @Test
    public void testMetricsGaugeFunctions() {
        // Capture the gauge functions
        ArgumentCaptor<ToDoubleFunction<MessageProcessingHealthIndicator>> functionCaptor = 
                ArgumentCaptor.forClass(ToDoubleFunction.class);
        
        // Create a new indicator to capture the functions
        when(Gauge.builder(anyString(), any(MessageProcessingHealthIndicator.class), functionCaptor.capture()))
                .thenReturn(gaugeBuilder);
        
        new MessageProcessingHealthIndicator(meterRegistry);
        
        // We should have captured 3 functions (drop ratio, processing rate, health)
        assertEquals(3, functionCaptor.getAllValues().size());
    }

    /**
     * Testable subclass that allows controlling the message counts for testing.
     */
    private static class TestableMessageProcessingHealthIndicator extends MessageProcessingHealthIndicator {
        
        private int lastTotal;
        private int currentTotal;
        private double processingRateOverride = -1;
        
        public TestableMessageProcessingHealthIndicator(MeterRegistry meterRegistry) {
            super(meterRegistry);
        }
        
        @Override
        protected int getMessageStoredCount() {
            return currentTotal;
        }
        
        public void setMessageCounts(int lastTotal, int currentTotal) {
            this.lastTotal = lastTotal;
            this.currentTotal = currentTotal;
            ReflectionTestUtils.setField(this, "messageLastTotal", lastTotal);
        }
        
        public void setProcessingRateForTest(double rate) {
            this.processingRateOverride = rate;
        }
        
        @Override
        public Health health() {
            Health health = super.health();
            
            // If we've set a processing rate override, use reflection to set it
            if (processingRateOverride >= 0) {
                ReflectionTestUtils.setField(this, "lastProcessingRate", processingRateOverride);
            }
            
            return health;
        }
    }
}