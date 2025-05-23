/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthEndpointGroups;
import org.springframework.boot.actuate.health.Status;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the HealthConfiguration class which provides configuration for the Protocol Service health check system.
 * This test verifies that health check thresholds, intervals, and component-specific settings are correctly loaded
 * and applied. It ensures that default values are used when configuration is missing and that configuration changes
 * are properly reflected in the health check system.
 */
public class HealthConfigurationTest {

    /**
     * Tests that default values are applied when configuration properties are not explicitly set.
     */
    @SpringBootTest(classes = HealthConfiguration.class)
    public static class DefaultConfigurationTest {

        @Autowired
        private HealthConfiguration healthConfiguration;

        @Autowired
        private HealthEndpointGroups healthEndpointGroups;

        @Test
        public void testDefaultValues() {
            // Verify default values are applied
            assertEquals(0.1, healthConfiguration.getMessageDropThreshold(), 0.001);
            assertEquals(30000, healthConfiguration.getHealthCheckInterval());
            assertEquals(5000, healthConfiguration.getHealthCheckTimeout());
            assertEquals(300000, healthConfiguration.getGracePeriod());
            assertTrue(healthConfiguration.isSystemdWatchdogEnabled());

            // Verify health endpoint groups are configured
            assertTrue(healthEndpointGroups.getNames().contains("liveness"));
            assertTrue(healthEndpointGroups.getNames().contains("readiness"));
            assertTrue(healthEndpointGroups.getNames().contains("startup"));
        }

        @Test
        public void testLivenessGroupConfiguration() {
            // Verify liveness group includes the expected health indicators
            var group = healthEndpointGroups.getGroup("liveness");
            assertTrue(group.getIncludeHealthIndicators().contains("livenessState"));
            assertTrue(group.getIncludeHealthIndicators().contains("diskSpace"));

            // Test status aggregation logic
            var aggregator = group.getStatusAggregator();
            assertEquals(Status.DOWN, aggregator.apply(Set.of(Status.UP, Status.DOWN)));
            assertEquals(Status.DOWN, aggregator.apply(Set.of(Status.DOWN, Status.OUT_OF_SERVICE)));
            assertEquals(Status.UP, aggregator.apply(Set.of(Status.UP, Status.UNKNOWN)));
        }

        @Test
        public void testReadinessGroupConfiguration() {
            // Verify readiness group includes the expected health indicators
            var group = healthEndpointGroups.getGroup("readiness");
            assertTrue(group.getIncludeHealthIndicators().contains("readinessState"));
            assertTrue(group.getIncludeHealthIndicators().contains("messageProcessing"));
            assertTrue(group.getIncludeHealthIndicators().contains("discoveryClient"));
            assertTrue(group.getIncludeHealthIndicators().contains("messageBroker"));

            // Test status aggregation logic
            var aggregator = group.getStatusAggregator();
            assertEquals(Status.OUT_OF_SERVICE, aggregator.apply(Set.of(Status.UP, Status.DOWN)));
            assertEquals(Status.OUT_OF_SERVICE, aggregator.apply(Set.of(Status.UP, Status.OUT_OF_SERVICE)));
            assertEquals(Status.UP, aggregator.apply(Set.of(Status.UP)));
        }

        @Test
        public void testStartupGroupConfiguration() {
            // Verify startup group includes the expected health indicators
            var group = healthEndpointGroups.getGroup("startup");
            assertTrue(group.getIncludeHealthIndicators().contains("startupState"));

            // Test status aggregation logic
            var aggregator = group.getStatusAggregator();
            assertEquals(Status.DOWN, aggregator.apply(Set.of(Status.UP, Status.DOWN)));
            assertEquals(Status.UP, aggregator.apply(Set.of(Status.UP)));
        }
    }

    /**
     * Tests that custom configuration values are correctly applied when explicitly set.
     */
    @SpringBootTest(classes = HealthConfiguration.class)
    @TestPropertySource(properties = {
            "message.processing.drop-threshold=0.05",
            "health.check.interval=15000",
            "health.check.timeout=2000",
            "health.systemd.watchdog.enabled=false",
            "health.grace-period=60000"
    })
    public static class CustomConfigurationTest {

        @Autowired
        private HealthConfiguration healthConfiguration;

        @Test
        public void testCustomValues() {
            // Verify custom values are applied
            assertEquals(0.05, healthConfiguration.getMessageDropThreshold(), 0.001);
            assertEquals(15000, healthConfiguration.getHealthCheckInterval());
            assertEquals(2000, healthConfiguration.getHealthCheckTimeout());
            assertEquals(60000, healthConfiguration.getGracePeriod());
            assertFalse(healthConfiguration.isSystemdWatchdogEnabled());
        }
    }

    /**
     * Tests that environment variables can override configuration properties.
     */
    @SpringBootTest(classes = HealthConfiguration.class)
    @TestConfiguration
    @Import(HealthConfiguration.class)
    public static class EnvironmentVariableTest {

        @Bean
        public static org.springframework.context.support.PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
            // This simulates environment variables by setting system properties
            System.setProperty("MESSAGE_PROCESSING_DROP_THRESHOLD", "0.2");
            System.setProperty("HEALTH_CHECK_INTERVAL", "60000");
            System.setProperty("HEALTH_CHECK_TIMEOUT", "10000");
            System.setProperty("HEALTH_SYSTEMD_WATCHDOG_ENABLED", "false");
            System.setProperty("HEALTH_GRACE_PERIOD", "120000");

            return new org.springframework.context.support.PropertySourcesPlaceholderConfigurer();
        }

        @Autowired
        private HealthConfiguration healthConfiguration;

        @Test
        public void testEnvironmentVariables() {
            // Verify environment variables override default values
            // Note: In a real environment, Spring Boot would automatically convert environment variables
            // to properties, but in a test we need to simulate this behavior
            assertEquals(0.2, healthConfiguration.getMessageDropThreshold(), 0.001);
            assertEquals(60000, healthConfiguration.getHealthCheckInterval());
            assertEquals(10000, healthConfiguration.getHealthCheckTimeout());
            assertEquals(120000, healthConfiguration.getGracePeriod());
            assertFalse(healthConfiguration.isSystemdWatchdogEnabled());

            // Clean up system properties after test
            System.clearProperty("MESSAGE_PROCESSING_DROP_THRESHOLD");
            System.clearProperty("HEALTH_CHECK_INTERVAL");
            System.clearProperty("HEALTH_CHECK_TIMEOUT");
            System.clearProperty("HEALTH_SYSTEMD_WATCHDOG_ENABLED");
            System.clearProperty("HEALTH_GRACE_PERIOD");
        }
    }

    /**
     * Tests integration with MessageProcessingHealthIndicator to ensure that configuration
     * values are correctly applied to health checks.
     */
    @SpringBootTest(classes = {HealthConfiguration.class, MessageProcessingHealthIndicatorTestConfig.class})
    @TestPropertySource(properties = {"message.processing.drop-threshold=0.15"})
    public static class MessageProcessingIntegrationTest {

        @Autowired
        private HealthConfiguration healthConfiguration;

        @Autowired
        private MessageProcessingHealthIndicator healthIndicator;

        @Test
        public void testMessageProcessingIntegration() {
            // Verify that the message drop threshold is correctly applied to the health indicator
            assertEquals(0.15, healthConfiguration.getMessageDropThreshold(), 0.001);
            assertEquals(0.15, healthIndicator.getDropThreshold(), 0.001);
        }
    }

    /**
     * Test configuration for MessageProcessingHealthIndicator tests.
     */
    @TestConfiguration
    public static class MessageProcessingHealthIndicatorTestConfig {

        @Bean
        public io.micrometer.core.instrument.MeterRegistry meterRegistry() {
            return io.micrometer.core.instrument.simple.SimpleMeterRegistry.builder().build();
        }

        @Bean
        public MessageProcessingHealthIndicator messageProcessingHealthIndicator(io.micrometer.core.instrument.MeterRegistry meterRegistry) {
            return new TestMessageProcessingHealthIndicator(meterRegistry);
        }

        /**
         * Test implementation of MessageProcessingHealthIndicator that exposes the drop threshold.
         */
        public static class TestMessageProcessingHealthIndicator extends MessageProcessingHealthIndicator {

            public TestMessageProcessingHealthIndicator(io.micrometer.core.instrument.MeterRegistry meterRegistry) {
                super(meterRegistry);
            }

            public double getDropThreshold() {
                return this.dropThreshold;
            }

            @Override
            protected int getMessageStoredCount() {
                // Return a constant value for testing
                return 100;
            }
        }
    }
}