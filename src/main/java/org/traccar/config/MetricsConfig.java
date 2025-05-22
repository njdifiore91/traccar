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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration module for Micrometer metrics integration.
 * This module configures the MeterRegistry for collecting and exposing metrics.
 */
public class MetricsConfig extends AbstractModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsConfig.class);

    @Override
    protected void configure() {
        bind(MeterRegistry.class).toProvider(MeterRegistryProvider.class).in(Singleton.class);
    }

    /**
     * Provider for MeterRegistry configuration.
     */
    public static class MeterRegistryProvider implements Provider<MeterRegistry> {

        private final Config config;

        @Inject
        public MeterRegistryProvider(Config config) {
            this.config = config;
        }

        @Override
        public MeterRegistry get() {
            String metricsType = config.getString("metrics.type", "prometheus");
            
            MeterRegistry registry;
            
            if ("prometheus".equalsIgnoreCase(metricsType)) {
                LOGGER.info("Initializing Prometheus metrics registry");
                registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            } else {
                LOGGER.info("Initializing Simple metrics registry");
                registry = new SimpleMeterRegistry();
            }
            
            // Add common tags
            registry.config().commonTags(
                "application", "traccar",
                "service", config.getString("metrics.service", "api-gateway")
            );
            
            return registry;
        }
    }
}