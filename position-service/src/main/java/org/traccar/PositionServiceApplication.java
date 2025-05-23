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
package org.traccar;

import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for the Position Service.
 * 
 * This class initializes the Spring Boot application, configures OpenTelemetry for distributed
 * tracing, and registers the service with the service discovery mechanism.
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableScheduling
public class PositionServiceApplication extends SpringBootServletInitializer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionServiceApplication.class);

    /**
     * Main method to start the Position Service application.
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(PositionServiceApplication.class, args);
    }

    /**
     * Configure OpenTelemetry for distributed tracing.
     *
     * @param spanExporter Span exporter for sending traces to a collector
     * @param serviceName Service name for tracing
     * @return OpenTelemetry instance
     */
    @Bean
    public OpenTelemetry openTelemetry(
            SpanExporter spanExporter,
            @Value("${spring.application.name}") String serviceName) {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.builder()
                        .put("service.name", serviceName)
                        .build()));
        
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .setResource(resource)
                .setSampler(Sampler.alwaysOn())
                .build();
        
        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(
                        TextMapPropagator.composite(W3CTraceContextPropagator.getInstance())))
                .build();
    }

    /**
     * Register service metrics with the meter registry.
     *
     * @param meterRegistry Meter registry
     * @param serviceName Service name for metrics
     */
    @Bean
    public void registerServiceMetrics(
            MeterRegistry meterRegistry,
            @Value("${spring.application.name}") String serviceName) {
        meterRegistry.config().commonTags("service", serviceName);
        LOGGER.info("Registered service metrics for {}", serviceName);
    }

    /**
     * Log application startup information.
     *
     * @param serviceName Service name
     * @param servicePort Service port
     */
    @Bean
    public void logStartupInfo(
            @Value("${spring.application.name}") String serviceName,
            @Value("${server.port}") int servicePort) {
        LOGGER.info("{} started on port {}", serviceName, servicePort);
    }
}