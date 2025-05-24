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
package org.traccar.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;

import java.util.Arrays;

/**
 * Main application configuration class for the Position Processing Service.
 * This class imports and organizes all other configuration components, defines global application settings,
 * environment-specific profiles, and configures cross-cutting concerns like logging, metrics, and security.
 * 
 * The Position Processing Service is responsible for processing and enriching position data received from
 * the Protocol Service via the message broker. It applies various handlers to the position data, such as
 * geocoding, geofence checking, and distance calculation, before publishing the enriched positions back
 * to the message broker for consumption by other services like the Event Processing Service.
 */
@Configuration
@Import({
    DatabaseConfig.class,
    MessageBrokerConfig.class,
    ServiceDiscoveryConfig.class,
    CircuitBreakerConfig.class
})
public class ApplicationConfig implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationConfig.class);

    private final Environment environment;

    public ApplicationConfig(Environment environment) {
        this.environment = environment;
    }

    /**
     * Configures OpenTelemetry for distributed tracing.
     * This bean sets up the OpenTelemetry SDK with appropriate exporters and resource attributes.
     *
     * @param serviceName The name of this service for tracing context
     * @param otelEndpoint The endpoint for the OpenTelemetry collector
     * @return Configured OpenTelemetry instance
     */
    @Bean
    @ConditionalOnProperty(name = "opentelemetry.enabled", havingValue = "true")
    public OpenTelemetry openTelemetry(
            @Value("${spring.application.name}") String serviceName,
            @Value("${opentelemetry.endpoint:http://localhost:4317}") String otelEndpoint) {
        
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        ResourceAttributes.SERVICE_NAME, serviceName,
                        ResourceAttributes.SERVICE_VERSION, "1.0.0")));

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(
                        OtlpGrpcSpanExporter.builder()
                                .setEndpoint(otelEndpoint)
                                .build())
                        .build())
                .setResource(resource)
                .build();

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(PeriodicMetricReader.builder(
                        OtlpGrpcMetricExporter.builder()
                                .setEndpoint(otelEndpoint)
                                .build())
                        .build())
                .setResource(resource)
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setMeterProvider(meterProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    /**
     * Creates a Tracer bean for use throughout the application.
     *
     * @param openTelemetry The OpenTelemetry instance
     * @return Configured Tracer
     */
    @Bean
    @ConditionalOnProperty(name = "opentelemetry.enabled", havingValue = "true")
    public Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer("position-service");
    }

    /**
     * Registers JVM metrics for monitoring application performance.
     *
     * @param registry The Micrometer registry
     * @return JVM memory metrics
     */
    @Bean
    public JvmMemoryMetrics jvmMemoryMetrics(MeterRegistry registry) {
        JvmMemoryMetrics metrics = new JvmMemoryMetrics();
        metrics.bindTo(registry);
        return metrics;
    }

    /**
     * Registers JVM garbage collection metrics.
     *
     * @param registry The Micrometer registry
     * @return JVM GC metrics
     */
    @Bean
    public JvmGcMetrics jvmGcMetrics(MeterRegistry registry) {
        JvmGcMetrics metrics = new JvmGcMetrics();
        metrics.bindTo(registry);
        return metrics;
    }

    /**
     * Registers JVM thread metrics.
     *
     * @param registry The Micrometer registry
     * @return JVM thread metrics
     */
    @Bean
    public JvmThreadMetrics jvmThreadMetrics(MeterRegistry registry) {
        JvmThreadMetrics metrics = new JvmThreadMetrics();
        metrics.bindTo(registry);
        return metrics;
    }

    /**
     * Registers system processor metrics.
     *
     * @param registry The Micrometer registry
     * @return Processor metrics
     */
    @Bean
    public ProcessorMetrics processorMetrics(MeterRegistry registry) {
        ProcessorMetrics metrics = new ProcessorMetrics();
        metrics.bindTo(registry);
        return metrics;
    }

    /**
     * Registers system uptime metrics.
     *
     * @param registry The Micrometer registry
     * @return Uptime metrics
     */
    @Bean
    public UptimeMetrics uptimeMetrics(MeterRegistry registry) {
        UptimeMetrics metrics = new UptimeMetrics();
        metrics.bindTo(registry);
        return metrics;
    }

    /**
     * Provides a health indicator for the application.
     * This can be extended to include more detailed health checks.
     *
     * @return Health indicator bean
     */
    @Bean
    public HealthIndicator applicationHealthIndicator() {
        return () -> Health.up()
                .withDetail("service", "position-processing")
                .withDetail("version", "1.0.0")
                .build();
    }
    
    /**
     * Configures application-specific properties.
     * This bean can be used to access configuration properties throughout the application.
     *
     * @return Config bean with application properties
     */
    @Bean
    public Config config(@Value("${traccar.config.file:./conf/traccar.xml}") String configFile) {
        try {
            return new Config(configFile);
        } catch (Exception e) {
            logger.error("Failed to load configuration", e);
            // Provide a minimal default configuration
            return new Config();
        }
    }

    /**
     * Development-specific configuration beans.
     */
    @Configuration
    @Profile("dev")
    public static class DevelopmentConfig {
        
        @Bean
        public HealthIndicator devModeIndicator() {
            return () -> Health.up().withDetail("mode", "development").build();
        }
    }

    /**
     * Production-specific configuration beans.
     */
    @Configuration
    @Profile("prod")
    public static class ProductionConfig {
        
        @Bean
        public HealthIndicator prodModeIndicator() {
            return () -> Health.up().withDetail("mode", "production").build();
        }
    }

    /**
     * Handles application startup events.
     * Logs active profiles and initialization status.
     *
     * @param event The application ready event
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        String[] activeProfiles = environment.getActiveProfiles();
        logger.info("Position Processing Service started with active profiles: {}", 
                Arrays.toString(activeProfiles));
        logger.info("Position Processing Service initialization completed");
    }
    
    /**
     * Handles application started events.
     * This event is triggered earlier than ApplicationReadyEvent and can be used for
     * early initialization tasks.
     *
     * @param event The application started event
     */
    @EventListener
    public void onApplicationStarted(ApplicationStartedEvent event) {
        logger.info("Position Processing Service starting up");
    }
    
    /**
     * Handles application shutdown events.
     * Ensures graceful shutdown of resources.
     *
     * @param event The context closed event
     */
    @EventListener
    public void onApplicationShutdown(ContextClosedEvent event) {
        logger.info("Position Processing Service shutting down");
        // Add any cleanup operations here if needed
    }
}