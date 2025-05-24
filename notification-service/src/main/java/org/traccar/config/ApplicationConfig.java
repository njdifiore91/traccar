/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.actuate.health.CompositeHealthContributor;
import org.springframework.boot.actuate.health.HealthContributor;
import org.springframework.boot.actuate.health.HealthIndicatorRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.web.servlet.ServletContextInitializer;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Main application configuration class for the Notification Service.
 * This class imports and organizes all other configuration components,
 * defines global application settings, environment-specific profiles,
 * and configures cross-cutting concerns like logging, metrics, and security.
 */
@Configuration
@EnableAsync
@EnableScheduling
@ComponentScan(basePackages = "org.traccar")
@Import({
    DatabaseConfig.class,
    MessageBrokerConfig.class,
    CircuitBreakerConfig.class,
    ServiceDiscoveryConfig.class
})
public class ApplicationConfig {

    private final Environment environment;

    public ApplicationConfig(Environment environment) {
        this.environment = environment;
    }

    /**
     * Configure CORS settings for the application.
     */
    @Bean
    public WebMvcConfigurer corsConfigurer() {
        return new WebMvcConfigurer() {
            @Override
            public void addCorsMappings(CorsRegistry registry) {
                registry.addMapping("/**")
                        .allowedOrigins("*")
                        .allowedMethods("GET", "POST", "PUT", "DELETE", "OPTIONS")
                        .allowedHeaders("*")
                        .maxAge(3600);
            }
        };
    }

    /**
     * Configure Prometheus metrics registry for monitoring.
     */
    @Bean
    public MeterRegistry meterRegistry() {
        PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        registry.config().commonTags("application", "notification-service");
        
        // Add JVM metrics
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        
        return registry;
    }
    
    /**
     * Customize meter registry with application-specific tags.
     */
    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCommonTags() {
        return registry -> registry.config()
                .commonTags("application", "notification-service",
                           "environment", String.join(",", environment.getActiveProfiles()));
    }
    
    /**
     * Configure composite health contributor for aggregating health indicators.
     */
    @Bean
    public CompositeHealthContributor healthContributor(HealthIndicatorRegistry registry) {
        return CompositeHealthContributor.fromMap(registry.getAll());
    }

    /**
     * Configure OpenTelemetry for distributed tracing.
     */
    @Bean
    public OpenTelemetry openTelemetry(
            @Value("${opentelemetry.service.name:notification-service}") String serviceName,
            @Value("${opentelemetry.collector.endpoint:http://localhost:4317}") String endpoint,
            @Value("${opentelemetry.sampler.probability:0.1}") double samplerProbability) {
        
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        ResourceAttributes.SERVICE_NAME, serviceName,
                        ResourceAttributes.SERVICE_VERSION, "1.0.0")));

        OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(endpoint)
                .setTimeout(Duration.ofSeconds(30))
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter)
                        .setScheduleDelay(100, TimeUnit.MILLISECONDS)
                        .build())
                .setSampler(Sampler.traceIdRatioBased(samplerProbability))
                .setResource(resource)
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(
                        W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();
    }

    /**
     * Configure application health indicator for monitoring.
     */
    @Bean
    public HealthIndicator applicationHealthIndicator() {
        return () -> Health.up()
                .withDetail("profiles", environment.getActiveProfiles())
                .withDetail("version", "1.0.0")
                .build();
    }

    /**
     * Configure servlet context initialization.
     */
    @Bean
    public ServletContextInitializer servletContextInitializer() {
        return new ServletContextInitializer() {
            @Override
            public void onStartup(ServletContext servletContext) throws ServletException {
                servletContext.setInitParameter("spring.profiles.active", 
                        String.join(",", environment.getActiveProfiles()));
            }
        };
    }

    /**
     * Configure application startup listener for initialization tasks.
     */
    @Bean
    public ApplicationListener<ApplicationReadyEvent> applicationReadyListener() {
        return event -> {
            // Log application startup information
            String[] profiles = environment.getActiveProfiles();
            String activeProfiles = profiles.length > 0 ? String.join(", ", profiles) : "default";
            System.out.println("Notification Service started with profiles: " + activeProfiles);
            
            // Register shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Notification Service shutting down...");
                // Perform any cleanup operations here
                System.out.println("Notification Service shutdown complete.");
            }));
        };
    }

    /**
     * Development-specific configuration.
     */
    @Configuration
    @Profile("dev")
    public static class DevelopmentConfig {
        
        @Bean
        @ConditionalOnProperty(name = "debug.enabled", havingValue = "true")
        public HealthIndicator debugHealthIndicator() {
            return () -> Health.up()
                    .withDetail("debug", "enabled")
                    .build();
        }
        
        /**
         * Configure more verbose logging for development environment.
         */
        @Bean
        public Object loggingConfig() {
            System.setProperty("logging.level.org.traccar", "DEBUG");
            return new Object();
        }
    }

    /**
     * Production-specific configuration.
     */
    @Configuration
    @Profile("prod")
    public static class ProductionConfig {
        
        /**
         * Configure production-specific settings.
         */
        @Bean
        public Object productionSettings() {
            // Set production-specific system properties
            System.setProperty("server.tomcat.max-threads", "200");
            System.setProperty("server.tomcat.min-spare-threads", "20");
            return new Object();
        }
        
        /**
         * Configure more restrictive CORS settings for production.
         */
        @Bean
        public WebMvcConfigurer productionCorsConfigurer() {
            return new WebMvcConfigurer() {
                @Override
                public void addCorsMappings(CorsRegistry registry) {
                    registry.addMapping("/api/**")
                            .allowedOrigins("${cors.allowed-origins:*}")
                            .allowedMethods("GET", "POST", "PUT", "DELETE")
                            .allowedHeaders("Authorization", "Content-Type")
                            .maxAge(3600);
                }
            };
        }
    }

    /**
     * Testing-specific configuration.
     */
    @Configuration
    @Profile("test")
    public static class TestConfig {
        
        /**
         * Configure in-memory dependencies for testing.
         */
        @Bean
        public Object testDependencies() {
            // Configure test-specific settings
            System.setProperty("spring.kafka.bootstrap-servers", "localhost:9092");
            System.setProperty("spring.cloud.consul.enabled", "false");
            return new Object();
        }
    }
}