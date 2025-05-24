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
package org.traccar;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import org.eclipse.jetty.server.handler.StatisticsHandler;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ServletWebServerFactory;
import org.springframework.boot.context.ApplicationPidFileWriter;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import java.util.concurrent.TimeUnit;

/**
 * Main application class that bootstraps the API Gateway service.
 * It initializes the Spring Boot application, configures the embedded web server,
 * sets up service discovery registration, and starts the WebSocket server.
 * This class is the entry point for the API Gateway microservice and orchestrates
 * the startup and shutdown of all gateway components.
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableWebMvc
@Configuration
public class ApiGatewayApplication implements ApplicationListener<ContextClosedEvent> {

    /**
     * Main method to start the API Gateway application.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        SpringApplication application = new SpringApplication(ApiGatewayApplication.class);
        
        // Add PID file writer to create a file containing the application PID
        // This can be used by external process managers to monitor and control the application
        application.addListeners(new ApplicationPidFileWriter("api-gateway.pid"));
        
        application.run(args);
    }
    
    /**
     * Executed when the application is fully started and ready to service requests.
     * Logs information about the application startup.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        System.out.println("API Gateway is now ready to accept requests");
    }

    /**
     * Configures the embedded Jetty server with WebSocket support.
     * 
     * @return Configured ServletWebServerFactory
     */
    @Bean
    public ServletWebServerFactory servletWebServerFactory(
            @Value("${server.shutdown.grace-period:30s}") String shutdownGracePeriod) {
        JettyServletWebServerFactory factory = new JettyServletWebServerFactory();
        // Configure Jetty for WebSocket support
        factory.addServerCustomizers(server -> {
            // Enable WebSocket support
            server.addBean(new org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer.Configurator());
            
            // Add StatisticsHandler for proper graceful shutdown
            StatisticsHandler statisticsHandler = new StatisticsHandler();
            statisticsHandler.setHandler(server.getHandler());
            server.setHandler(statisticsHandler);
            
            // Parse shutdown grace period (default 30 seconds)
            long gracePeriodMillis = parseTimeToMillis(shutdownGracePeriod);
            
            // Configure graceful shutdown
            server.setStopTimeout(gracePeriodMillis);
            server.setStopAtShutdown(true);
        });
        return factory;
    }
    
    /**
     * Parses a time string like "30s" or "1m" to milliseconds.
     * 
     * @param timeString Time string with unit suffix (s, m, h)
     * @return Time in milliseconds
     */
    private long parseTimeToMillis(String timeString) {
        if (timeString == null || timeString.isEmpty()) {
            return TimeUnit.SECONDS.toMillis(30); // Default 30 seconds
        }
        
        String value = timeString.replaceAll("[^\\d.]", "");
        String unit = timeString.replaceAll("[\\d.]", "");
        
        double numericValue = Double.parseDouble(value);
        
        return switch (unit.toLowerCase()) {
            case "ms" -> (long) numericValue;
            case "s" -> (long) (numericValue * 1000);
            case "m" -> (long) (numericValue * 60 * 1000);
            case "h" -> (long) (numericValue * 60 * 60 * 1000);
            default -> (long) (numericValue * 1000); // Default to seconds
        };
    }
    
    /**
     * Customizes the Micrometer registry with application-specific tags.
     * 
     * @param environment Spring environment for accessing properties
     * @return MeterRegistryCustomizer for configuring metrics
     */
    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCommonTags(Environment environment) {
        return registry -> registry.config()
                .commonTags("application", "api-gateway")
                .commonTags("service", "api-gateway")
                .commonTags("env", environment.getProperty("spring.profiles.active", "default"));
    }
    
    /**
     * Configures JVM garbage collection metrics collection.
     * 
     * @return JvmGcMetrics for monitoring garbage collection
     */
    @Bean
    public JvmGcMetrics jvmGcMetrics() {
        return new JvmGcMetrics();
    }
    
    /**
     * Configures JVM memory metrics collection.
     * 
     * @return JvmMemoryMetrics for monitoring memory usage
     */
    @Bean
    public JvmMemoryMetrics jvmMemoryMetrics() {
        return new JvmMemoryMetrics();
    }
    
    /**
     * Configures processor metrics collection.
     * 
     * @return ProcessorMetrics for monitoring CPU usage
     */
    @Bean
    public ProcessorMetrics processorMetrics() {
        return new ProcessorMetrics();
    }
    
    /**
     * Handles application shutdown event to ensure graceful termination.
     * This method is called when the Spring context is being closed.
     * 
     * @param event The ContextClosedEvent triggered during application shutdown
     */
    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        // Log shutdown initiation
        System.out.println("API Gateway is shutting down gracefully...");
        System.out.println("Waiting for active requests to complete...");
        
        // Additional shutdown logic can be added here if needed
        // For example, custom resource cleanup or notification to other services
    }
}