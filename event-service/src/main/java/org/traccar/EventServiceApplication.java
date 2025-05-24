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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.discovery.ServiceRegistryFactory;
import org.traccar.messaging.EventProcessingService;

/**
 * Main application class for the Event Processing Service.
 * 
 * This service is responsible for analyzing position data to detect significant events
 * such as geofence transitions, speed violations, device status changes, and maintenance alerts.
 * It implements complex event processing with stateful operations for time-window events.
 * 
 * The service consumes enriched position data from the message broker, processes it through
 * various event handlers, and publishes detected events back to the message broker for
 * consumption by the Notification Service.
 * 
 * Key responsibilities:
 * - Consuming enriched position data from the message broker
 * - Detecting events based on position data and device state
 * - Publishing detected events to the message broker
 * - Registering with service discovery for dynamic service location
 * - Exposing health check endpoints for container orchestration
 * - Collecting metrics for monitoring and alerting
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableAsync
@EnableScheduling
@ComponentScan(basePackages = {"org.traccar"})
public class EventServiceApplication implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventServiceApplication.class);
    
    private final ServiceRegistry serviceRegistry;
    private final EventProcessingService eventProcessingService;

    /**
     * Constructor for the Event Service Application.
     * 
     * @param serviceRegistryFactory Factory for creating service registry instances
     * @param eventProcessingService Service that orchestrates event processing
     */
    public EventServiceApplication(ServiceRegistryFactory serviceRegistryFactory,
                                  EventProcessingService eventProcessingService) {
        this.serviceRegistry = serviceRegistryFactory.createServiceRegistry();
        this.eventProcessingService = eventProcessingService;
        LOGGER.info("Event Processing Service initialized");
    }

    /**
     * Main method to start the Event Processing Service.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        LOGGER.info("Starting Event Processing Service application");
        SpringApplication.run(EventServiceApplication.class, args);
    }

    /**
     * Customizes the Micrometer registry with application-specific tags.
     * 
     * @return A customizer for the meter registry
     */
    @Bean
    MeterRegistryCustomizer<MeterRegistry> metricsCommonTags() {
        return registry -> registry.config()
                .commonTags("application", "event-service")
                .commonTags("service", "event-processing");
    }
    
    /**
     * Logs system information at startup.
     * 
     * @return A bean that logs system information when created
     */
    @Bean
    public Object logSystemInfo() {
        Runtime runtime = Runtime.getRuntime();
        LOGGER.info("Operating system: {} version: {} arch: {}", 
                System.getProperty("os.name"), 
                System.getProperty("os.version"), 
                System.getProperty("os.arch"));
        LOGGER.info("Java runtime: {} version: {}", 
                System.getProperty("java.runtime.name"), 
                System.getProperty("java.runtime.version"));
        LOGGER.info("Available processors: {}", runtime.availableProcessors());
        LOGGER.info("Max memory: {}MB", runtime.maxMemory() / (1024 * 1024));
        return new Object(); // Return a dummy bean
    }

    /**
     * Handles the application ready event to perform post-startup initialization.
     * 
     * This method is called when the application is fully started and ready to serve requests.
     * It registers the service with the service registry and starts the event processing pipeline.
     * 
     * @param event The application ready event
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        LOGGER.info("Event Processing Service starting up...");
        
        try {
            // Register with service discovery
            LOGGER.info("Registering with service discovery");
            serviceRegistry.register();
            
            // Start the event processing pipeline
            LOGGER.info("Starting event processing pipeline");
            eventProcessingService.start();
            
            // Add shutdown hook for graceful termination
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Event Processing Service shutting down...");
                try {
                    // Deregister from service discovery
                    LOGGER.info("Deregistering from service discovery");
                    serviceRegistry.deregister();
                    
                    // Stop the event processing pipeline
                    LOGGER.info("Stopping event processing pipeline");
                    eventProcessingService.stop();
                    
                    LOGGER.info("Event Processing Service shutdown complete");
                } catch (Exception e) {
                    LOGGER.error("Error during shutdown", e);
                }
            }));
            
            LOGGER.info("Event Processing Service started successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to start Event Processing Service", e);
            // Rethrow to allow Spring Boot to handle the failure
            throw new RuntimeException("Failed to start Event Processing Service", e);
        }
    }
}