/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.cloud.client.circuitbreaker.EnableCircuitBreaker;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.web.client.RestTemplate;

import brave.sampler.Sampler;
import io.micrometer.core.aop.TimedAspect;
import io.micrometer.core.instrument.MeterRegistry;
import org.traccar.messaging.NotificationChannels;

/**
 * Main entry point for the Notification Service microservice.
 * This class initializes the Spring Boot application, configures dependency injection,
 * sets up message broker connections, and registers the service with the service discovery system.
 * 
 * The Notification Service is responsible for:
 * - Consuming events from the message broker
 * - Processing notifications based on event types
 * - Delivering notifications through multiple channels (email, SMS, push, etc.)
 * - Reporting notification status back to the system
 */
@SpringBootApplication
@EnableDiscoveryClient
@EnableAsync
@EnableScheduling
@EnableCircuitBreaker
@ConfigurationPropertiesScan
@EnableBinding(NotificationChannels.class)
public class NotificationServiceApplication {

    /**
     * Main method that starts the Spring Boot application.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
    
    /**
     * Configures the TimedAspect for Micrometer metrics collection.
     * This enables the @Timed annotation for monitoring method execution times.
     * 
     * @param registry The meter registry for collecting metrics
     * @return The configured TimedAspect bean
     */
    @Bean
    public TimedAspect timedAspect(MeterRegistry registry) {
        return new TimedAspect(registry);
    }
    
    /**
     * Configures distributed tracing with 100% sampling rate for all requests.
     * This ensures that all notification processing can be traced across services.
     * 
     * @return The configured Sampler bean
     */
    @Bean
    public Sampler defaultSampler() {
        return Sampler.ALWAYS_SAMPLE;
    }
    
    /**
     * Creates a load-balanced RestTemplate for service-to-service communication.
     * The RestTemplate will use service discovery to resolve service names to instances.
     * 
     * @param builder The RestTemplateBuilder to use
     * @return The configured RestTemplate bean
     */
    @Bean
    @LoadBalanced
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.build();
    }
}