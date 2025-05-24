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

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.loadbalancer.LoadBalanced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

/**
 * Configuration class for service discovery in the Notification Service.
 * This class configures service registration and discovery mechanisms
 * to enable dynamic service location and load balancing.
 */
@Configuration
@EnableDiscoveryClient
@ConditionalOnProperty(name = "service.discovery.enabled", havingValue = "true", matchIfMissing = true)
public class ServiceDiscoveryConfig {

    @Value("${spring.application.name:notification-service}")
    private String applicationName;

    @Value("${service.discovery.healthCheckPath:/actuator/health}")
    private String healthCheckPath;

    @Value("${service.discovery.healthCheckInterval:10s}")
    private String healthCheckInterval;

    /**
     * Configure load-balanced RestTemplate for service-to-service communication.
     */
    @Bean
    @LoadBalanced
    public RestTemplate loadBalancedRestTemplate() {
        return new RestTemplate();
    }

    /**
     * Configure regular RestTemplate for external service communication.
     */
    @Bean
    public RestTemplate restTemplate() {
        return new RestTemplate();
    }

    /**
     * Configure service registration properties for Consul or Kubernetes.
     * This is handled by Spring Cloud's auto-configuration, but we can customize it here.
     */
    @Bean
    @ConditionalOnProperty(name = "spring.cloud.consul.enabled", havingValue = "true")
    public Object consulRegistrationCustomizer() {
        // This bean is a placeholder for Consul registration customization
        // The actual customization is done through application properties
        return new Object();
    }

    /**
     * Configure service instance properties.
     */
    @Bean
    public Object serviceInstanceProperties() {
        // This bean is a placeholder for service instance properties
        // The actual properties are configured through application.yml
        return new Object();
    }
}