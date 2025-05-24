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
package org.traccar;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.timelimiter.TimeLimiterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.circuitbreaker.resilience4j.ReactiveResilience4JCircuitBreakerFactory;
import org.springframework.cloud.circuitbreaker.resilience4j.Resilience4JConfigBuilder;
import org.springframework.cloud.client.circuitbreaker.Customizer;
import org.springframework.cloud.client.discovery.ReactiveDiscoveryClient;
import org.springframework.cloud.gateway.discovery.DiscoveryClientRouteDefinitionLocator;
import org.springframework.cloud.gateway.discovery.DiscoveryLocatorProperties;
import org.springframework.cloud.gateway.filter.factory.RetryGatewayFilterFactory;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.traccar.config.Config;

import java.util.HashMap;
import java.util.Map;

import java.time.Duration;

/**
 * API Gateway routing configuration that defines how incoming requests are mapped to backend microservices.
 * It integrates with service discovery to dynamically locate backend services, applies load balancing
 * across service instances, and implements circuit breakers for resilient communication.
 */
@Configuration
public class ApiGatewayRouting {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewayRouting.class);

    private final Config config;

    @Autowired
    public ApiGatewayRouting(Config config) {
        this.config = config;
    }

    /**
     * Configures the route locator for the API Gateway.
     * Defines route mappings from API endpoints to backend microservices.
     *
     * @param builder The route locator builder
     * @return The configured route locator
     */
    @Bean
    public RouteLocator routeLocator(RouteLocatorBuilder builder) {
        LOGGER.info("Configuring API Gateway routes");

        return builder.routes()
                // Position Service routes
                .route("position-service", r -> r.path("/api/positions/**", "/api/devices/*/positions")
                        .filters(f -> f
                                .retry(config -> config
                                        .setRetries(3)
                                        .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true))
                                .circuitBreaker(c -> c
                                        .setName("position-service")
                                        .setFallbackUri("forward:/fallback/position-service")))
                        .uri("lb://position-service"))

                // Event Service routes
                .route("event-service", r -> r.path("/api/events/**", "/api/notifications/**")
                        .filters(f -> f
                                .retry(config -> config
                                        .setRetries(3)
                                        .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true))
                                .circuitBreaker(c -> c
                                        .setName("event-service")
                                        .setFallbackUri("forward:/fallback/event-service")))
                        .uri("lb://event-service"))

                // Protocol Service routes
                .route("protocol-service", r -> r.path("/api/commands/**", "/api/protocols/**")
                        .filters(f -> f
                                .retry(config -> config
                                        .setRetries(3)
                                        .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true))
                                .circuitBreaker(c -> c
                                        .setName("protocol-service")
                                        .setFallbackUri("forward:/fallback/protocol-service")))
                        .uri("lb://protocol-service"))

                // Reporting Service routes
                .route("reporting-service", r -> r.path("/api/reports/**", "/api/statistics/**")
                        .filters(f -> f
                                .retry(config -> config
                                        .setRetries(2)
                                        .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true))
                                .circuitBreaker(c -> c
                                        .setName("reporting-service")
                                        .setFallbackUri("forward:/fallback/reporting-service")))
                        .uri("lb://reporting-service"))

                // Notification Service routes
                .route("notification-service", r -> r.path("/api/notification/**")
                        .filters(f -> f
                                .retry(config -> config
                                        .setRetries(3)
                                        .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true))
                                .circuitBreaker(c -> c
                                        .setName("notification-service")
                                        .setFallbackUri("forward:/fallback/notification-service")))
                        .uri("lb://notification-service"))

                // WebSocket route for real-time updates
                .route("websocket-route", r -> r.path("/api/socket")
                        .filters(f -> f
                                .circuitBreaker(c -> c
                                        .setName("websocket-service")
                                        .setFallbackUri("forward:/fallback/websocket-service")))
                        .uri("lb://api-gateway"))

                // Default route for other API endpoints
                .route("default-api", r -> r.path("/api/**")
                        .filters(f -> f
                                .retry(config -> config
                                        .setRetries(2)
                                        .setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY)
                                        .setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true))
                                .circuitBreaker(c -> c
                                        .setName("default-service")
                                        .setFallbackUri("forward:/fallback/default")))
                        .uri("lb://position-service"))
                .build();
    }

    /**
     * Configures service discovery integration for dynamic endpoint resolution.
     *
     * @param discoveryClient The reactive discovery client
     * @param properties The discovery locator properties
     * @return The discovery client route definition locator
     */
    /**
     * Configures service discovery integration for dynamic endpoint resolution.
     * This allows the API Gateway to dynamically discover and route to service instances
     * registered with the service registry (Consul or Kubernetes).
     *
     * @param discoveryClient The reactive discovery client
     * @param properties The discovery locator properties
     * @return The discovery client route definition locator
     */
    @Bean
    public DiscoveryClientRouteDefinitionLocator discoveryClientRouteDefinitionLocator(
            ReactiveDiscoveryClient discoveryClient,
            DiscoveryLocatorProperties properties) {
        LOGGER.info("Configuring service discovery integration");
        properties.setIncludeExpression("true");
        properties.setLowerCaseServiceId(true);
        return new DiscoveryClientRouteDefinitionLocator(discoveryClient, properties);
    }

    /**
     * Configures the circuit breaker factory with default settings.
     * Circuit breakers prevent cascading failures by failing fast when a service is unavailable.
     *
     * @return The circuit breaker factory customizer
     */
    @Bean
    public Customizer<ReactiveResilience4JCircuitBreakerFactory> defaultCustomizer() {
        LOGGER.info("Configuring default circuit breaker settings");
        return factory -> factory.configureDefault(id -> new Resilience4JConfigBuilder(id)
                .circuitBreakerConfig(CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(50)
                        .waitDurationInOpenState(Duration.ofSeconds(10))
                        .permittedNumberOfCallsInHalfOpenState(5)
                        .slowCallRateThreshold(50)
                        .slowCallDurationThreshold(Duration.ofSeconds(2))
                        .automaticTransitionFromOpenToHalfOpenEnabled(true)
                        .build())
                .timeLimiterConfig(TimeLimiterConfig.custom()
                        .timeoutDuration(Duration.ofSeconds(3))
                        .build())
                .build());
    }

    /**
     * Configures the position service circuit breaker with specific settings.
     * Position service has more stringent circuit breaker settings due to its critical nature.
     *
     * @return The circuit breaker factory customizer for position service
     */
    @Bean
    public Customizer<ReactiveResilience4JCircuitBreakerFactory> positionServiceCustomizer() {
        LOGGER.info("Configuring position service circuit breaker settings");
        return factory -> factory.configure(builder -> builder
                .circuitBreakerConfig(CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(40)
                        .waitDurationInOpenState(Duration.ofSeconds(5))
                        .permittedNumberOfCallsInHalfOpenState(3)
                        .automaticTransitionFromOpenToHalfOpenEnabled(true)
                        .build())
                .timeLimiterConfig(TimeLimiterConfig.custom()
                        .timeoutDuration(Duration.ofSeconds(2))
                        .build()), "position-service");
    }
    
    /**
     * Configures the event service circuit breaker with specific settings.
     *
     * @return The circuit breaker factory customizer for event service
     */
    @Bean
    public Customizer<ReactiveResilience4JCircuitBreakerFactory> eventServiceCustomizer() {
        LOGGER.info("Configuring event service circuit breaker settings");
        return factory -> factory.configure(builder -> builder
                .circuitBreakerConfig(CircuitBreakerConfig.custom()
                        .slidingWindowSize(10)
                        .failureRateThreshold(45)
                        .waitDurationInOpenState(Duration.ofSeconds(7))
                        .permittedNumberOfCallsInHalfOpenState(3)
                        .automaticTransitionFromOpenToHalfOpenEnabled(true)
                        .build())
                .timeLimiterConfig(TimeLimiterConfig.custom()
                        .timeoutDuration(Duration.ofSeconds(2))
                        .build()), "event-service");
    }

    /**
     * Configures the retry filter factory for retrying failed requests.
     * This allows the API Gateway to automatically retry failed requests to backend services.
     *
     * @return The retry filter factory
     */
    @Bean
    public RetryGatewayFilterFactory retryGatewayFilterFactory() {
        LOGGER.info("Configuring retry filter factory");
        RetryGatewayFilterFactory factory = new RetryGatewayFilterFactory();
        RetryGatewayFilterFactory.RetryConfig config = new RetryGatewayFilterFactory.RetryConfig();
        config.setRetries(3);
        config.setMethods(HttpStatus.Series.SERVER_ERROR);
        config.setStatuses(HttpStatus.INTERNAL_SERVER_ERROR, HttpStatus.BAD_GATEWAY, HttpStatus.SERVICE_UNAVAILABLE);
        config.setBackoff(Duration.ofMillis(100), Duration.ofMillis(1000), 2, true);
        factory.setDefaultConfig(config);
        return factory;
    }
    
    /**
     * Inner class that implements the fallback controller for handling circuit breaker fallbacks.
     * This controller will be called when a circuit breaker is triggered.
     */
    @RestController
    @RequestMapping("/fallback")
    public static class ApiGatewayFallbackController {
        
        private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewayFallbackController.class);
        
        /**
         * Generic fallback handler for any service
         * 
         * @param service The service that failed
         * @return A fallback response
         */
        @GetMapping("/{service}")
        public Map<String, Object> serviceFallback(@PathVariable("service") String service) {
            LOGGER.warn("Fallback triggered for service: {}", service);
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "Service temporarily unavailable");
            response.put("service", service);
            return response;
        }
        
        /**
         * Default fallback handler
         * 
         * @return A fallback response
         */
        @GetMapping("/default")
        public Map<String, Object> defaultFallback() {
            LOGGER.warn("Default fallback triggered");
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", "Service temporarily unavailable");
            return response;
        }
    }
}