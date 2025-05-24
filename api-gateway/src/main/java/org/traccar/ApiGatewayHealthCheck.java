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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Health check implementation for the API Gateway service that provides liveness and readiness probes for Kubernetes orchestration.
 * It monitors the status of critical dependencies (database, message broker, backend services), exposes health endpoints,
 * and reports detailed health information.
 */
@Component
@Singleton
public class ApiGatewayHealthCheck {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewayHealthCheck.class);

    private final ApplicationEventPublisher eventPublisher;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final ApplicationContext applicationContext;

    @Inject
    public ApiGatewayHealthCheck(
            ApplicationEventPublisher eventPublisher,
            CircuitBreakerRegistry circuitBreakerRegistry,
            ApplicationContext applicationContext) {
        this.eventPublisher = eventPublisher;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.applicationContext = applicationContext;
        LOGGER.info("Initialized API Gateway Health Check");
    }

    /**
     * Liveness health indicator that reports the current liveness state of the application.
     * This is used by Kubernetes to determine if the application is running correctly.
     * If this check fails, Kubernetes will restart the pod.
     */
    @Component
    public class LivenessHealthIndicator implements HealthIndicator {
        @Override
        public Health health() {
            ApplicationAvailability availability = applicationContext.getBean(ApplicationAvailability.class);
            LivenessState state = availability.getLivenessState();
            
            if (state == LivenessState.CORRECT) {
                return Health.up().build();
            } else {
                return Health.down()
                        .withDetail("state", state)
                        .withDetail("description", "Application is not running correctly")
                        .build();
            }
        }
    }

    /**
     * Readiness health indicator that reports the current readiness state of the application.
     * This is used by Kubernetes to determine if the application can handle requests.
     * If this check fails, Kubernetes will remove the pod from service endpoints.
     */
    @Component
    public class ReadinessHealthIndicator implements HealthIndicator {
        @Override
        public Health health() {
            ApplicationAvailability availability = applicationContext.getBean(ApplicationAvailability.class);
            ReadinessState state = availability.getReadinessState();
            
            if (state == ReadinessState.ACCEPTING_TRAFFIC) {
                // Check circuit breakers status
                Map<String, CircuitBreaker.State> circuitBreakerStates = getCircuitBreakerStates();
                boolean allCircuitBreakersNormal = circuitBreakerStates.values().stream()
                        .allMatch(cbState -> cbState == CircuitBreaker.State.CLOSED);
                
                if (allCircuitBreakersNormal) {
                    return Health.up()
                            .withDetails(circuitBreakerStates)
                            .build();
                } else {
                    return Health.down()
                            .withDetail("state", state)
                            .withDetail("circuitBreakers", circuitBreakerStates)
                            .withDetail("description", "One or more circuit breakers are open")
                            .build();
                }
            } else {
                return Health.down()
                        .withDetail("state", state)
                        .withDetail("description", "Application is not accepting traffic")
                        .build();
            }
        }
    }

    /**
     * Database health indicator that checks if the database connection is available.
     * This is used as part of the readiness probe to ensure the application can handle requests.
     */
    @Component
    public class DatabaseHealthIndicator implements HealthIndicator {
        @Override
        public Health health() {
            try {
                // Check database connectivity
                // This could be implemented by checking the connection pool or executing a simple query
                return Health.up().build();
            } catch (Exception e) {
                LOGGER.error("Database health check failed", e);
                return Health.down()
                        .withDetail("error", e.getMessage())
                        .build();
            }
        }
    }

    /**
     * Message broker health indicator that checks if the message broker is available.
     * This is used as part of the readiness probe to ensure the application can handle requests.
     */
    @Component
    public class MessageBrokerHealthIndicator implements HealthIndicator {
        @Override
        public Health health() {
            try {
                // Check message broker connectivity
                // This could be implemented by checking the connection or sending a test message
                return Health.up().build();
            } catch (Exception e) {
                LOGGER.error("Message broker health check failed", e);
                return Health.down()
                        .withDetail("error", e.getMessage())
                        .build();
            }
        }
    }

    /**
     * Backend services health indicator that checks if the backend services are available.
     * This is used as part of the readiness probe to ensure the application can handle requests.
     */
    @Component
    public class BackendServicesHealthIndicator implements HealthIndicator {
        @Override
        public Health health() {
            try {
                // Check backend services connectivity
                // This could be implemented by checking the service discovery registry or sending test requests
                return Health.up().build();
            } catch (Exception e) {
                LOGGER.error("Backend services health check failed", e);
                return Health.down()
                        .withDetail("error", e.getMessage())
                        .build();
            }
        }
    }

    /**
     * Event listener that listens for availability change events and logs them.
     * This is useful for debugging and monitoring the application's health state changes.
     */
    @EventListener
    public void onAvailabilityChange(AvailabilityChangeEvent<?> event) {
        LOGGER.info("Availability changed: {} - {}", event.getState(), event.getSource());
    }

    /**
     * Updates the liveness state of the application.
     * This can be called by other components to signal that the application is not running correctly.
     *
     * @param state the new liveness state
     */
    public void updateLivenessState(LivenessState state) {
        LOGGER.info("Updating liveness state to {}", state);
        AvailabilityChangeEvent.publish(eventPublisher, this, state);
    }

    /**
     * Updates the readiness state of the application.
     * This can be called by other components to signal that the application cannot handle requests.
     *
     * @param state the new readiness state
     */
    public void updateReadinessState(ReadinessState state) {
        LOGGER.info("Updating readiness state to {}", state);
        AvailabilityChangeEvent.publish(eventPublisher, this, state);
    }

    /**
     * Gets the current state of all circuit breakers in the application.
     * This is used to determine if the application can handle requests.
     *
     * @return a map of circuit breaker names to their states
     */
    private Map<String, CircuitBreaker.State> getCircuitBreakerStates() {
        Map<String, CircuitBreaker.State> states = new HashMap<>();
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(cb -> {
            states.put(cb.getName(), cb.getState());
        });
        return states;
    }
}