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
package org.traccar.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Exposes standardized health check endpoints compatible with Kubernetes liveness, readiness, and startup probes.
 * This controller provides HTTP endpoints that return appropriate HTTP status codes based on the service's health state.
 * <p>
 * The endpoints are:
 * - /actuator/health/live: Used for liveness probes to detect hung or deadlocked services
 * - /actuator/health/ready: Used for readiness probes to determine if service can handle traffic
 * - /actuator/health/startup: Used for startup probes to allow for longer initialization periods
 */
@RestController
@RequestMapping("/actuator/health")
public class HealthController {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthController.class);

    private final HealthEndpoint healthEndpoint;

    /**
     * Constructs a new HealthController with the specified dependencies.
     *
     * @param healthEndpoint the health endpoint that provides access to health indicators
     */
    @Autowired
    public HealthController(HealthEndpoint healthEndpoint) {
        this.healthEndpoint = healthEndpoint;
    }

    /**
     * Liveness probe endpoint for Kubernetes.
     * This endpoint is used to detect hung or deadlocked services that cannot recover without being restarted.
     * <p>
     * Returns:
     * - 200 OK if the service is alive
     * - 503 Service Unavailable if the service is not alive
     *
     * @return ResponseEntity with appropriate HTTP status code
     */
    @GetMapping("/live")
    public ResponseEntity<HealthComponent> getLivenessStatus() {
        HealthComponent health = healthEndpoint.healthForPath("liveness");
        LOGGER.debug("Liveness health check: {}", health.getStatus());
        return createResponseEntity(health);
    }

    /**
     * Readiness probe endpoint for Kubernetes.
     * This endpoint is used to determine if the service can handle traffic.
     * <p>
     * Returns:
     * - 200 OK if the service is ready to handle traffic
     * - 503 Service Unavailable if the service is not ready
     *
     * @return ResponseEntity with appropriate HTTP status code
     */
    @GetMapping("/ready")
    public ResponseEntity<HealthComponent> getReadinessStatus() {
        HealthComponent health = healthEndpoint.healthForPath("readiness");
        LOGGER.debug("Readiness health check: {}", health.getStatus());
        return createResponseEntity(health);
    }

    /**
     * Startup probe endpoint for Kubernetes.
     * This endpoint is used to determine if the service has started up successfully.
     * <p>
     * Returns:
     * - 200 OK if the service has started up successfully
     * - 503 Service Unavailable if the service is still starting up
     *
     * @return ResponseEntity with appropriate HTTP status code
     */
    @GetMapping("/startup")
    public ResponseEntity<HealthComponent> getStartupStatus() {
        HealthComponent health = healthEndpoint.healthForPath("startup");
        LOGGER.debug("Startup health check: {}", health.getStatus());
        return createResponseEntity(health);
    }

    /**
     * General health endpoint that provides overall health status.
     * <p>
     * Returns:
     * - 200 OK if the service is healthy
     * - 503 Service Unavailable if the service is not healthy
     *
     * @return ResponseEntity with appropriate HTTP status code
     */
    @GetMapping
    public ResponseEntity<HealthComponent> getHealth() {
        HealthComponent health = healthEndpoint.health();
        LOGGER.debug("Overall health check: {}", health.getStatus());
        return createResponseEntity(health);
    }

    /**
     * Creates a ResponseEntity with the appropriate HTTP status code based on the health status.
     *
     * @param health the health component containing the health status
     * @return ResponseEntity with appropriate HTTP status code
     */
    private ResponseEntity<HealthComponent> createResponseEntity(HealthComponent health) {
        if (Status.UP.equals(health.getStatus())) {
            return ResponseEntity.ok(health);
        } else if (Status.OUT_OF_SERVICE.equals(health.getStatus())) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(health);
        } else if (Status.DOWN.equals(health.getStatus())) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(health);
        } else {
            // For any other status (e.g., UNKNOWN), return 503 Service Unavailable
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(health);
        }
    }
}