/*
 * Copyright 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.push;

import com.google.firebase.messaging.FirebaseMessaging;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.serviceregistry.Registration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Health check controller for the push notification service.
 * Provides liveness, readiness, and startup probes for Kubernetes container orchestration.
 * Checks Firebase connectivity and reports overall service health.
 * Integrates with service discovery to update service availability based on health status.
 */
@RestController
@RequestMapping("/actuator/health")
public class HealthCheckController implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckController.class);

    private final FirebaseClient firebaseClient;
    private final Registration registration;
    private final DiscoveryClient discoveryClient;

    private boolean isStartupComplete = false;
    private boolean isFirebaseConnected = false;

    @Autowired
    public HealthCheckController(
            FirebaseClient firebaseClient,
            Registration registration,
            DiscoveryClient discoveryClient) {
        this.firebaseClient = firebaseClient;
        this.registration = registration;
        this.discoveryClient = discoveryClient;
        
        // Initialize health check
        checkFirebaseConnection();
        isStartupComplete = true;
    }

    /**
     * Liveness probe endpoint for Kubernetes.
     * Checks if the service is running and responsive.
     * 
     * @return Health status with UP if service is alive, DOWN otherwise
     */
    @GetMapping("/live")
    public Health liveness() {
        return Health.up().build();
    }

    /**
     * Readiness probe endpoint for Kubernetes.
     * Checks if the service is ready to accept traffic by verifying Firebase connectivity.
     * 
     * @return Health status with UP if service is ready, DOWN otherwise
     */
    @GetMapping("/ready")
    public Health readiness() {
        if (isFirebaseConnected) {
            return Health.up().build();
        } else {
            // Try to reconnect to Firebase
            checkFirebaseConnection();
            
            if (isFirebaseConnected) {
                return Health.up().build();
            } else {
                return Health.down()
                        .withDetail("reason", "Firebase connection failed")
                        .build();
            }
        }
    }

    /**
     * Startup probe endpoint for Kubernetes.
     * Checks if the service has completed initialization.
     * 
     * @return Health status with UP if startup is complete, DOWN otherwise
     */
    @GetMapping("/startup")
    public Health startup() {
        if (isStartupComplete) {
            return Health.up().build();
        } else {
            return Health.down()
                    .withDetail("reason", "Startup not complete")
                    .build();
        }
    }

    /**
     * Overall health check implementation for Spring Boot Actuator.
     * Aggregates all health indicators into a single health status.
     * 
     * @return Aggregated health status
     */
    @Override
    public Health health() {
        Health.Builder builder = Health.up();
        
        // Check Firebase connectivity
        if (!isFirebaseConnected) {
            checkFirebaseConnection();
        }
        
        if (!isFirebaseConnected) {
            builder = Health.down()
                    .withDetail("firebase", "disconnected");
        } else {
            builder.withDetail("firebase", "connected");
        }
        
        // Add startup status
        builder.withDetail("startup", isStartupComplete ? "complete" : "incomplete");
        
        // Add service discovery status
        builder.withDetail("serviceDiscovery", "registered");
        
        return builder.build();
    }

    /**
     * Checks Firebase connectivity by attempting to get the Firebase Messaging instance.
     * Updates the isFirebaseConnected flag based on the result.
     */
    private void checkFirebaseConnection() {
        try {
            FirebaseMessaging messaging = firebaseClient.getInstance();
            if (messaging != null) {
                isFirebaseConnected = true;
                LOGGER.debug("Firebase connection check successful");
                
                // Update service discovery status if needed
                updateServiceAvailability(true);
            } else {
                isFirebaseConnected = false;
                LOGGER.warn("Firebase connection check failed: messaging instance is null");
                
                // Update service discovery status if needed
                updateServiceAvailability(false);
            }
        } catch (Exception e) {
            isFirebaseConnected = false;
            LOGGER.error("Firebase connection check failed", e);
            
            // Update service discovery status if needed
            updateServiceAvailability(false);
        }
    }

    /**
     * Updates service availability in the service discovery system.
     * 
     * @param isAvailable true if the service is available, false otherwise
     */
    private void updateServiceAvailability(boolean isAvailable) {
        try {
            if (registration != null) {
                // Update service metadata to reflect current health status
                registration.getMetadata().put("health.status", isAvailable ? "UP" : "DOWN");
                
                LOGGER.debug("Updated service discovery status to: {}", isAvailable ? "UP" : "DOWN");
            }
        } catch (Exception e) {
            LOGGER.error("Failed to update service discovery status", e);
        }
    }
}