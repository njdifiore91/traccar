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
package org.traccar.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.traccar.health.ResourceMonitor;
import org.traccar.health.MetricsCollector;
import org.traccar.health.DistributedTracingManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Controller that exposes health and readiness information for the Reporting Service.
 * This is essential for Kubernetes orchestration to properly manage the service lifecycle.
 */
@RestController
@RequestMapping("/actuator")
public class HealthCheckController {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckController.class);

    private final ResourceMonitor resourceMonitor;
    private final MetricsCollector metricsCollector;
    private final DistributedTracingManager tracingManager;
    private final ApplicationEventPublisher eventPublisher;

    @Autowired
    public HealthCheckController(
            ResourceMonitor resourceMonitor,
            MetricsCollector metricsCollector,
            DistributedTracingManager tracingManager,
            ApplicationEventPublisher eventPublisher) {
        this.resourceMonitor = resourceMonitor;
        this.metricsCollector = metricsCollector;
        this.tracingManager = tracingManager;
        this.eventPublisher = eventPublisher;
    }

    /**
     * Liveness probe endpoint for Kubernetes.
     * Verifies that the application is running and not deadlocked.
     * This probe should only check things that can be fixed by restarting the container.
     *
     * @return Health status response with HTTP 200 if alive, HTTP 503 if not
     */
    @GetMapping("/health/liveness")
    public ResponseEntity<Map<String, Object>> livenessCheck() {
        Map<String, Object> response = new HashMap<>();
        boolean isAlive = true;

        try {
            // Check if the application is running properly
            // For liveness, we only check things that can be fixed by a restart
            boolean memoryHealthy = resourceMonitor.isMemoryHealthy();
            boolean cpuHealthy = resourceMonitor.isCpuUtilizationHealthy();
            boolean threadPoolHealthy = resourceMonitor.isThreadPoolHealthy();
            
            isAlive = memoryHealthy && cpuHealthy && threadPoolHealthy;
            
            response.put("status", isAlive ? "UP" : "DOWN");
            response.put("memory", Map.of("status", memoryHealthy ? "UP" : "DOWN"));
            response.put("cpu", Map.of("status", cpuHealthy ? "UP" : "DOWN"));
            response.put("threadPool", Map.of("status", threadPoolHealthy ? "UP" : "DOWN"));
            
            // Record metrics for monitoring
            metricsCollector.recordHealthCheck("liveness", isAlive);
            
            // Create a trace span for this health check
            tracingManager.createHealthCheckSpan("liveness", isAlive);
            
            // If the service is not alive, publish an availability change event
            if (!isAlive) {
                LOGGER.warn("Liveness check failed: memory healthy: {}, CPU healthy: {}, thread pool healthy: {}", 
                        memoryHealthy, cpuHealthy, threadPoolHealthy);
                AvailabilityChangeEvent.publish(eventPublisher, this, LivenessState.BROKEN);
            } else if (LivenessState.BROKEN.equals(resourceMonitor.getCurrentLivenessState())) {
                // If the service was previously broken but is now alive, publish an event
                LOGGER.info("Service is now alive");
                AvailabilityChangeEvent.publish(eventPublisher, this, LivenessState.CORRECT);
            }
            
            return new ResponseEntity<>(response, isAlive ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            LOGGER.error("Error during liveness check", e);
            response.put("status", "DOWN");
            response.put("error", e.getMessage());
            
            // Record the failure in metrics
            metricsCollector.recordHealthCheck("liveness", false);
            
            // Create a trace span for this failed health check
            tracingManager.createHealthCheckSpan("liveness", false);
            
            // Publish an availability change event
            AvailabilityChangeEvent.publish(eventPublisher, this, LivenessState.BROKEN);
            
            return new ResponseEntity<>(response, HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    /**
     * Readiness probe endpoint for Kubernetes.
     * Verifies that the application is able to handle requests.
     * This probe checks if the service can accept traffic.
     *
     * @return Health status response with HTTP 200 if ready, HTTP 503 if not
     */
    @GetMapping("/health/readiness")
    public ResponseEntity<Map<String, Object>> readinessCheck() {
        Map<String, Object> response = new HashMap<>();
        boolean isReady = true;

        try {
            // Check if the application is ready to handle requests
            boolean databaseHealthy = resourceMonitor.isDatabaseConnectionHealthy();
            boolean diskSpaceHealthy = resourceMonitor.isDiskSpaceHealthy();
            boolean reportGenerationHealthy = resourceMonitor.isReportGenerationHealthy();
            
            // For readiness, we check if the service can process requests
            isReady = databaseHealthy && diskSpaceHealthy && reportGenerationHealthy;
            
            response.put("status", isReady ? "UP" : "DOWN");
            response.put("database", Map.of("status", databaseHealthy ? "UP" : "DOWN"));
            response.put("diskSpace", Map.of("status", diskSpaceHealthy ? "UP" : "DOWN"));
            response.put("reportGeneration", Map.of("status", reportGenerationHealthy ? "UP" : "DOWN"));
            
            // Record metrics for monitoring
            metricsCollector.recordHealthCheck("readiness", isReady);
            
            // Create a trace span for this health check
            tracingManager.createHealthCheckSpan("readiness", isReady);
            
            // If the service is not ready, publish an availability change event
            if (!isReady) {
                LOGGER.warn("Readiness check failed: database healthy: {}, disk space healthy: {}, report generation healthy: {}", 
                        databaseHealthy, diskSpaceHealthy, reportGenerationHealthy);
                AvailabilityChangeEvent.publish(eventPublisher, this, ReadinessState.REFUSING_TRAFFIC);
            } else if (ReadinessState.REFUSING_TRAFFIC.equals(resourceMonitor.getCurrentReadinessState())) {
                // If the service was previously refusing traffic but is now ready, publish an event
                LOGGER.info("Service is now ready to accept traffic");
                AvailabilityChangeEvent.publish(eventPublisher, this, ReadinessState.ACCEPTING_TRAFFIC);
            }
            
            return new ResponseEntity<>(response, isReady ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            LOGGER.error("Error during readiness check", e);
            response.put("status", "DOWN");
            response.put("error", e.getMessage());
            
            // Record the failure in metrics
            metricsCollector.recordHealthCheck("readiness", false);
            
            // Create a trace span for this failed health check
            tracingManager.createHealthCheckSpan("readiness", false);
            
            // Publish an availability change event
            AvailabilityChangeEvent.publish(eventPublisher, this, ReadinessState.REFUSING_TRAFFIC);
            
            return new ResponseEntity<>(response, HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    /**
     * Startup probe endpoint for Kubernetes.
     * Verifies that the application has completed its initialization.
     * This probe prevents liveness and readiness checks from starting until the application is fully initialized.
     *
     * @return Health status response with HTTP 200 if startup is complete, HTTP 503 if not
     */
    @GetMapping("/health/startup")
    public ResponseEntity<Map<String, Object>> startupCheck() {
        Map<String, Object> response = new HashMap<>();
        boolean isStarted = true;

        try {
            // Check if the application has completed initialization
            boolean databaseInitialized = resourceMonitor.isDatabaseInitialized();
            boolean dependenciesReady = resourceMonitor.areDependenciesReady();
            boolean configurationLoaded = resourceMonitor.isConfigurationLoaded();
            
            isStarted = databaseInitialized && dependenciesReady && configurationLoaded;
            
            response.put("status", isStarted ? "UP" : "DOWN");
            response.put("database", Map.of("status", databaseInitialized ? "UP" : "DOWN"));
            response.put("dependencies", Map.of("status", dependenciesReady ? "UP" : "DOWN"));
            response.put("configuration", Map.of("status", configurationLoaded ? "UP" : "DOWN"));
            
            // Record metrics for monitoring
            metricsCollector.recordHealthCheck("startup", isStarted);
            
            // Create a trace span for this health check
            tracingManager.createHealthCheckSpan("startup", isStarted);
            
            if (!isStarted) {
                LOGGER.warn("Startup check failed: database initialized: {}, dependencies ready: {}, configuration loaded: {}", 
                        databaseInitialized, dependenciesReady, configurationLoaded);
            } else {
                LOGGER.debug("Startup check passed: application is fully initialized");
            }
            
            return new ResponseEntity<>(response, isStarted ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            LOGGER.error("Error during startup check", e);
            response.put("status", "DOWN");
            response.put("error", e.getMessage());
            
            // Record the failure in metrics
            metricsCollector.recordHealthCheck("startup", false);
            
            // Create a trace span for this failed health check
            tracingManager.createHealthCheckSpan("startup", false);
            
            return new ResponseEntity<>(response, HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    /**
     * Comprehensive health check endpoint that combines all health checks.
     * This is the main health endpoint that provides a complete view of the application's health.
     *
     * @return Detailed health status response
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Object> response = new HashMap<>();
        boolean isHealthy = true;

        try {
            // Get individual health check results
            ResponseEntity<Map<String, Object>> livenessResult = livenessCheck();
            ResponseEntity<Map<String, Object>> readinessResult = readinessCheck();
            ResponseEntity<Map<String, Object>> startupResult = startupCheck();
            
            boolean livenessOk = livenessResult.getStatusCode().is2xxSuccessful();
            boolean readinessOk = readinessResult.getStatusCode().is2xxSuccessful();
            boolean startupOk = startupResult.getStatusCode().is2xxSuccessful();
            
            isHealthy = livenessOk && readinessOk && startupOk;
            
            // Combine all health check results
            response.put("status", isHealthy ? "UP" : "DOWN");
            response.put("groups", Map.of(
                "liveness", Map.of("status", livenessOk ? "UP" : "DOWN"),
                "readiness", Map.of("status", readinessOk ? "UP" : "DOWN"),
                "startup", Map.of("status", startupOk ? "UP" : "DOWN")
            ));
            
            // Add detailed component statuses
            Map<String, Object> components = new HashMap<>();
            
            // Add memory status
            components.put("memory", Map.of(
                "status", resourceMonitor.isMemoryHealthy() ? "UP" : "DOWN",
                "details", Map.of(
                    "total", resourceMonitor.getTotalMemory(),
                    "free", resourceMonitor.getFreeMemory(),
                    "used", resourceMonitor.getUsedMemory(),
                    "threshold", resourceMonitor.getMemoryThreshold()
                )
            ));
            
            // Add CPU status
            components.put("cpu", Map.of(
                "status", resourceMonitor.isCpuUtilizationHealthy() ? "UP" : "DOWN",
                "details", Map.of(
                    "utilization", resourceMonitor.getCpuUtilization(),
                    "threshold", resourceMonitor.getCpuThreshold()
                )
            ));
            
            // Add database status
            components.put("database", Map.of(
                "status", resourceMonitor.isDatabaseConnectionHealthy() ? "UP" : "DOWN",
                "details", Map.of(
                    "active", resourceMonitor.getActiveDatabaseConnections(),
                    "idle", resourceMonitor.getIdleDatabaseConnections(),
                    "max", resourceMonitor.getMaxDatabaseConnections()
                )
            ));
            
            // Add disk space status
            components.put("diskSpace", Map.of(
                "status", resourceMonitor.isDiskSpaceHealthy() ? "UP" : "DOWN",
                "details", Map.of(
                    "total", resourceMonitor.getTotalDiskSpace(),
                    "free", resourceMonitor.getFreeDiskSpace(),
                    "threshold", resourceMonitor.getDiskSpaceThreshold()
                )
            ));
            
            // Add report generation status
            components.put("reportGeneration", Map.of(
                "status", resourceMonitor.isReportGenerationHealthy() ? "UP" : "DOWN",
                "details", Map.of(
                    "averageExecutionTime", resourceMonitor.getAverageReportExecutionTime(),
                    "activeReportJobs", resourceMonitor.getActiveReportJobs(),
                    "queuedReportJobs", resourceMonitor.getQueuedReportJobs(),
                    "maxConcurrentJobs", resourceMonitor.getMaxConcurrentReportJobs()
                )
            ));
            
            response.put("components", components);
            
            // Record metrics for monitoring
            metricsCollector.recordHealthCheck("overall", isHealthy);
            
            // Create a trace span for this health check
            tracingManager.createHealthCheckSpan("overall", isHealthy);
            
            return new ResponseEntity<>(response, isHealthy ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
        } catch (Exception e) {
            LOGGER.error("Error during comprehensive health check", e);
            response.put("status", "DOWN");
            response.put("error", e.getMessage());
            
            // Record the failure in metrics
            metricsCollector.recordHealthCheck("overall", false);
            
            // Create a trace span for this failed health check
            tracingManager.createHealthCheckSpan("overall", false);
            
            return new ResponseEntity<>(response, HttpStatus.SERVICE_UNAVAILABLE);
        }
    }

    /**
     * Provides information about the service for discovery and monitoring systems.
     * This endpoint is used by monitoring tools to get metadata about the service.
     *
     * @return Service information response
     */
    @GetMapping("/info")
    public ResponseEntity<Map<String, Object>> serviceInfo() {
        Map<String, Object> response = new HashMap<>();
        
        response.put("service", "reporting-service");
        response.put("version", getClass().getPackage().getImplementationVersion());
        response.put("build", Map.of(
            "time", System.getProperty("build.time", "unknown"),
            "version", System.getProperty("build.version", "unknown")
        ));
        response.put("metrics", Map.of(
            "endpoint", "/actuator/metrics",
            "format", "prometheus"
        ));
        response.put("health", Map.of(
            "liveness", "/actuator/health/liveness",
            "readiness", "/actuator/health/readiness",
            "startup", "/actuator/health/startup",
            "overall", "/actuator/health"
        ));
        response.put("kubernetes", Map.of(
            "probes", Map.of(
                "liveness", "httpGet to /actuator/health/liveness",
                "readiness", "httpGet to /actuator/health/readiness",
                "startup", "httpGet to /actuator/health/startup"
            )
        ));
        
        return new ResponseEntity<>(response, HttpStatus.OK);
    }
}