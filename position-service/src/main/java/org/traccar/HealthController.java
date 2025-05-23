package org.traccar;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthComponent;
import org.springframework.boot.actuate.health.HealthEndpoint;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import org.traccar.metrics.HealthMetrics;

/**
 * Controller for exposing health check endpoints for the Position Processing Service.
 * Provides liveness and readiness probes for Kubernetes integration and dependency health monitoring.
 */
@RestController
@RequestMapping("/actuator/health")
public class HealthController {

    private final HealthEndpoint healthEndpoint;
    private final HealthMetrics healthMetrics;

    /**
     * Constructs a new HealthController with the specified dependencies.
     *
     * @param healthEndpoint Spring Boot's health endpoint for accessing health indicators
     * @param healthMetrics Metrics collector for recording health status
     */
    @Autowired
    public HealthController(HealthEndpoint healthEndpoint, HealthMetrics healthMetrics) {
        this.healthEndpoint = healthEndpoint;
        this.healthMetrics = healthMetrics;
    }

    /**
     * Liveness probe endpoint for Kubernetes.
     * Checks if the service is running and not deadlocked.
     *
     * @return 200 OK if the service is alive, 503 Service Unavailable otherwise
     */
    @GetMapping("/live")
    public ResponseEntity<HealthComponent> liveness() {
        HealthComponent health = healthEndpoint.healthForPath("liveness");
        boolean isHealthy = health.getStatus() == Status.UP;
        
        // Record the liveness check in metrics
        healthMetrics.recordHealthCheck("liveness", isHealthy);
        
        return createHealthResponse(health);
    }

    /**
     * Readiness probe endpoint for Kubernetes.
     * Checks if the service is ready to handle traffic, including dependency checks.
     *
     * @return 200 OK if the service is ready, 503 Service Unavailable otherwise
     */
    @GetMapping("/ready")
    public ResponseEntity<HealthComponent> readiness() {
        HealthComponent health = healthEndpoint.healthForPath("readiness");
        boolean isHealthy = health.getStatus() == Status.UP;
        
        // Record the readiness check in metrics
        healthMetrics.recordHealthCheck("readiness", isHealthy);
        
        return createHealthResponse(health);
    }

    /**
     * Startup probe endpoint for Kubernetes.
     * Checks if the service has completed its startup process.
     *
     * @return 200 OK if the service has started up, 503 Service Unavailable otherwise
     */
    @GetMapping("/startup")
    public ResponseEntity<HealthComponent> startup() {
        HealthComponent health = healthEndpoint.healthForPath("startup");
        boolean isHealthy = health.getStatus() == Status.UP;
        
        // Record the startup check in metrics
        healthMetrics.recordHealthCheck("startup", isHealthy);
        
        return createHealthResponse(health);
    }

    /**
     * Overall health endpoint that aggregates all health indicators.
     * Provides detailed health information for all components.
     *
     * @return 200 OK if all components are healthy, 503 Service Unavailable otherwise
     */
    @GetMapping
    public ResponseEntity<HealthComponent> health() {
        HealthComponent health = healthEndpoint.health();
        boolean isHealthy = health.getStatus() == Status.UP;
        
        // Record the overall health check in metrics
        healthMetrics.recordHealthCheck("overall", isHealthy);
        
        // Update the service health status gauge
        healthMetrics.setServiceHealthStatus(isHealthy ? 1.0 : 0.0);
        
        return createHealthResponse(health);
    }

    /**
     * Creates a ResponseEntity with the appropriate HTTP status based on the health status.
     *
     * @param health The health component to create a response for
     * @return ResponseEntity with the health component and appropriate HTTP status
     */
    private ResponseEntity<HealthComponent> createHealthResponse(HealthComponent health) {
        if (health.getStatus() == Status.UP) {
            return ResponseEntity.ok(health);
        } else if (health.getStatus() == Status.OUT_OF_SERVICE) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(health);
        } else if (health.getStatus() == Status.DOWN) {
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(health);
        } else {
            // For any unknown status, return 503 Service Unavailable
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(health);
        }
    }
}