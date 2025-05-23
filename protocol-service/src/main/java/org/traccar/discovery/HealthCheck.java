package org.traccar.discovery;

import java.util.Map;

/**
 * Interface defining health check reporting for services.
 * Provides methods to check if a service is healthy, ready to serve requests,
 * and to report detailed health status. This is crucial for service discovery
 * to determine if a service instance should receive traffic.
 */
public interface HealthCheck {

    /**
     * Enum representing the possible health states of a service or component.
     */
    enum Status {
        /**
         * Service is functioning normally and can handle requests.
         */
        UP,
        
        /**
         * Service is running but in a degraded state with limited functionality.
         */
        DEGRADED,
        
        /**
         * Service is running but not ready to handle requests (e.g., during initialization).
         */
        OUT_OF_SERVICE,
        
        /**
         * Service is not functioning and cannot handle requests.
         */
        DOWN,
        
        /**
         * Service health state cannot be determined.
         */
        UNKNOWN
    }

    /**
     * Checks if the service is healthy and can handle requests.
     * This is used for basic liveness checks to determine if the service is running.
     *
     * @return true if the service is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Checks if the service is ready to serve requests.
     * This is used for readiness checks to determine if the service should receive traffic.
     * A service might be healthy (running) but not ready (e.g., during initialization).
     *
     * @return true if the service is ready to serve requests, false otherwise
     */
    boolean isReady();

    /**
     * Gets the detailed health status of the service.
     * This includes the overall status and component-level health information.
     *
     * @return a map containing the overall status and component-specific health details
     */
    Map<String, Object> getStatus();

    /**
     * Gets the health status of a specific component or dependency.
     * This allows for granular health checking of individual service components.
     *
     * @param component the name of the component to check
     * @return the status of the specified component
     */
    Status getComponentStatus(String component);

    /**
     * Registers a custom health indicator for a specific component.
     * This allows services to define custom health checks for their dependencies.
     *
     * @param component the name of the component
     * @param healthIndicator a function that returns the health status of the component
     */
    void registerHealthIndicator(String component, HealthIndicator healthIndicator);

    /**
     * Functional interface for custom health indicators.
     * Implementations should perform the necessary checks and return the appropriate status.
     */
    @FunctionalInterface
    interface HealthIndicator {
        /**
         * Checks the health of a component and returns its status.
         *
         * @return the health status of the component
         */
        Status check();
    }
}