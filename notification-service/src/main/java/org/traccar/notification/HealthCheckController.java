package org.traccar.notification;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.actuate.health.Status;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Controller that exposes health check endpoints for the Notification Service.
 * Provides liveness and readiness probes for Kubernetes integration and detailed
 * health information for monitoring systems.
 * 
 * This controller implements the following endpoints:
 * - /actuator/health/live: Liveness probe for basic operational status
 * - /actuator/health/ready: Readiness probe for dependency status checking
 * - /actuator/health/startup: Startup probe for initialization completion
 * - /actuator/health/details: Detailed health metrics for monitoring systems
 * 
 * It also provides methods for tracking notification metrics and exposing them via Prometheus.
 */
@RestController
@RequestMapping("/actuator/health")
public class HealthCheckController {

    private final Map<String, HealthIndicator> healthIndicators;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MeterRegistry meterRegistry;
    
    // Counters for metrics
    private final AtomicInteger totalRequests = new AtomicInteger(0);
    private final AtomicInteger successfulDeliveries = new AtomicInteger(0);
    private final AtomicInteger failedDeliveries = new AtomicInteger(0);
    
    // Prometheus metrics
    private Counter totalRequestsCounter;
    private Counter successfulDeliveriesCounter;
    private Counter failedDeliveriesCounter;

    @Autowired
    public HealthCheckController(Map<String, HealthIndicator> healthIndicators, 
                                CircuitBreakerRegistry circuitBreakerRegistry,
                                MeterRegistry meterRegistry) {
        this.healthIndicators = healthIndicators;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.meterRegistry = meterRegistry;
        
        // Initialize Prometheus metrics
        this.totalRequestsCounter = Counter.builder("notification_requests_total")
            .description("Total number of notification requests")
            .register(meterRegistry);
            
        this.successfulDeliveriesCounter = Counter.builder("notification_deliveries_successful_total")
            .description("Total number of successful notification deliveries")
            .register(meterRegistry);
            
        this.failedDeliveriesCounter = Counter.builder("notification_deliveries_failed_total")
            .description("Total number of failed notification deliveries")
            .register(meterRegistry);
            
        // Register gauge for success rate
        Gauge.builder("notification_delivery_success_rate", this, controller -> {
            int total = controller.totalRequests.get();
            return total > 0 ? (double) controller.successfulDeliveries.get() / total : 1.0;
        }).description("Success rate of notification deliveries").register(meterRegistry);
    }

    /**
     * Liveness probe endpoint for Kubernetes.
     * Checks if the service is running and responsive.
     * 
     * @return ResponseEntity with health status
     */
    @GetMapping("/live")
    public ResponseEntity<Map<String, Object>> livenessProbe() {
        Map<String, Object> response = new HashMap<>();
        response.put("status", "UP");
        response.put("timestamp", System.currentTimeMillis());
        response.put("service", "notification-service");
        response.put("version", getClass().getPackage().getImplementationVersion());
        
        return ResponseEntity.ok(response);
    }

    /**
     * Readiness probe endpoint for Kubernetes.
     * Checks if the service is ready to accept traffic by verifying dependencies.
     * 
     * @return ResponseEntity with health status and dependency details
     */
    @GetMapping("/ready")
    public ResponseEntity<Map<String, Object>> readinessProbe() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> dependencies = new HashMap<>();
        boolean allHealthy = true;
        
        // Check all registered health indicators
        for (Map.Entry<String, HealthIndicator> entry : healthIndicators.entrySet()) {
            Health health = entry.getValue().health();
            dependencies.put(entry.getKey(), health.getStatus().getCode());
            
            if (health.getStatus() != Status.UP) {
                allHealthy = false;
            }
        }
        
        response.put("status", allHealthy ? "UP" : "DOWN");
        response.put("dependencies", dependencies);
        response.put("timestamp", System.currentTimeMillis());
        
        return allHealthy ? 
            ResponseEntity.ok(response) : 
            ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
    }

    /**
     * Startup probe endpoint for Kubernetes.
     * Checks if the service has completed its initialization.
     * 
     * @return ResponseEntity with startup status
     */
    @GetMapping("/startup")
    public ResponseEntity<Map<String, Object>> startupProbe() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        
        // Check critical components needed for startup
        boolean messageBrokerReady = healthIndicators.entrySet().stream()
                .filter(entry -> entry.getKey().contains("messageBroker"))
                .map(entry -> entry.getValue().health())
                .allMatch(health -> health.getStatus() == Status.UP);
        
        components.put("messageBroker", messageBrokerReady ? "UP" : "DOWN");
        
        // Service is considered started if message broker is available
        String status = messageBrokerReady ? "UP" : "DOWN";
        
        response.put("status", status);
        response.put("components", components);
        response.put("timestamp", System.currentTimeMillis());
        response.put("service", "notification-service");
        response.put("version", getClass().getPackage().getImplementationVersion());
        
        return status.equals("UP") ? 
            ResponseEntity.ok(response) : 
            ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(response);
    }

    /**
     * Detailed health metrics endpoint for monitoring systems.
     * Provides comprehensive health information including circuit breaker status.
     * 
     * @return ResponseEntity with detailed health metrics
     */
    @GetMapping("/details")
    public ResponseEntity<Map<String, Object>> healthDetails() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> dependencies = new HashMap<>();
        Map<String, Object> metrics = new HashMap<>();
        Map<String, Object> circuitBreakers = new HashMap<>();
        Map<String, Object> channelMetrics = new HashMap<>();
        
        // Check all registered health indicators
        for (Map.Entry<String, HealthIndicator> entry : healthIndicators.entrySet()) {
            Health health = entry.getValue().health();
            Map<String, Object> details = new HashMap<>();
            details.put("status", health.getStatus().getCode());
            details.put("details", health.getDetails());
            dependencies.put(entry.getKey(), details);
        }
        
        // Add notification metrics
        metrics.put("totalRequests", totalRequests.get());
        metrics.put("successfulDeliveries", successfulDeliveries.get());
        metrics.put("failedDeliveries", failedDeliveries.get());
        
        // Calculate success rate if there are any requests
        if (totalRequests.get() > 0) {
            double successRate = (double) successfulDeliveries.get() / totalRequests.get() * 100.0;
            metrics.put("successRate", String.format("%.2f%%", successRate));
        }
        
        // Add circuit breaker status
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(cb -> {
            Map<String, Object> cbDetails = new HashMap<>();
            cbDetails.put("state", cb.getState().name());
            cbDetails.put("failureRate", cb.getMetrics().getFailureRate());
            cbDetails.put("slowCallRate", cb.getMetrics().getSlowCallRate());
            cbDetails.put("numberOfFailedCalls", cb.getMetrics().getNumberOfFailedCalls());
            cbDetails.put("numberOfSlowCalls", cb.getMetrics().getNumberOfSlowCalls());
            cbDetails.put("numberOfSuccessfulCalls", cb.getMetrics().getNumberOfSuccessfulCalls());
            
            // Add additional circuit breaker details
            Map<String, Object> cbConfig = new HashMap<>();
            cbConfig.put("failureRateThreshold", cb.getCircuitBreakerConfig().getFailureRateThreshold());
            cbConfig.put("slowCallRateThreshold", cb.getCircuitBreakerConfig().getSlowCallRateThreshold());
            cbConfig.put("slowCallDurationThreshold", cb.getCircuitBreakerConfig().getSlowCallDurationThreshold());
            cbConfig.put("permittedNumberOfCallsInHalfOpenState", 
                    cb.getCircuitBreakerConfig().getPermittedNumberOfCallsInHalfOpenState());
            cbConfig.put("waitDurationInOpenState", 
                    cb.getCircuitBreakerConfig().getWaitDurationInOpenState());
            
            cbDetails.put("config", cbConfig);
            circuitBreakers.put(cb.getName(), cbDetails);
        });
        
        // Collect channel-specific metrics from Prometheus registry
        meterRegistry.find("notification_deliveries_successful_total").meters().forEach(meter -> {
            String channel = meter.getId().getTag("channel");
            if (channel != null) {
                Map<String, Object> channelData = (Map<String, Object>) channelMetrics.getOrDefault(channel, new HashMap<>());
                channelData.put("successful", meter.measure().iterator().next().getValue());
                channelMetrics.put(channel, channelData);
            }
        });
        
        meterRegistry.find("notification_deliveries_failed_total").meters().forEach(meter -> {
            String channel = meter.getId().getTag("channel");
            if (channel != null) {
                Map<String, Object> channelData = (Map<String, Object>) channelMetrics.getOrDefault(channel, new HashMap<>());
                channelData.put("failed", meter.measure().iterator().next().getValue());
                channelMetrics.put(channel, channelData);
            }
        });
        
        // Collect error type metrics
        Map<String, Object> errorMetrics = new HashMap<>();
        meterRegistry.find("notification_delivery_errors_total").meters().forEach(meter -> {
            String channel = meter.getId().getTag("channel");
            String errorType = meter.getId().getTag("error_type");
            if (channel != null && errorType != null) {
                String key = channel + "-" + errorType;
                errorMetrics.put(key, meter.measure().iterator().next().getValue());
            }
        });
        
        // Add all metrics to response
        metrics.put("byChannel", channelMetrics);
        metrics.put("errors", errorMetrics);
        
        // Determine overall status
        boolean isDown = dependencies.values().stream()
                .anyMatch(d -> ((Map<String, Object>) d).get("status").equals("DOWN"));
                
        // Check if any circuit breakers are open
        boolean anyCircuitBreakerOpen = circuitBreakerRegistry.getAllCircuitBreakers().stream()
                .anyMatch(cb -> cb.getState() == CircuitBreaker.State.OPEN);
        
        String status = isDown || anyCircuitBreakerOpen ? "DOWN" : "UP";
        
        response.put("status", status);
        response.put("dependencies", dependencies);
        response.put("metrics", metrics);
        response.put("circuitBreakers", circuitBreakers);
        response.put("timestamp", System.currentTimeMillis());
        
        return ResponseEntity.ok(response);
    }

    /**
     * Increments the total request counter.
     * This method should be called when a notification request is received.
     */
    public void incrementTotalRequests() {
        totalRequests.incrementAndGet();
        totalRequestsCounter.increment();
    }

    /**
     * Increments the successful delivery counter.
     * This method should be called when a notification is successfully delivered.
     */
    public void incrementSuccessfulDeliveries() {
        successfulDeliveries.incrementAndGet();
        successfulDeliveriesCounter.increment();
    }

    /**
     * Increments the failed delivery counter.
     * This method should be called when a notification delivery fails.
     */
    public void incrementFailedDeliveries() {
        failedDeliveries.incrementAndGet();
        failedDeliveriesCounter.increment();
    }
    
    /**
     * Increments the notification counters with channel information.
     * This method should be called when a notification is processed.
     * 
     * @param channel The notification channel (email, sms, push, etc.)
     * @param success Whether the delivery was successful
     */
    public void recordDeliveryAttempt(String channel, boolean success) {
        totalRequests.incrementAndGet();
        totalRequestsCounter.increment();
        
        if (success) {
            successfulDeliveries.incrementAndGet();
            Counter.builder("notification_deliveries_successful_total")
                .tag("channel", channel)
                .description("Total number of successful notification deliveries by channel")
                .register(meterRegistry)
                .increment();
        } else {
            failedDeliveries.incrementAndGet();
            Counter.builder("notification_deliveries_failed_total")
                .tag("channel", channel)
                .description("Total number of failed notification deliveries by channel")
                .register(meterRegistry)
                .increment();
        }
    }
    
    /**
     * Records a notification delivery attempt with detailed information.
     * 
     * @param channel The notification channel (email, sms, push, etc.)
     * @param success Whether the delivery was successful
     * @param errorType The type of error if delivery failed (null if successful)
     */
    public void recordDeliveryAttempt(String channel, boolean success, String errorType) {
        recordDeliveryAttempt(channel, success);
        
        if (!success && errorType != null) {
            Counter.builder("notification_delivery_errors_total")
                .tag("channel", channel)
                .tag("error_type", errorType)
                .description("Total number of notification delivery errors by type")
                .register(meterRegistry)
                .increment();
        }
    }
}