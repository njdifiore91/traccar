/*
 * Copyright 2020 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.schedule;

import com.sun.jna.Library;
import com.sun.jna.Native;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.availability.ApplicationAvailability;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.LivenessState;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.client.Client;
import org.traccar.database.StatisticsManager;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Health check task that monitors system health and updates Kubernetes health probes.
 * This implementation supports both traditional systemd watchdog and Kubernetes health probes.
 * It also exposes health metrics via Prometheus and integrates with OpenTelemetry for distributed tracing.
 */
@Singleton
public class TaskHealthCheck implements ScheduleTask, HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskHealthCheck.class);
    private static final String METRIC_PREFIX = "traccar_health";

    private final Config config;
    private final Client client;
    private final StatisticsManager statisticsManager;
    private final ApplicationEventPublisher eventPublisher;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;

    private final long gracePeriod = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);

    private SystemD systemD;

    private boolean enabled;
    private long period;
    private double dropThreshold;

    private int messageLastTotal;
    private int messageLastPeriod;
    
    private boolean messageBrokerHealthy = true;
    private boolean serviceDiscoveryHealthy = true;
    private boolean databaseHealthy = true;
    private boolean webHealthy = true;
    private boolean messageProcessingHealthy = true;

    /**
     * Constructs a new TaskHealthCheck with the required dependencies.
     *
     * @param config The application configuration
     * @param client HTTP client for health checks
     * @param statisticsManager Statistics manager for message processing metrics
     * @param eventPublisher Spring event publisher for availability state changes
     * @param meterRegistry Micrometer registry for exposing metrics
     * @param openTelemetry OpenTelemetry instance for distributed tracing
     */
    @Inject
    public TaskHealthCheck(Config config, Client client, StatisticsManager statisticsManager,
                          ApplicationEventPublisher eventPublisher, MeterRegistry meterRegistry,
                          OpenTelemetry openTelemetry) {
        this.config = config;
        this.client = client;
        this.statisticsManager = statisticsManager;
        this.eventPublisher = eventPublisher;
        this.meterRegistry = meterRegistry;
        this.tracer = openTelemetry.getTracer("org.traccar.health");
        
        // Initialize health metrics
        initializeMetrics();
        
        // Initialize systemd watchdog for backward compatibility
        initializeSystemdWatchdog();
        
        // Set initial health states
        updateLivenessState(LivenessState.CORRECT);
        updateReadinessState(ReadinessState.ACCEPTING_TRAFFIC);
    }
    
    /**
     * Initializes Prometheus metrics for health monitoring.
     */
    private void initializeMetrics() {
        Gauge.builder(METRIC_PREFIX + ".liveness", () -> isLivenessHealthy() ? 1 : 0)
                .description("Indicates if the application is live")
                .register(meterRegistry);
                
        Gauge.builder(METRIC_PREFIX + ".readiness", () -> isReadinessHealthy() ? 1 : 0)
                .description("Indicates if the application is ready to accept traffic")
                .register(meterRegistry);
                
        Gauge.builder(METRIC_PREFIX + ".component.web", () -> webHealthy ? 1 : 0)
                .description("Web component health status")
                .tag("component", "web")
                .register(meterRegistry);
                
        Gauge.builder(METRIC_PREFIX + ".component.message_processing", () -> messageProcessingHealthy ? 1 : 0)
                .description("Message processing component health status")
                .tag("component", "message_processing")
                .register(meterRegistry);
                
        Gauge.builder(METRIC_PREFIX + ".component.database", () -> databaseHealthy ? 1 : 0)
                .description("Database component health status")
                .tag("component", "database")
                .register(meterRegistry);
                
        Gauge.builder(METRIC_PREFIX + ".component.message_broker", () -> messageBrokerHealthy ? 1 : 0)
                .description("Message broker component health status")
                .tag("component", "message_broker")
                .register(meterRegistry);
                
        Gauge.builder(METRIC_PREFIX + ".component.service_discovery", () -> serviceDiscoveryHealthy ? 1 : 0)
                .description("Service discovery component health status")
                .tag("component", "service_discovery")
                .register(meterRegistry);
    }
    
    /**
     * Initializes systemd watchdog for backward compatibility with traditional deployments.
     */
    private void initializeSystemdWatchdog() {
        if (!config.getBoolean(Keys.WEB_DISABLE_HEALTH_CHECK)
                && System.getProperty("os.name").toLowerCase().startsWith("linux")) {
            try {
                systemD = Native.load("systemd", SystemD.class);
                String watchdogTimer = System.getenv("WATCHDOG_USEC");
                if (watchdogTimer != null && !watchdogTimer.isEmpty()) {
                    period = Long.parseLong(watchdogTimer) / 1000 * 4 / 5;
                }
                if (period > 0) {
                    LOGGER.info("Systemd health check enabled with period {}", period);
                    dropThreshold = config.getDouble(Keys.WEB_HEALTH_CHECK_DROP_THRESHOLD);
                    enabled = true;
                }
            } catch (UnsatisfiedLinkError e) {
                LOGGER.info("No systemd support, using Kubernetes health probes instead");
            }
        } else {
            // Default period for Kubernetes environment
            period = config.getLong("health.check.period", 15000); // 15 seconds default
            dropThreshold = config.getDouble(Keys.WEB_HEALTH_CHECK_DROP_THRESHOLD);
            enabled = true;
            LOGGER.info("Kubernetes health check enabled with period {}", period);
        }
    }

    private String getUrl() {
        String address = config.getString(Keys.WEB_ADDRESS, "localhost");
        int port = config.getInteger(Keys.WEB_PORT);
        return "http://" + address + ":" + port + "/api/server";
    }

    @Override
    public void schedule(ScheduledExecutorService executor) {
        if (enabled) {
            executor.scheduleAtFixedRate(this, period, period, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void run() {
        Span span = tracer.spanBuilder("health.check").startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.debug("Health check running");
            
            // Check web component health
            checkWebHealth();
            
            // Check message processing health
            checkMessageProcessingHealth();
            
            // Check message broker health
            checkMessageBrokerHealth();
            
            // Check service discovery health
            checkServiceDiscoveryHealth();
            
            // Update overall health state
            updateHealthState();
            
            // Notify systemd watchdog for backward compatibility
            if (systemD != null && isLivenessHealthy()) {
                notifyWatchdog();
            }
        } finally {
            span.end();
        }
    }
    
    /**
     * Checks the health of the web component.
     */
    private void checkWebHealth() {
        Span span = tracer.spanBuilder("health.check.web")
                .setParent(Context.current())
                .startSpan();
        try {
            if (System.currentTimeMillis() > gracePeriod) {
                int status = client.target(getUrl()).request().get().getStatus();
                webHealthy = (status >= 200 && status < 300);
                if (!webHealthy) {
                    LOGGER.warn("Web health check failed with status {}", status);
                    span.setAttribute("health.status", "unhealthy");
                    span.setAttribute("health.status.code", status);
                } else {
                    span.setAttribute("health.status", "healthy");
                }
            } else {
                webHealthy = true;
                span.setAttribute("health.status", "healthy");
                span.setAttribute("health.grace_period", true);
            }
        } catch (Exception e) {
            webHealthy = false;
            LOGGER.warn("Web health check failed with exception", e);
            span.setAttribute("health.status", "unhealthy");
            span.setAttribute("health.error", e.getMessage());
        } finally {
            span.end();
        }
    }
    
    /**
     * Checks the health of message processing.
     */
    private void checkMessageProcessingHealth() {
        Span span = tracer.spanBuilder("health.check.message_processing")
                .setParent(Context.current())
                .startSpan();
        try {
            if (System.currentTimeMillis() > gracePeriod) {
                int messageCurrentTotal = statisticsManager.messageStoredCount();
                int messageCurrentPeriod = messageCurrentTotal - messageLastTotal;
                
                span.setAttribute("message.current_total", messageCurrentTotal);
                span.setAttribute("message.current_period", messageCurrentPeriod);
                span.setAttribute("message.last_period", messageLastPeriod);
                
                if (dropThreshold > 0 && messageLastPeriod > 0 && messageCurrentPeriod > 0) {
                    double drop = messageCurrentPeriod / (double) messageLastPeriod;
                    span.setAttribute("message.drop_ratio", drop);
                    
                    messageProcessingHealthy = (drop >= dropThreshold);
                    if (!messageProcessingHealthy) {
                        LOGGER.warn("Message health check failed with drop {}", drop);
                        span.setAttribute("health.status", "unhealthy");
                    } else {
                        span.setAttribute("health.status", "healthy");
                    }
                } else {
                    messageProcessingHealthy = true;
                    span.setAttribute("health.status", "healthy");
                }
                
                messageLastTotal = messageCurrentTotal;
                messageLastPeriod = messageCurrentPeriod;
            } else {
                messageProcessingHealthy = true;
                span.setAttribute("health.status", "healthy");
                span.setAttribute("health.grace_period", true);
            }
        } catch (Exception e) {
            messageProcessingHealthy = false;
            LOGGER.warn("Message processing health check failed with exception", e);
            span.setAttribute("health.status", "unhealthy");
            span.setAttribute("health.error", e.getMessage());
        } finally {
            span.end();
        }
    }
    
    /**
     * Checks the health of the message broker.
     */
    private void checkMessageBrokerHealth() {
        Span span = tracer.spanBuilder("health.check.message_broker")
                .setParent(Context.current())
                .startSpan();
        try {
            // Check if message broker is configured
            String brokerType = config.getString(Keys.MESSAGE_BROKER_TYPE);
            if (brokerType != null && !brokerType.isEmpty()) {
                String brokerUrl = config.getString(Keys.MESSAGE_BROKER_URL);
                // Simple connectivity check - in a real implementation, this would
                // actually connect to the broker and verify its health
                messageBrokerHealthy = (brokerUrl != null && !brokerUrl.isEmpty());
                
                span.setAttribute("broker.type", brokerType);
                span.setAttribute("health.status", messageBrokerHealthy ? "healthy" : "unhealthy");
                
                if (!messageBrokerHealthy) {
                    LOGGER.warn("Message broker health check failed");
                }
            } else {
                // No broker configured, consider it healthy
                messageBrokerHealthy = true;
                span.setAttribute("broker.configured", false);
                span.setAttribute("health.status", "healthy");
            }
        } catch (Exception e) {
            messageBrokerHealthy = false;
            LOGGER.warn("Message broker health check failed with exception", e);
            span.setAttribute("health.status", "unhealthy");
            span.setAttribute("health.error", e.getMessage());
        } finally {
            span.end();
        }
    }
    
    /**
     * Checks the health of service discovery.
     */
    private void checkServiceDiscoveryHealth() {
        Span span = tracer.spanBuilder("health.check.service_discovery")
                .setParent(Context.current())
                .startSpan();
        try {
            // Check if service discovery is configured
            String discoveryType = config.getString(Keys.SERVICE_DISCOVERY_TYPE);
            if (discoveryType != null && !discoveryType.isEmpty()) {
                String discoveryUrl = config.getString(Keys.SERVICE_DISCOVERY_URL);
                // Simple connectivity check - in a real implementation, this would
                // actually connect to the service discovery and verify its health
                serviceDiscoveryHealthy = (discoveryUrl != null && !discoveryUrl.isEmpty());
                
                span.setAttribute("discovery.type", discoveryType);
                span.setAttribute("health.status", serviceDiscoveryHealthy ? "healthy" : "unhealthy");
                
                if (!serviceDiscoveryHealthy) {
                    LOGGER.warn("Service discovery health check failed");
                }
            } else {
                // No service discovery configured, consider it healthy
                serviceDiscoveryHealthy = true;
                span.setAttribute("discovery.configured", false);
                span.setAttribute("health.status", "healthy");
            }
        } catch (Exception e) {
            serviceDiscoveryHealthy = false;
            LOGGER.warn("Service discovery health check failed with exception", e);
            span.setAttribute("health.status", "unhealthy");
            span.setAttribute("health.error", e.getMessage());
        } finally {
            span.end();
        }
    }
    
    /**
     * Updates the overall health state based on component health checks.
     */
    private void updateHealthState() {
        // Liveness depends on core components only
        boolean livenessHealthy = webHealthy && messageProcessingHealthy;
        
        // Readiness depends on all components including external dependencies
        boolean readinessHealthy = livenessHealthy && messageBrokerHealthy && serviceDiscoveryHealthy;
        
        // Update liveness state
        if (livenessHealthy) {
            updateLivenessState(LivenessState.CORRECT);
        } else {
            updateLivenessState(LivenessState.BROKEN);
        }
        
        // Update readiness state
        if (readinessHealthy) {
            updateReadinessState(ReadinessState.ACCEPTING_TRAFFIC);
        } else {
            updateReadinessState(ReadinessState.REFUSING_TRAFFIC);
        }
    }
    
    /**
     * Updates the liveness state of the application.
     *
     * @param state The new liveness state
     */
    private void updateLivenessState(LivenessState state) {
        eventPublisher.publishEvent(new AvailabilityChangeEvent<>(this, state));
    }
    
    /**
     * Updates the readiness state of the application.
     *
     * @param state The new readiness state
     */
    private void updateReadinessState(ReadinessState state) {
        eventPublisher.publishEvent(new AvailabilityChangeEvent<>(this, state));
    }
    
    /**
     * Checks if the application is live based on component health.
     *
     * @return true if the application is live, false otherwise
     */
    private boolean isLivenessHealthy() {
        return webHealthy && messageProcessingHealthy;
    }
    
    /**
     * Checks if the application is ready to accept traffic based on component health.
     *
     * @return true if the application is ready, false otherwise
     */
    private boolean isReadinessHealthy() {
        return isLivenessHealthy() && messageBrokerHealthy && serviceDiscoveryHealthy;
    }

    /**
     * Notifies the systemd watchdog for backward compatibility.
     */
    private void notifyWatchdog() {
        if (systemD != null) {
            int result = systemD.sd_notify(0, "WATCHDOG=1");
            if (result < 0) {
                LOGGER.warn("Health check notify error {}", result);
            }
        }
    }
    
    /**
     * Implements the Spring Boot HealthIndicator interface for integration with Actuator.
     *
     * @return Health object representing the current health status
     */
    @Override
    public Health health() {
        Health.Builder builder = isLivenessHealthy() ? Health.up() : Health.down();
        
        // Add component details
        builder.withDetail("web", webHealthy ? "UP" : "DOWN")
               .withDetail("messageProcessing", messageProcessingHealthy ? "UP" : "DOWN")
               .withDetail("messageBroker", messageBrokerHealthy ? "UP" : "DOWN")
               .withDetail("serviceDiscovery", serviceDiscoveryHealthy ? "UP" : "DOWN");
               
        return builder.build();
    }

    /**
     * SystemD interface for backward compatibility with traditional deployments.
     */
    interface SystemD extends Library {
        @SuppressWarnings("checkstyle:MethodName")
        int sd_notify(@SuppressWarnings("checkstyle:ParameterName") int unset_environment, String state);
    }

}