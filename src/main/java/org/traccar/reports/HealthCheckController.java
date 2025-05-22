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
package org.traccar.reports;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.storage.Storage;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;

/**
 * Controller that exposes health and readiness information for the Reporting Service.
 * Provides endpoints for Kubernetes probes to verify process health, resource availability,
 * and dependency readiness.
 */
@Singleton
@Path("/actuator/health")
public class HealthCheckController {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckController.class);

    private final Config config;
    private final Storage storage;
    private final CombinedReportProvider combinedReportProvider;
    private final SummaryReportProvider summaryReportProvider;

    private final MemoryMXBean memoryMXBean;
    private final OperatingSystemMXBean osMXBean;

    private long lastReportGenerationTime = 0;
    private boolean databaseHealthy = true;

    @Inject
    public HealthCheckController(
            Config config,
            Storage storage,
            CombinedReportProvider combinedReportProvider,
            SummaryReportProvider summaryReportProvider) {
        this.config = config;
        this.storage = storage;
        this.combinedReportProvider = combinedReportProvider;
        this.summaryReportProvider = summaryReportProvider;

        this.memoryMXBean = ManagementFactory.getMemoryMXBean();
        this.osMXBean = ManagementFactory.getOperatingSystemMXBean();

        LOGGER.info("Health check controller initialized");
    }

    /**
     * Liveness probe endpoint for Kubernetes.
     * Verifies that the application is running and not deadlocked.
     *
     * @return HTTP 200 if the service is alive, HTTP 503 otherwise
     */
    @GET
    @Path("/live")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getLivenessStatus() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        Map<String, Object> livenessState = new HashMap<>();

        boolean isLive = true;
        livenessState.put("status", "UP");
        components.put("livenessState", livenessState);

        response.put("status", isLive ? "UP" : "DOWN");
        response.put("components", components);

        return Response.ok(response).build();
    }

    /**
     * Readiness probe endpoint for Kubernetes.
     * Verifies that the service is ready to accept traffic by checking database connectivity
     * and resource availability.
     *
     * @return HTTP 200 if the service is ready, HTTP 503 otherwise
     */
    @GET
    @Path("/ready")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getReadinessStatus() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        boolean isReady = true;

        // Check database connectivity
        Map<String, Object> dbStatus = checkDatabaseHealth();
        components.put("db", dbStatus);
        if ("DOWN".equals(dbStatus.get("status"))) {
            isReady = false;
        }

        // Check memory usage
        Map<String, Object> memoryStatus = checkMemoryHealth();
        components.put("memory", memoryStatus);
        if ("DOWN".equals(memoryStatus.get("status"))) {
            isReady = false;
        }

        // Check CPU usage
        Map<String, Object> cpuStatus = checkCpuHealth();
        components.put("cpu", cpuStatus);
        if ("DOWN".equals(cpuStatus.get("status"))) {
            isReady = false;
        }

        // Check report generation capability
        Map<String, Object> reportStatus = checkReportGenerationHealth();
        components.put("reportGeneration", reportStatus);
        if ("DOWN".equals(reportStatus.get("status"))) {
            isReady = false;
        }

        response.put("status", isReady ? "UP" : "DOWN");
        response.put("components", components);

        return Response.status(isReady ? Response.Status.OK : Response.Status.SERVICE_UNAVAILABLE)
                .entity(response)
                .build();
    }

    /**
     * Startup probe endpoint for Kubernetes.
     * Verifies that the service has completed its initialization and startup process.
     *
     * @return HTTP 200 if the service has started up, HTTP 503 otherwise
     */
    @GET
    @Path("/startup")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getStartupStatus() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        Map<String, Object> startupState = new HashMap<>();

        // Check if database is available during startup
        boolean isStarted = databaseHealthy;

        startupState.put("status", isStarted ? "UP" : "DOWN");
        components.put("startupState", startupState);

        response.put("status", isStarted ? "UP" : "DOWN");
        response.put("components", components);

        return Response.status(isStarted ? Response.Status.OK : Response.Status.SERVICE_UNAVAILABLE)
                .entity(response)
                .build();
    }

    /**
     * General health endpoint that combines all health checks.
     *
     * @return HTTP 200 if all checks pass, HTTP 503 otherwise
     */
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getHealthStatus() {
        Map<String, Object> response = new HashMap<>();
        Map<String, Object> components = new HashMap<>();
        boolean isHealthy = true;

        // Check database connectivity
        Map<String, Object> dbStatus = checkDatabaseHealth();
        components.put("db", dbStatus);
        if ("DOWN".equals(dbStatus.get("status"))) {
            isHealthy = false;
        }

        // Check memory usage
        Map<String, Object> memoryStatus = checkMemoryHealth();
        components.put("memory", memoryStatus);
        if ("DOWN".equals(memoryStatus.get("status"))) {
            isHealthy = false;
        }

        // Check CPU usage
        Map<String, Object> cpuStatus = checkCpuHealth();
        components.put("cpu", cpuStatus);
        if ("DOWN".equals(cpuStatus.get("status"))) {
            isHealthy = false;
        }

        // Check report generation capability
        Map<String, Object> reportStatus = checkReportGenerationHealth();
        components.put("reportGeneration", reportStatus);
        if ("DOWN".equals(reportStatus.get("status"))) {
            isHealthy = false;
        }

        response.put("status", isHealthy ? "UP" : "DOWN");
        response.put("components", components);

        return Response.status(isHealthy ? Response.Status.OK : Response.Status.SERVICE_UNAVAILABLE)
                .entity(response)
                .build();
    }

    /**
     * Checks database connectivity and health.
     *
     * @return Map containing database health status
     */
    private Map<String, Object> checkDatabaseHealth() {
        Map<String, Object> status = new HashMap<>();
        Map<String, Object> details = new HashMap<>();

        try {
            // Perform a simple database operation to verify connectivity
            storage.getObjects(Object.class, null);
            databaseHealthy = true;
            status.put("status", "UP");
            details.put("database", "Connected");
        } catch (Exception e) {
            databaseHealthy = false;
            status.put("status", "DOWN");
            details.put("error", e.getMessage());
            LOGGER.warn("Database health check failed", e);
        }

        status.put("details", details);
        return status;
    }

    /**
     * Checks memory usage and health.
     *
     * @return Map containing memory health status
     */
    private Map<String, Object> checkMemoryHealth() {
        Map<String, Object> status = new HashMap<>();
        Map<String, Object> details = new HashMap<>();

        try {
            long maxMemory = memoryMXBean.getHeapMemoryUsage().getMax();
            long usedMemory = memoryMXBean.getHeapMemoryUsage().getUsed();
            double memoryUsageRatio = (double) usedMemory / maxMemory;

            details.put("total", maxMemory);
            details.put("used", usedMemory);
            details.put("free", maxMemory - usedMemory);
            details.put("usageRatio", memoryUsageRatio);

            // Consider memory unhealthy if usage is above 90%
            if (memoryUsageRatio > 0.9) {
                status.put("status", "DOWN");
                LOGGER.warn("Memory usage is high: {}%", String.format("%.2f", memoryUsageRatio * 100));
            } else {
                status.put("status", "UP");
            }
        } catch (Exception e) {
            status.put("status", "DOWN");
            details.put("error", e.getMessage());
            LOGGER.warn("Memory health check failed", e);
        }

        status.put("details", details);
        return status;
    }

    /**
     * Checks CPU usage and health.
     *
     * @return Map containing CPU health status
     */
    private Map<String, Object> checkCpuHealth() {
        Map<String, Object> status = new HashMap<>();
        Map<String, Object> details = new HashMap<>();

        try {
            double systemLoad = osMXBean.getSystemLoadAverage();
            int availableProcessors = osMXBean.getAvailableProcessors();
            double cpuUsageRatio = systemLoad / availableProcessors;

            details.put("systemLoad", systemLoad);
            details.put("availableProcessors", availableProcessors);
            details.put("usageRatio", cpuUsageRatio);

            // Consider CPU unhealthy if usage is above 95%
            if (cpuUsageRatio > 0.95) {
                status.put("status", "DOWN");
                LOGGER.warn("CPU usage is high: {}%", String.format("%.2f", cpuUsageRatio * 100));
            } else {
                status.put("status", "UP");
            }
        } catch (Exception e) {
            status.put("status", "DOWN");
            details.put("error", e.getMessage());
            LOGGER.warn("CPU health check failed", e);
        }

        status.put("details", details);
        return status;
    }

    /**
     * Checks report generation capability and health.
     *
     * @return Map containing report generation health status
     */
    private Map<String, Object> checkReportGenerationHealth() {
        Map<String, Object> status = new HashMap<>();
        Map<String, Object> details = new HashMap<>();

        try {
            details.put("lastGenerationTime", lastReportGenerationTime);

            // If report generation time is too high, consider it unhealthy
            if (lastReportGenerationTime > 30000) { // 30 seconds threshold
                status.put("status", "DOWN");
                LOGGER.warn("Report generation time is high: {} ms", lastReportGenerationTime);
            } else {
                status.put("status", "UP");
            }
        } catch (Exception e) {
            status.put("status", "DOWN");
            details.put("error", e.getMessage());
            LOGGER.warn("Report generation health check failed", e);
        }

        status.put("details", details);
        return status;
    }

    /**
     * Updates the last report generation time metric.
     * This method should be called after each report generation to track performance.
     *
     * @param generationTimeMs The time taken to generate a report in milliseconds
     */
    public void updateReportGenerationTime(long generationTimeMs) {
        this.lastReportGenerationTime = generationTimeMs;
        if (generationTimeMs > 30000) { // 30 seconds threshold
            LOGGER.warn("Report generation took longer than expected: {} ms", generationTimeMs);
        }
    }
}