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
package org.traccar.schedule;

import com.google.inject.Injector;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.LifecycleObject;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.messaging.MessageProducer;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * ServiceDiscoveryScheduler integrates with Consul or Kubernetes service discovery to coordinate
 * scheduled task execution across multiple service instances. It provides leader election capabilities
 * to ensure that singleton tasks only run on one instance, while allowing distributed tasks to run
 * on multiple instances.
 */
@Singleton
public class ServiceDiscoveryScheduler implements LifecycleObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryScheduler.class);
    private static final String METRIC_PREFIX = "traccar_scheduler_discovery";
    private static final long LOCK_REFRESH_INTERVAL_MS = 5000; // 5 seconds
    private static final long HEALTH_CHECK_INTERVAL_MS = 10000; // 10 seconds
    private static final long LEADER_CHECK_INTERVAL_MS = 15000; // 15 seconds

    private final Config config;
    private final ServiceDiscovery serviceDiscovery;
    private final MessageProducer messageProducer;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final String instanceId;
    private final Map<String, ScheduledFuture<?>> lockRefreshers = new ConcurrentHashMap<>();
    private final Map<String, Boolean> taskLeaderStatus = new ConcurrentHashMap<>();
    private ScheduledExecutorService executor;
    private ScheduledFuture<?> healthCheckFuture;
    private ScheduledFuture<?> leaderCheckFuture;
    private boolean registered = false;

    /**
     * Constructs a new ServiceDiscoveryScheduler with the required dependencies.
     *
     * @param config The application configuration
     * @param serviceDiscovery Service discovery for distributed task coordination
     * @param messageProducer Message producer for task event publishing
     * @param meterRegistry Metrics registry for performance monitoring
     * @param openTelemetry OpenTelemetry for distributed tracing
     */
    @Inject
    public ServiceDiscoveryScheduler(Config config, ServiceDiscovery serviceDiscovery,
                                    MessageProducer messageProducer, MeterRegistry meterRegistry,
                                    OpenTelemetry openTelemetry) {
        this.config = config;
        this.serviceDiscovery = serviceDiscovery;
        this.messageProducer = messageProducer;
        this.meterRegistry = meterRegistry;
        this.tracer = openTelemetry.getTracer("org.traccar.scheduler.discovery");
        
        // Generate or get instance ID
        String configuredInstanceId = config.getString("scheduler.instanceId");
        this.instanceId = configuredInstanceId != null ? configuredInstanceId : UUID.randomUUID().toString();
        
        // Register metrics
        Counter leaderElectionCounter = Counter.builder(METRIC_PREFIX + ".leader_elections")
                .description("Number of leader elections participated in")
                .register(meterRegistry);
        
        Counter lockAcquisitionCounter = Counter.builder(METRIC_PREFIX + ".lock_acquisitions")
                .description("Number of distributed locks acquired")
                .register(meterRegistry);
        
        Counter lockReleaseCounter = Counter.builder(METRIC_PREFIX + ".lock_releases")
                .description("Number of distributed locks released")
                .register(meterRegistry);
    }

    /**
     * Starts the service discovery scheduler, registering with the service discovery system
     * and initializing health checks and leader election processes.
     */
    @Override
    public void start() {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.start").startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.info("Starting service discovery scheduler with instance ID: {}", instanceId);
            
            // Register this instance with service discovery
            registerWithServiceDiscovery();
            
            // Start health check reporting
            startHealthChecks();
            
            // Start leader election check
            startLeaderCheck();
            
            LOGGER.info("Service discovery scheduler started successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to start service discovery scheduler", e);
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Stops the service discovery scheduler, releasing all locks and deregistering from
     * the service discovery system.
     */
    @Override
    public void stop() {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.stop").startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.info("Stopping service discovery scheduler");
            
            // Stop health check reporting
            if (healthCheckFuture != null) {
                healthCheckFuture.cancel(false);
                healthCheckFuture = null;
            }
            
            // Stop leader check
            if (leaderCheckFuture != null) {
                leaderCheckFuture.cancel(false);
                leaderCheckFuture = null;
            }
            
            // Release all locks
            releaseAllLocks();
            
            // Deregister from service discovery
            deregisterFromServiceDiscovery();
            
            LOGGER.info("Service discovery scheduler stopped successfully");
        } catch (Exception e) {
            LOGGER.error("Error stopping service discovery scheduler", e);
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Sets the executor service to use for scheduling tasks.
     *
     * @param executor The executor service
     */
    public void setExecutor(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    /**
     * Acquires a distributed lock for a task to ensure it only runs on one instance.
     * If the lock is acquired, a background task is scheduled to refresh the lock periodically.
     *
     * @param taskName The name of the task to acquire a lock for
     * @return true if the lock was acquired, false otherwise
     */
    public boolean acquireLock(String taskName) {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.acquireLock")
                .setAttribute("task.name", taskName)
                .startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.debug("Attempting to acquire lock for task: {}", taskName);
            
            // Check if we already have this lock
            if (lockRefreshers.containsKey(taskName)) {
                LOGGER.debug("Lock already held for task: {}", taskName);
                return true;
            }
            
            // Try to acquire the lock
            String lockPath = "tasks/" + taskName + "/lock";
            long lockTtl = config.getLong("scheduler.lockTtl", 30); // 30 seconds default
            
            boolean acquired = serviceDiscovery.acquireLock(lockPath, instanceId, Duration.ofSeconds(lockTtl));
            
            if (acquired) {
                LOGGER.info("Acquired lock for task: {}", taskName);
                
                // Schedule lock refresh
                ScheduledFuture<?> refreshFuture = executor.scheduleAtFixedRate(
                        () -> refreshLock(taskName, lockPath, lockTtl),
                        LOCK_REFRESH_INTERVAL_MS,
                        LOCK_REFRESH_INTERVAL_MS,
                        TimeUnit.MILLISECONDS);
                
                lockRefreshers.put(taskName, refreshFuture);
                
                // Update leader status
                taskLeaderStatus.put(taskName, true);
                
                // Publish event
                publishLockEvent(taskName, "acquired");
                
                // Update metrics
                Counter lockAcquisitionCounter = meterRegistry.counter(METRIC_PREFIX + ".lock_acquisitions");
                lockAcquisitionCounter.increment();
                
                return true;
            } else {
                LOGGER.debug("Failed to acquire lock for task: {}", taskName);
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Error acquiring lock for task: {}", taskName, e);
            span.recordException(e);
            return false;
        } finally {
            span.end();
        }
    }

    /**
     * Releases a distributed lock for a task, allowing other instances to acquire it.
     *
     * @param taskName The name of the task to release the lock for
     * @return true if the lock was released, false otherwise
     */
    public boolean releaseLock(String taskName) {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.releaseLock")
                .setAttribute("task.name", taskName)
                .startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.debug("Releasing lock for task: {}", taskName);
            
            // Check if we have this lock
            ScheduledFuture<?> refreshFuture = lockRefreshers.remove(taskName);
            if (refreshFuture == null) {
                LOGGER.debug("No lock held for task: {}", taskName);
                return true;
            }
            
            // Cancel the refresh task
            refreshFuture.cancel(false);
            
            // Release the lock
            String lockPath = "tasks/" + taskName + "/lock";
            boolean released = serviceDiscovery.releaseLock(lockPath, instanceId);
            
            if (released) {
                LOGGER.info("Released lock for task: {}", taskName);
                
                // Update leader status
                taskLeaderStatus.put(taskName, false);
                
                // Publish event
                publishLockEvent(taskName, "released");
                
                // Update metrics
                Counter lockReleaseCounter = meterRegistry.counter(METRIC_PREFIX + ".lock_releases");
                lockReleaseCounter.increment();
                
                return true;
            } else {
                LOGGER.warn("Failed to release lock for task: {}", taskName);
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Error releasing lock for task: {}", taskName, e);
            span.recordException(e);
            return false;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if this instance is the leader for a specific task.
     * This is used to determine if singleton tasks should run on this instance.
     *
     * @param taskName The name of the task to check leadership for
     * @return true if this instance is the leader for the task, false otherwise
     */
    public boolean isLeaderForTask(String taskName) {
        // Check our cached leader status first
        Boolean isLeader = taskLeaderStatus.get(taskName);
        if (isLeader != null) {
            return isLeader;
        }
        
        // If we don't have a cached status, check with service discovery
        String lockPath = "tasks/" + taskName + "/lock";
        String lockHolder = serviceDiscovery.getLockHolder(lockPath);
        
        boolean result = instanceId.equals(lockHolder);
        taskLeaderStatus.put(taskName, result);
        
        return result;
    }

    /**
     * Refreshes a distributed lock to prevent it from expiring.
     *
     * @param taskName The name of the task
     * @param lockPath The path of the lock in the service discovery system
     * @param lockTtl The time-to-live for the lock in seconds
     */
    private void refreshLock(String taskName, String lockPath, long lockTtl) {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.refreshLock")
                .setAttribute("task.name", taskName)
                .startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.debug("Refreshing lock for task: {}", taskName);
            
            boolean refreshed = serviceDiscovery.refreshLock(lockPath, instanceId, Duration.ofSeconds(lockTtl));
            
            if (!refreshed) {
                LOGGER.warn("Failed to refresh lock for task: {}. Lock may have been lost.", taskName);
                
                // Try to reacquire the lock
                boolean reacquired = serviceDiscovery.acquireLock(lockPath, instanceId, Duration.ofSeconds(lockTtl));
                
                if (reacquired) {
                    LOGGER.info("Reacquired lock for task: {}", taskName);
                } else {
                    LOGGER.warn("Failed to reacquire lock for task: {}", taskName);
                    
                    // Update leader status
                    taskLeaderStatus.put(taskName, false);
                    
                    // Publish event
                    publishLockEvent(taskName, "lost");
                    
                    // Cancel the refresh task
                    ScheduledFuture<?> refreshFuture = lockRefreshers.remove(taskName);
                    if (refreshFuture != null) {
                        refreshFuture.cancel(false);
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error refreshing lock for task: {}", taskName, e);
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Releases all distributed locks held by this instance.
     */
    private void releaseAllLocks() {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.releaseAllLocks").startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.info("Releasing all locks");
            
            // Make a copy of the keys to avoid concurrent modification
            for (String taskName : lockRefreshers.keySet().toArray(new String[0])) {
                try {
                    releaseLock(taskName);
                } catch (Exception e) {
                    LOGGER.error("Error releasing lock for task: {}", taskName, e);
                }
            }
            
            // Clear collections
            lockRefreshers.clear();
            taskLeaderStatus.clear();
        } finally {
            span.end();
        }
    }

    /**
     * Registers this instance with the service discovery system.
     */
    private void registerWithServiceDiscovery() {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.registerWithServiceDiscovery").startSpan();
        try (var scope = span.makeCurrent()) {
            if (registered) {
                LOGGER.debug("Already registered with service discovery");
                return;
            }
            
            LOGGER.info("Registering with service discovery as instance: {}", instanceId);
            
            // Prepare service metadata
            Map<String, String> metadata = new HashMap<>();
            metadata.put("instanceId", instanceId);
            metadata.put("startTime", String.valueOf(System.currentTimeMillis()));
            metadata.put("version", config.getString("server.version", "unknown"));
            
            // Register the service
            String serviceName = config.getString("scheduler.serviceName", "traccar-scheduler");
            int servicePort = config.getInteger("scheduler.servicePort", 8080);
            
            boolean success = serviceDiscovery.register(serviceName, instanceId, servicePort, metadata);
            
            if (success) {
                LOGGER.info("Successfully registered with service discovery");
                registered = true;
            } else {
                LOGGER.error("Failed to register with service discovery");
            }
        } catch (Exception e) {
            LOGGER.error("Error registering with service discovery", e);
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Deregisters this instance from the service discovery system.
     */
    private void deregisterFromServiceDiscovery() {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.deregisterFromServiceDiscovery").startSpan();
        try (var scope = span.makeCurrent()) {
            if (!registered) {
                LOGGER.debug("Not registered with service discovery");
                return;
            }
            
            LOGGER.info("Deregistering from service discovery");
            
            // Deregister the service
            String serviceName = config.getString("scheduler.serviceName", "traccar-scheduler");
            
            boolean success = serviceDiscovery.deregister(serviceName, instanceId);
            
            if (success) {
                LOGGER.info("Successfully deregistered from service discovery");
                registered = false;
            } else {
                LOGGER.error("Failed to deregister from service discovery");
            }
        } catch (Exception e) {
            LOGGER.error("Error deregistering from service discovery", e);
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Starts periodic health check reporting to the service discovery system.
     */
    private void startHealthChecks() {
        if (executor == null) {
            LOGGER.warn("Executor not set, cannot start health checks");
            return;
        }
        
        LOGGER.info("Starting health check reporting");
        
        healthCheckFuture = executor.scheduleAtFixedRate(
                this::reportHealthStatus,
                0,
                HEALTH_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Reports the health status of this instance to the service discovery system.
     */
    private void reportHealthStatus() {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.reportHealthStatus").startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.debug("Reporting health status to service discovery");
            
            // Prepare health check data
            Map<String, Object> healthData = new HashMap<>();
            healthData.put("status", "UP");
            healthData.put("timestamp", System.currentTimeMillis());
            healthData.put("instanceId", instanceId);
            healthData.put("taskCount", lockRefreshers.size());
            
            // Report health status
            String serviceName = config.getString("scheduler.serviceName", "traccar-scheduler");
            boolean success = serviceDiscovery.reportHealth(serviceName, instanceId, healthData);
            
            if (!success) {
                LOGGER.warn("Failed to report health status to service discovery");
            }
        } catch (Exception e) {
            LOGGER.error("Error reporting health status", e);
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Starts periodic leader election checks for tasks.
     */
    private void startLeaderCheck() {
        if (executor == null) {
            LOGGER.warn("Executor not set, cannot start leader checks");
            return;
        }
        
        LOGGER.info("Starting leader election checks");
        
        leaderCheckFuture = executor.scheduleAtFixedRate(
                this::checkLeaderStatus,
                LEADER_CHECK_INTERVAL_MS,
                LEADER_CHECK_INTERVAL_MS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Checks the leader status for all tasks and updates the local cache.
     */
    private void checkLeaderStatus() {
        Span span = tracer.spanBuilder("serviceDiscoveryScheduler.checkLeaderStatus").startSpan();
        try (var scope = span.makeCurrent()) {
            LOGGER.debug("Checking leader status for tasks");
            
            // Update leader status for all tasks we're tracking
            for (String taskName : taskLeaderStatus.keySet()) {
                String lockPath = "tasks/" + taskName + "/lock";
                String lockHolder = serviceDiscovery.getLockHolder(lockPath);
                
                boolean isLeader = instanceId.equals(lockHolder);
                boolean wasLeader = taskLeaderStatus.getOrDefault(taskName, false);
                
                if (isLeader != wasLeader) {
                    LOGGER.info("Leader status changed for task {}: {} -> {}", taskName, wasLeader, isLeader);
                    taskLeaderStatus.put(taskName, isLeader);
                    
                    // Update metrics
                    Counter leaderElectionCounter = meterRegistry.counter(METRIC_PREFIX + ".leader_elections");
                    leaderElectionCounter.increment();
                    
                    // Publish event
                    if (isLeader) {
                        publishLockEvent(taskName, "acquired");
                    } else {
                        publishLockEvent(taskName, "lost");
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error checking leader status", e);
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Publishes a lock event to the message broker.
     *
     * @param taskName The name of the task
     * @param status The lock status (acquired, released, lost)
     */
    private void publishLockEvent(String taskName, String status) {
        try {
            if (messageProducer != null) {
                Map<String, Object> event = new HashMap<>();
                event.put("taskName", taskName);
                event.put("status", status);
                event.put("timestamp", System.currentTimeMillis());
                event.put("instanceId", instanceId);
                
                messageProducer.publish("scheduler.locks", event);
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to publish lock event for task: {}", taskName, e);
        }
    }

    /**
     * Gets the instance ID of this scheduler instance.
     *
     * @return the instance ID
     */
    public String getInstanceId() {
        return instanceId;
    }
}