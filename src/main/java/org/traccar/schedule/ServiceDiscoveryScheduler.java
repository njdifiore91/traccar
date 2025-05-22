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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides leader election capabilities for scheduled tasks in a distributed environment.
 * Uses service discovery to determine which instance should run singleton tasks.
 */
public class ServiceDiscoveryScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryScheduler.class);
    
    private final ServiceDiscovery serviceDiscovery;
    private final String taskName;
    private final String instanceId;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private ScheduledFuture<?> leaderCheckFuture;
    private ScheduledFuture<?> taskFuture;

    /**
     * Creates a new ServiceDiscoveryScheduler.
     *
     * @param serviceDiscovery The service discovery implementation to use for leader election
     * @param taskName The name of the task for which to elect a leader
     */
    public ServiceDiscoveryScheduler(ServiceDiscovery serviceDiscovery, String taskName) {
        this.serviceDiscovery = serviceDiscovery;
        this.taskName = taskName;
        this.instanceId = UUID.randomUUID().toString();
    }

    /**
     * Schedules a task to run only on the elected leader instance.
     *
     * @param executor The executor service to use for scheduling
     * @param task The task to run
     * @param initialDelay The initial delay before the first execution
     * @param period The period between successive executions
     * @param unit The time unit for the initial delay and period
     */
    public void scheduleWithLeaderElection(
            ScheduledExecutorService executor,
            Runnable task,
            long initialDelay,
            long period,
            TimeUnit unit) {
        
        // Schedule the leader election check to run frequently
        leaderCheckFuture = executor.scheduleAtFixedRate(
                this::checkLeadership,
                0,
                Math.min(period / 4, 15), // Run leadership check more frequently than the task
                unit);
        
        // Schedule the actual task, but it will only execute if this instance is the leader
        taskFuture = executor.scheduleAtFixedRate(
                () -> {
                    if (isLeader.get()) {
                        LOGGER.debug("Running task '{}' as leader", taskName);
                        task.run();
                    } else {
                        LOGGER.trace("Skipping task '{}' execution as non-leader", taskName);
                    }
                },
                initialDelay,
                period,
                unit);
    }

    /**
     * Checks if this instance should be the leader for the task.
     * The instance with the lowest ID among healthy instances becomes the leader.
     */
    private void checkLeadership() {
        try {
            // Get all healthy instances of this service
            List<ServiceInstance> instances = serviceDiscovery.findHealthyInstances("traccar");
            
            if (instances.isEmpty()) {
                // If no instances found, assume we're the only one and become leader
                if (!isLeader.getAndSet(true)) {
                    LOGGER.info("No other instances found, assuming leadership for task '{}'", taskName);
                }
                return;
            }
            
            // Sort instances by ID to ensure consistent leader selection
            instances.sort((a, b) -> a.getId().compareTo(b.getId()));
            
            // The instance with the lowest ID becomes the leader
            ServiceInstance leader = instances.get(0);
            boolean shouldBeLeader = leader.getId().equals(instanceId);
            
            // Update leadership status if it changed
            if (shouldBeLeader != isLeader.get()) {
                isLeader.set(shouldBeLeader);
                if (shouldBeLeader) {
                    LOGGER.info("Acquired leadership for task '{}'", taskName);
                } else {
                    LOGGER.info("Relinquished leadership for task '{}'", taskName);
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Error during leadership check for task '{}'", taskName, e);
        }
    }

    /**
     * Cancels the scheduled task and leadership check.
     */
    public void cancel() {
        if (leaderCheckFuture != null) {
            leaderCheckFuture.cancel(false);
        }
        if (taskFuture != null) {
            taskFuture.cancel(false);
        }
    }

    /**
     * @return true if this instance is currently the leader, false otherwise
     */
    public boolean isLeader() {
        return isLeader.get();
    }
}