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
package org.traccar.discovery;

import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.schedule.ScheduleTask;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for scheduled tasks that need leader election in a distributed environment.
 * Only one instance of the task will run across all service instances.
 */
public abstract class ServiceDiscoveryScheduler implements ScheduleTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryScheduler.class);

    private final String taskName;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    
    @Inject
    private ServiceDiscoveryManager discoveryManager;

    protected ServiceDiscoveryScheduler(String taskName) {
        this.taskName = taskName;
    }

    @Override
    public boolean multipleInstances() {
        return false;
    }

    /**
     * Checks if this instance is the leader for the task.
     * 
     * @return true if this instance is the leader, false otherwise
     */
    protected boolean isLeader() {
        if (discoveryManager == null) {
            // If no discovery manager is available, assume this is the only instance
            return true;
        }
        
        // Try to acquire leadership if we don't have it
        if (!isLeader.get()) {
            boolean acquired = discoveryManager.acquireLeadership(taskName);
            if (acquired) {
                LOGGER.info("Acquired leadership for task: {}", taskName);
                isLeader.set(true);
            }
        } else {
            // Renew leadership if we already have it
            boolean renewed = discoveryManager.renewLeadership(taskName);
            if (!renewed) {
                LOGGER.info("Lost leadership for task: {}", taskName);
                isLeader.set(false);
            }
        }
        
        return isLeader.get();
    }

    /**
     * Releases leadership when the service is shutting down.
     */
    public void shutdown() {
        if (isLeader.get() && discoveryManager != null) {
            discoveryManager.releaseLeadership(taskName);
            isLeader.set(false);
            LOGGER.info("Released leadership for task: {}", taskName);
        }
    }
}