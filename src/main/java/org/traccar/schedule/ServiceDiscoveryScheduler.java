/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Base class for scheduled tasks that need leader election via service discovery.
 * Only one instance of the task will be active across all service instances.
 */
public abstract class ServiceDiscoveryScheduler implements ScheduleTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryScheduler.class);
    
    private final String leadershipRole;
    private final ServiceDiscovery serviceDiscovery;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    
    /**
     * Constructor for ServiceDiscoveryScheduler.
     * 
     * @param leadershipRole The role name used for leader election
     * @param serviceDiscovery The service discovery implementation
     */
    public ServiceDiscoveryScheduler(String leadershipRole, ServiceDiscovery serviceDiscovery) {
        this.leadershipRole = leadershipRole;
        this.serviceDiscovery = serviceDiscovery;
        
        // Start leadership election process
        startLeaderElection();
    }
    
    /**
     * Start the leader election process.
     */
    private void startLeaderElection() {
        try {
            serviceDiscovery.registerLeadershipListener(leadershipRole, this::onLeadershipChange);
            serviceDiscovery.acquireLeadership(leadershipRole);
            LOGGER.info("Registered for leadership role: {}", leadershipRole);
        } catch (Exception e) {
            LOGGER.warn("Failed to start leader election for role: {}", leadershipRole, e);
        }
    }
    
    /**
     * Callback when leadership status changes.
     * 
     * @param isLeader true if this instance is now the leader, false otherwise
     */
    private void onLeadershipChange(boolean isLeader) {
        this.isLeader.set(isLeader);
        if (isLeader) {
            LOGGER.info("Acquired leadership for role: {}", leadershipRole);
        } else {
            LOGGER.info("Lost leadership for role: {}", leadershipRole);
        }
    }
    
    /**
     * Check if this instance is currently the leader.
     * 
     * @return true if this instance is the leader, false otherwise
     */
    protected boolean isLeader() {
        return isLeader.get();
    }
    
    @Override
    public boolean multipleInstances() {
        // Always return false since we're using leader election
        return false;
    }
}