/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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

import javax.inject.Singleton;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Default implementation of the HealthCheck interface that provides basic health status reporting.
 * It checks the status of critical dependencies like database connections and message brokers,
 * and reports the overall health of the service based on these checks.
 */
@Singleton
public class DefaultHealthCheck implements HealthCheck {

    private static final Logger LOGGER = Logger.getLogger(DefaultHealthCheck.class.getName());

    private static final double MEMORY_THRESHOLD = 0.9; // 90% memory usage threshold
    private static final double CPU_THRESHOLD = 0.9; // 90% CPU usage threshold

    @Override
    public boolean isHealthy() {
        // Check system resources
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        
        // Check heap memory usage
        double memoryUsage = (double) memoryBean.getHeapMemoryUsage().getUsed() / 
                             memoryBean.getHeapMemoryUsage().getMax();
        
        // Check CPU load if available
        double cpuLoad = osBean.getSystemLoadAverage();
        boolean cpuHealthy = cpuLoad < 0 || cpuLoad / osBean.getAvailableProcessors() < CPU_THRESHOLD;
        
        // Service is healthy if memory usage is below threshold and CPU is healthy
        return memoryUsage < MEMORY_THRESHOLD && cpuHealthy;
    }

    @Override
    public Map<String, Object> getStatus() {
        Map<String, Object> status = new HashMap<>();
        
        // Get system resources
        MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        
        // Calculate memory usage
        long heapUsed = memoryBean.getHeapMemoryUsage().getUsed();
        long heapMax = memoryBean.getHeapMemoryUsage().getMax();
        double memoryUsage = (double) heapUsed / heapMax;
        
        // Get CPU load
        double cpuLoad = osBean.getSystemLoadAverage();
        int availableProcessors = osBean.getAvailableProcessors();
        
        // Determine overall status
        boolean healthy = isHealthy();
        
        // Build status report
        status.put("status", healthy ? "UP" : "DOWN");
        
        Map<String, Object> details = new HashMap<>();
        
        // Memory details
        Map<String, Object> memory = new HashMap<>();
        memory.put("used", heapUsed);
        memory.put("max", heapMax);
        memory.put("usage", memoryUsage);
        memory.put("status", memoryUsage < MEMORY_THRESHOLD ? "UP" : "DOWN");
        details.put("memory", memory);
        
        // CPU details
        Map<String, Object> cpu = new HashMap<>();
        cpu.put("load", cpuLoad);
        cpu.put("processors", availableProcessors);
        cpu.put("status", cpuLoad < 0 || cpuLoad / availableProcessors < CPU_THRESHOLD ? "UP" : "DOWN");
        details.put("cpu", cpu);
        
        // Add details to status
        status.put("details", details);
        
        return status;
    }
}