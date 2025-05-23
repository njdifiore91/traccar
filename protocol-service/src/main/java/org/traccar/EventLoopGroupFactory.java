/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.netty.util.internal.PlatformDependent;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory for creating Netty EventLoopGroups optimized for containerized environments.
 * Provides container-aware thread allocation, metrics collection, and graceful shutdown support.
 */
public final class EventLoopGroupFactory {

    private static final String CGROUP_CPU_QUOTA_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_quota_us";
    private static final String CGROUP_CPU_PERIOD_PATH = "/sys/fs/cgroup/cpu/cpu.cfs_period_us";
    private static final String CGROUP_V2_CPU_MAX_PATH = "/sys/fs/cgroup/cpu.max";
    
    private static final int DEFAULT_BOSS_THREAD_COUNT = 1;
    private static final int DEFAULT_WORKER_THREAD_MULTIPLIER = 2;
    private static final int MIN_WORKER_THREADS = 2;
    
    private static MeterRegistry meterRegistry;
    
    private EventLoopGroupFactory() {
        // Utility class
    }
    
    /**
     * Set the meter registry for metrics collection.
     * 
     * @param registry The Micrometer registry for metrics collection
     */
    public static void setMeterRegistry(MeterRegistry registry) {
        meterRegistry = registry;
    }

    /**
     * Creates a boss EventLoopGroup with optimized thread count.
     * 
     * @param threadNamePrefix Prefix for thread names
     * @return Configured EventLoopGroup for boss threads
     */
    public static EventLoopGroup createBossGroup(String threadNamePrefix) {
        return createEventLoopGroup(DEFAULT_BOSS_THREAD_COUNT, threadNamePrefix + "-boss");
    }

    /**
     * Creates a worker EventLoopGroup with optimized thread count based on available resources.
     * 
     * @param threadNamePrefix Prefix for thread names
     * @return Configured EventLoopGroup for worker threads
     */
    public static EventLoopGroup createWorkerGroup(String threadNamePrefix) {
        int threadCount = calculateOptimalThreadCount();
        return createEventLoopGroup(threadCount, threadNamePrefix + "-worker");
    }

    /**
     * Creates an EventLoopGroup with the specified thread count and name prefix.
     * 
     * @param threadCount Number of threads in the group
     * @param threadNamePrefix Prefix for thread names
     * @return Configured EventLoopGroup
     */
    public static EventLoopGroup createEventLoopGroup(int threadCount, String threadNamePrefix) {
        ThreadFactory threadFactory = new DefaultThreadFactory(threadNamePrefix, true);
        EventLoopGroup eventLoopGroup;
        
        if (PlatformDependent.isLinux() && PlatformDependent.hasUnsafe()) {
            eventLoopGroup = new EpollEventLoopGroup(threadCount, threadFactory);
        } else {
            eventLoopGroup = new NioEventLoopGroup(threadCount, threadFactory);
        }
        
        if (meterRegistry != null) {
            registerMetrics(eventLoopGroup, threadNamePrefix);
        }
        
        return eventLoopGroup;
    }
    
    /**
     * Calculates the optimal thread count based on available CPU resources.
     * Takes into account container CPU limits if running in a containerized environment.
     * 
     * @return Optimal number of worker threads
     */
    private static int calculateOptimalThreadCount() {
        double cpuLimit = detectContainerCpuLimit();
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        
        if (cpuLimit > 0 && cpuLimit < availableProcessors) {
            // Use container CPU limit if available and less than reported processors
            int threadCount = Math.max(MIN_WORKER_THREADS, (int) Math.ceil(cpuLimit * DEFAULT_WORKER_THREAD_MULTIPLIER));
            return threadCount;
        } else {
            // Fall back to default calculation based on available processors
            return Math.max(MIN_WORKER_THREADS, availableProcessors * DEFAULT_WORKER_THREAD_MULTIPLIER);
        }
    }
    
    /**
     * Detects CPU limits in containerized environments by reading cgroup information.
     * 
     * @return CPU limit as a decimal value (e.g., 2.5 for 2.5 CPUs), or -1 if not detected
     */
    private static double detectContainerCpuLimit() {
        // Try to read from cgroup v2 first
        try {
            File cgroupV2File = new File(CGROUP_V2_CPU_MAX_PATH);
            if (cgroupV2File.exists()) {
                try (BufferedReader reader = new BufferedReader(new FileReader(cgroupV2File))) {
                    String line = reader.readLine();
                    if (line != null && !line.startsWith("max")) {
                        String[] parts = line.split("\\s+");
                        if (parts.length >= 2) {
                            long quotaUs = Long.parseLong(parts[0]);
                            long periodUs = Long.parseLong(parts[1]);
                            if (quotaUs > 0 && periodUs > 0) {
                                return (double) quotaUs / periodUs;
                            }
                        }
                    }
                }
            }
        } catch (IOException | NumberFormatException e) {
            // Fall through to cgroup v1
        }
        
        // Try cgroup v1
        try {
            File quotaFile = new File(CGROUP_CPU_QUOTA_PATH);
            File periodFile = new File(CGROUP_CPU_PERIOD_PATH);
            
            if (quotaFile.exists() && periodFile.exists()) {
                try (BufferedReader quotaReader = new BufferedReader(new FileReader(quotaFile));
                     BufferedReader periodReader = new BufferedReader(new FileReader(periodFile))) {
                    
                    long quotaUs = Long.parseLong(quotaReader.readLine().trim());
                    long periodUs = Long.parseLong(periodReader.readLine().trim());
                    
                    if (quotaUs > 0 && periodUs > 0) {
                        return (double) quotaUs / periodUs;
                    }
                }
            }
        } catch (IOException | NumberFormatException e) {
            // Fall through to environment variables
        }
        
        // Try Kubernetes/Docker environment variables
        try {
            String cpuLimit = System.getenv("CPU_LIMIT");
            if (cpuLimit != null && !cpuLimit.isEmpty()) {
                return Double.parseDouble(cpuLimit);
            }
        } catch (NumberFormatException e) {
            // Ignore and return default
        }
        
        return -1; // Not detected
    }
    
    /**
     * Registers metrics for the EventLoopGroup with Micrometer.
     * 
     * @param group The EventLoopGroup to monitor
     * @param name Name prefix for the metrics
     */
    private static void registerMetrics(EventLoopGroup group, String name) {
        Tags tags = Tags.of("name", name);
        
        // Register pending tasks metric
        Gauge.builder("netty.eventloop.pending.tasks", group, EventLoopGroupFactory::getPendingTasks)
                .tags(tags)
                .description("Number of pending tasks in the EventLoopGroup")
                .register(meterRegistry);
        
        // Register thread utilization metric (percentage of busy threads)
        Gauge.builder("netty.eventloop.utilization", group, EventLoopGroupFactory::getThreadUtilization)
                .tags(tags)
                .description("Percentage of busy threads in the EventLoopGroup")
                .register(meterRegistry);
        
        // Register total thread count metric
        Gauge.builder("netty.eventloop.thread.count", group, EventLoopGroupFactory::getThreadCount)
                .tags(tags)
                .description("Total number of threads in the EventLoopGroup")
                .register(meterRegistry);
    }
    
    /**
     * Gets the total number of pending tasks across all event loops in the group.
     * 
     * @param group The EventLoopGroup to check
     * @return Total number of pending tasks
     */
    private static long getPendingTasks(EventLoopGroup group) {
        long pendingTasks = 0;
        for (EventExecutor executor : group) {
            if (executor instanceof SingleThreadEventExecutor) {
                pendingTasks += ((SingleThreadEventExecutor) executor).pendingTasks();
            }
        }
        return pendingTasks;
    }
    
    /**
     * Calculates the thread utilization as a percentage (0-100).
     * 
     * @param group The EventLoopGroup to check
     * @return Percentage of busy threads
     */
    private static double getThreadUtilization(EventLoopGroup group) {
        int totalThreads = 0;
        int busyThreads = 0;
        
        for (EventExecutor executor : group) {
            totalThreads++;
            if (executor.inEventLoop() || ((SingleThreadEventExecutor) executor).pendingTasks() > 0) {
                busyThreads++;
            }
        }
        
        return totalThreads > 0 ? (busyThreads * 100.0) / totalThreads : 0;
    }
    
    /**
     * Gets the total number of threads in the EventLoopGroup.
     * 
     * @param group The EventLoopGroup to check
     * @return Total number of threads
     */
    private static int getThreadCount(EventLoopGroup group) {
        AtomicInteger count = new AtomicInteger(0);
        group.forEach(executor -> count.incrementAndGet());
        return count.get();
    }
    
    /**
     * Gracefully shuts down an EventLoopGroup with proper handling for Kubernetes lifecycle.
     * Allows in-flight requests to complete before terminating.
     * 
     * @param group The EventLoopGroup to shut down
     * @param quietPeriod The quiet period in seconds
     * @param timeout The maximum amount of time to wait in seconds
     * @return true if the shutdown completed successfully, false if it timed out
     */
    public static boolean shutdownGracefully(EventLoopGroup group, int quietPeriod, int timeout) {
        if (group != null) {
            try {
                return group.shutdownGracefully(quietPeriod, timeout, TimeUnit.SECONDS).await(timeout, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        return false;
    }
}