/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import io.prometheus.client.exporter.HTTPServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.HealthCheck;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.discovery.ServiceRegistryFactory;
import org.traccar.health.HealthController;
import org.traccar.health.MessageProcessingHealthIndicator;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.PositionMessageProducer;
import org.traccar.metrics.MetricsConfiguration;
import org.traccar.metrics.ProtocolMetrics;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

/**
 * Main entry point for the Protocol Service microservice.
 * Initializes the service, registers with service discovery, configures the message broker integration,
 * and starts the protocol servers.
 */
public final class ProtocolServiceMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolServiceMain.class);

    private static Injector injector;

    public static Injector getInjector() {
        return injector;
    }

    private ProtocolServiceMain() {
    }

    /**
     * Logs system information for diagnostics.
     */
    public static void logSystemInfo() {
        try {
            OperatingSystemMXBean operatingSystemBean = ManagementFactory.getOperatingSystemMXBean();
            LOGGER.info(
                    "Operating system name: {} version: {} architecture: {}",
                    operatingSystemBean.getName(), operatingSystemBean.getVersion(), operatingSystemBean.getArch());

            RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
            LOGGER.info(
                    "Java runtime name: {} vendor: {} version: {}",
                    runtimeBean.getVmName(), runtimeBean.getVmVendor(), runtimeBean.getVmVersion());

            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
            LOGGER.info(
                    "Memory limit heap: {}mb non-heap: {}mb",
                    memoryBean.getHeapMemoryUsage().getMax() / (1024 * 1024),
                    memoryBean.getNonHeapMemoryUsage().getMax() / (1024 * 1024));

            LOGGER.info("Character encoding: {}", Charset.defaultCharset().displayName());

        } catch (Exception error) {
            LOGGER.warn("Failed to get system info");
        }
    }

    /**
     * Main method to start the Protocol Service.
     * @param args Command line arguments
     * @throws Exception If an error occurs during startup
     */
    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.ENGLISH);

        final String configFile;
        if (args.length <= 0) {
            configFile = "./debug.xml";
            if (!new File(configFile).exists()) {
                throw new RuntimeException("Configuration file is not provided");
            }
        } else {
            configFile = args[args.length - 1];
        }

        run(configFile);
    }

    /**
     * Runs the Protocol Service with the specified configuration file.
     * @param configFile Path to the configuration file
     */
    public static void run(String configFile) {
        try {
            // Initialize dependency injection
            injector = Guice.createInjector(new ProtocolServiceModule(configFile));
            
            // Log system information
            logSystemInfo();
            LOGGER.info("Protocol Service Version: {}", ProtocolServiceMain.class.getPackage().getImplementationVersion());
            LOGGER.info("Starting Protocol Service...");

            // Initialize metrics
            MetricsConfiguration metricsConfig = injector.getInstance(MetricsConfiguration.class);
            ProtocolMetrics metrics = injector.getInstance(ProtocolMetrics.class);
            HTTPServer metricsServer = metricsConfig.createMetricsServer();
            LOGGER.info("Metrics server started on port {}", metricsConfig.getPort());

            // Initialize health check controller
            HealthController healthController = injector.getInstance(HealthController.class);
            healthController.start();
            LOGGER.info("Health check endpoints started");

            // Register with service discovery
            ServiceRegistry serviceRegistry = injector.getInstance(ServiceRegistryFactory.class).createServiceRegistry();
            serviceRegistry.register();
            LOGGER.info("Registered with service discovery");

            // Initialize message producers
            MessageProducer messageProducer = injector.getInstance(MessageProducer.class);
            PositionMessageProducer positionProducer = injector.getInstance(PositionMessageProducer.class);
            LOGGER.info("Message broker integration initialized");

            // Initialize and start protocol servers
            ServerManager serverManager = injector.getInstance(ServerManager.class);
            serverManager.start();
            LOGGER.info("Protocol servers started");

            // Initialize health check indicators
            MessageProcessingHealthIndicator healthIndicator = injector.getInstance(MessageProcessingHealthIndicator.class);
            HealthCheck healthCheck = injector.getInstance(HealthCheck.class);
            LOGGER.info("Health check indicators initialized");

            // Store lifecycle objects for proper shutdown
            var services = new ArrayList<LifecycleObject>();
            services.add(serverManager);
            services.add(healthController);

            // Set up uncaught exception handler
            Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOGGER.error("Thread exception", e));

            // Set up shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Stopping Protocol Service...");

                // Deregister from service discovery
                try {
                    serviceRegistry.deregister();
                    LOGGER.info("Deregistered from service discovery");
                } catch (Exception e) {
                    LOGGER.warn("Failed to deregister from service discovery", e);
                }

                // Stop all services
                for (var service : services) {
                    try {
                        service.stop();
                    } catch (Exception e) {
                        LOGGER.warn("Failed to stop service: {}", service.getClass().getSimpleName(), e);
                    }
                }

                // Shutdown metrics server
                try {
                    if (metricsServer != null) {
                        metricsServer.stop();
                        LOGGER.info("Metrics server stopped");
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to stop metrics server", e);
                }

                // Shutdown executor service
                injector.getInstance(ExecutorService.class).shutdown();
                LOGGER.info("Protocol Service stopped");
            }));

            LOGGER.info("Protocol Service started successfully");
        } catch (Exception e) {
            Throwable unwrapped;
            if (e instanceof ProvisionException) {
                unwrapped = e.getCause();
            } else {
                unwrapped = e;
            }
            LOGGER.error("Failed to start Protocol Service", unwrapped);
            System.exit(1);
        }
    }
}