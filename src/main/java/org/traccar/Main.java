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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.broadcast.BroadcastService;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.metrics.MicrometerMetricsManager;
import org.traccar.schedule.ScheduleManager;
import org.traccar.storage.DatabaseModule;
import org.traccar.telemetry.OpenTelemetryManager;
import org.traccar.web.WebModule;
import org.traccar.web.WebServer;

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

public final class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    private static Injector injector;

    public static Injector getInjector() {
        return injector;
    }

    private Main() {
    }

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

        if (args.length > 0 && args[0].startsWith("--")) {
            WindowsService windowsService = new WindowsService("traccar") {
                @Override
                public void run() {
                    Main.run(configFile);
                }
            };
            switch (args[0]) {
                case "--install":
                    windowsService.install("traccar", null, null, null, null, configFile);
                    return;
                case "--uninstall":
                    windowsService.uninstall();
                    return;
                case "--service":
                default:
                    windowsService.init();
                    break;
            }
        } else {
            run(configFile);
        }
    }

    public static void run(String configFile) {
        try {
            injector = Guice.createInjector(new MainModule(configFile), new DatabaseModule(), new WebModule());
            logSystemInfo();
            LOGGER.info("Version: {}", Main.class.getPackage().getImplementationVersion());
            LOGGER.info("Starting server...");

            // Detect execution mode (monolithic or microservices)
            Config config = injector.getInstance(Config.class);
            boolean microservicesMode = config.getBoolean(Keys.SERVER_MICROSERVICES_MODE);
            LOGGER.info("Execution mode: {}", microservicesMode ? "microservices" : "monolithic");

            // Initialize OpenTelemetry for distributed tracing
            OpenTelemetryManager openTelemetryManager = null;
            if (config.getBoolean(Keys.TELEMETRY_ENABLE)) {
                openTelemetryManager = injector.getInstance(OpenTelemetryManager.class);
                openTelemetryManager.start();
                LOGGER.info("OpenTelemetry tracing initialized");
            }

            // Initialize Micrometer for metrics collection
            MicrometerMetricsManager metricsManager = null;
            if (config.getBoolean(Keys.METRICS_ENABLE)) {
                metricsManager = injector.getInstance(MicrometerMetricsManager.class);
                metricsManager.start();
                LOGGER.info("Micrometer metrics initialized");
            }

            // Initialize message broker for asynchronous communication
            MessageBrokerManager messageBrokerManager = null;
            if (microservicesMode && config.getBoolean(Keys.MESSAGE_BROKER_ENABLE)) {
                messageBrokerManager = injector.getInstance(MessageBrokerManager.class);
                messageBrokerManager.start();
                LOGGER.info("Message broker initialized");
            }

            // Initialize service discovery and registry for microservices mode
            ServiceDiscovery serviceDiscovery = null;
            ServiceRegistry serviceRegistry = null;
            if (microservicesMode && config.getBoolean(Keys.SERVICE_DISCOVERY_ENABLE)) {
                serviceDiscovery = injector.getInstance(ServiceDiscovery.class);
                serviceRegistry = injector.getInstance(ServiceRegistry.class);
                serviceRegistry.start();
                LOGGER.info("Service discovery initialized");
            }

            var services = new ArrayList<LifecycleObject>();
            for (var clazz : List.of(
                    ScheduleManager.class, ServerManager.class, WebServer.class, BroadcastService.class)) {
                var service = injector.getInstance(clazz);
                if (service != null) {
                    service.start();
                    services.add(service);
                }
            }

            Thread.setDefaultUncaughtExceptionHandler((t, e) -> LOGGER.error("Thread exception", e));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOGGER.info("Stopping server...");

                // Graceful shutdown of all services
                for (var service : services) {
                    try {
                        service.stop();
                    } catch (Exception e) {
                        LOGGER.error("Error stopping service", e);
                    }
                }

                // Graceful shutdown of microservices components
                if (serviceRegistry != null) {
                    try {
                        serviceRegistry.stop();
                    } catch (Exception e) {
                        LOGGER.error("Error stopping service registry", e);
                    }
                }

                if (messageBrokerManager != null) {
                    try {
                        messageBrokerManager.stop();
                    } catch (Exception e) {
                        LOGGER.error("Error stopping message broker", e);
                    }
                }

                // Shutdown metrics and telemetry
                if (metricsManager != null) {
                    try {
                        metricsManager.stop();
                    } catch (Exception e) {
                        LOGGER.error("Error stopping metrics manager", e);
                    }
                }

                if (openTelemetryManager != null) {
                    try {
                        openTelemetryManager.stop();
                    } catch (Exception e) {
                        LOGGER.error("Error stopping OpenTelemetry manager", e);
                    }
                }

                injector.getInstance(ExecutorService.class).shutdown();
            }));
        } catch (Exception e) {
            Throwable unwrapped;
            if (e instanceof ProvisionException) {
                unwrapped = e.getCause();
            } else {
                unwrapped = e;
            }
            LOGGER.error("Main method error", unwrapped);
            System.exit(1);
        }
    }

}