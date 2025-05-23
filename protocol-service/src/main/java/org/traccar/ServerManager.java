/*
 * Copyright 2012 - 2025 Anton Tananaev (anton@traccar.org)
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

import com.google.inject.Injector;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.health.HealthCheckManager;
import org.traccar.health.HealthStatus;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Server manager for the Protocol Service microservice.
 * Manages the lifecycle of all protocol servers, integrates with service discovery,
 * provides health check status reporting, and collects metrics for monitoring.
 * Optimized for containerized deployment in Kubernetes environment.
 */
@Singleton
public class ServerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerManager.class);

    private final Injector injector;
    private final ProtocolServiceConfig config;
    private final MeterRegistry meterRegistry;
    private final ServiceRegistry serviceRegistry;
    private final HealthCheckManager healthCheckManager;

    private final Map<String, TrackerServer> servers = new ConcurrentHashMap<>();
    private final List<TrackerServer> serverList = new CopyOnWriteArrayList<>();
    private final Set<String> enabledProtocols = new HashSet<>();

    private ServiceInstance serviceInstance;
    private boolean running;

    /**
     * Constructs a new ServerManager with the specified dependencies.
     *
     * @param injector The Guice injector for creating server instances
     * @param config The protocol service configuration
     * @param meterRegistry The meter registry for metrics collection
     * @param serviceRegistry The service registry for service discovery
     * @param healthCheckManager The health check manager for health status reporting
     */
    @Inject
    public ServerManager(
            Injector injector,
            ProtocolServiceConfig config,
            MeterRegistry meterRegistry,
            ServiceRegistry serviceRegistry,
            HealthCheckManager healthCheckManager) {
        this.injector = injector;
        this.config = config;
        this.meterRegistry = meterRegistry;
        this.serviceRegistry = serviceRegistry;
        this.healthCheckManager = healthCheckManager;

        // Register health check provider
        healthCheckManager.registerHealthCheck("servers", this::getHealthStatus);

        // Register metrics
        Gauge.builder("protocol_service_servers", servers, Map::size)
                .description("Number of protocol servers")
                .register(meterRegistry);

        Gauge.builder("protocol_service_enabled_protocols", enabledProtocols, Set::size)
                .description("Number of enabled protocols")
                .register(meterRegistry);

        Gauge.builder("protocol_service_active_connections", this, ServerManager::getTotalActiveConnections)
                .description("Total number of active connections across all servers")
                .register(meterRegistry);
    }

    /**
     * Initializes the server manager by loading protocol configurations.
     * In a Kubernetes environment, this can dynamically load protocols from ConfigMaps.
     */
    public void init() {
        LOGGER.info("Initializing Protocol Service server manager");

        // Load enabled protocols from configuration
        loadEnabledProtocols();

        // Register service with discovery
        registerService();

        LOGGER.info("Protocol Service server manager initialized with {} enabled protocols", enabledProtocols.size());
    }

    /**
     * Loads the list of enabled protocols from configuration.
     * In a Kubernetes environment, this can load from ConfigMaps or environment variables.
     */
    private void loadEnabledProtocols() {
        // First check environment variable for comma-separated list of protocols
        String protocolsEnv = System.getenv("TRACCAR_PROTOCOLS");
        if (protocolsEnv != null && !protocolsEnv.isEmpty()) {
            String[] protocols = protocolsEnv.split(",");
            for (String protocol : protocols) {
                enabledProtocols.add(protocol.trim());
            }
            LOGGER.info("Loaded {} protocols from environment variable", enabledProtocols.size());
            return;
        }

        // Then check configuration file
        Collection<String> protocols = config.getStringList("protocols");
        if (protocols != null && !protocols.isEmpty()) {
            enabledProtocols.addAll(protocols);
            LOGGER.info("Loaded {} protocols from configuration file", enabledProtocols.size());
            return;
        }

        // Finally, try to discover protocols from class files
        try {
            discoverProtocols();
            LOGGER.info("Discovered {} protocols from class files", enabledProtocols.size());
        } catch (IOException e) {
            LOGGER.warn("Failed to discover protocols from class files", e);
        }

        if (enabledProtocols.isEmpty()) {
            LOGGER.warn("No protocols enabled, server will not accept any connections");
        }
    }

    /**
     * Discovers available protocols by scanning the classpath.
     * This is a fallback method if protocols are not specified in configuration.
     *
     * @throws IOException If an I/O error occurs during protocol discovery
     */
    private void discoverProtocols() throws IOException {
        String packageName = "org.traccar.protocol";
        String packagePath = packageName.replace('.', '/');

        // Try to find protocol directory in the classpath
        String classPath = System.getProperty("java.class.path");
        String[] classPathEntries = classPath.split(System.getProperty("path.separator"));

        for (String classPathEntry : classPathEntries) {
            Path dir = Paths.get(classPathEntry, packagePath);
            if (Files.isDirectory(dir)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, "*Protocol.class")) {
                    for (Path path : stream) {
                        String fileName = path.getFileName().toString();
                        String protocolName = fileName.substring(0, fileName.length() - "Protocol.class".length());
                        enabledProtocols.add(protocolName.toLowerCase());
                    }
                }
            }
        }

        // Also check for protocols in JAR files
        // This is important for containerized deployments where classes are in JARs
        try {
            Class<?>[] classes = ClasspathHelper.getClasses(packageName);
            for (Class<?> clazz : classes) {
                String className = clazz.getSimpleName();
                if (className.endsWith("Protocol")) {
                    String protocolName = className.substring(0, className.length() - "Protocol".length());
                    enabledProtocols.add(protocolName.toLowerCase());
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to scan JAR files for protocols", e);
        }
    }

    /**
     * Registers the Protocol Service with the service discovery mechanism.
     */
    private void registerService() {
        String host = getServiceHost();
        int port = config.getInteger("web.port", 8082);

        serviceInstance = ServiceInstance.builder()
                .id("protocol-service-" + host + "-" + port)
                .name("protocol-service")
                .host(host)
                .port(port)
                .metadata("protocols", String.join(",", enabledProtocols))
                .healthCheckEndpoint("/health")
                .build();

        serviceRegistry.register(serviceInstance);
        LOGGER.info("Registered Protocol Service with service registry as {}", serviceInstance.getId());
    }

    /**
     * Gets the host to use for service registration.
     * In Kubernetes, this should be the pod IP or service name.
     *
     * @return The host to register
     */
    private String getServiceHost() {
        // In Kubernetes, use pod IP from environment variable if available
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isEmpty()) {
            return podIp;
        }

        // Fall back to hostname
        String hostname = System.getenv("HOSTNAME");
        if (hostname != null && !hostname.isEmpty()) {
            return hostname;
        }

        // Last resort: use configured web server address
        return config.getString("web.address", "0.0.0.0");
    }

    /**
     * Starts all configured protocol servers.
     */
    public void start() {
        LOGGER.info("Starting Protocol Service servers");

        running = true;

        for (String protocol : enabledProtocols) {
            startServer(protocol);
        }

        LOGGER.info("Started {} Protocol Service servers", servers.size());
    }

    /**
     * Starts a server for the specified protocol.
     *
     * @param protocol The protocol name
     */
    private void startServer(String protocol) {
        String protocolName = protocol.toLowerCase();

        // Skip if already started
        if (servers.containsKey(protocolName)) {
            LOGGER.warn("Protocol server for {} is already started", protocolName);
            return;
        }

        // Check if protocol is enabled
        if (!enabledProtocols.contains(protocolName)) {
            LOGGER.warn("Protocol {} is not enabled", protocolName);
            return;
        }

        // Get protocol-specific configuration
        String configKey = protocolName + ".port";
        int port = config.getInteger(configKey);
        if (port == 0) {
            LOGGER.warn("Port not configured for protocol {}", protocolName);
            return;
        }

        // Create and start the server
        try {
            // Get protocol-specific pipeline factory
            String factoryName = protocolName + "Pipeline";
            PipelineFactory pipelineFactory = injector.getInstance(PipelineFactory.class);

            // Get bind address
            String address = config.getString(protocolName + ".address", config.getString("server.address", "0.0.0.0"));

            // Check if SSL is enabled for this protocol
            boolean ssl = config.getBoolean(protocolName + ".ssl");

            // Create the server
            TrackerServer server = injector.getInstance(TrackerServer.class);
            server.start();

            // Store the server
            servers.put(protocolName, server);
            serverList.add(server);

            // Register server metrics
            registerServerMetrics(server);

            LOGGER.info("Started {} server on {}:{} (SSL: {})", protocolName, address, port, ssl);
        } catch (Exception e) {
            LOGGER.warn("Failed to start {} server", protocolName, e);
        }
    }

    /**
     * Registers metrics for a server.
     *
     * @param server The server to register metrics for
     */
    private void registerServerMetrics(TrackerServer server) {
        String protocol = server.getProtocol();
        Tags tags = Tags.of("protocol", protocol, "secure", String.valueOf(server.isSecure()));

        // Register server-specific metrics
        Gauge.builder("protocol_server_active_connections", server, TrackerServer::getActiveConnections)
                .description("Number of active connections for a specific protocol server")
                .tags(tags)
                .register(meterRegistry);
    }

    /**
     * Stops all protocol servers.
     */
    public void stop() {
        LOGGER.info("Stopping Protocol Service servers");

        running = false;

        // Deregister from service registry
        if (serviceInstance != null) {
            serviceRegistry.deregister(serviceInstance);
            LOGGER.info("Deregistered Protocol Service from service registry");
        }

        // Stop all servers with a grace period
        List<Future<?>> futures = new ArrayList<>();
        for (TrackerServer server : serverList) {
            Future<?> future = server.stop();
            if (future != null) {
                futures.add(future);
            }
        }

        // Wait for all servers to stop
        for (Future<?> future : futures) {
            try {
                future.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for server to stop", e);
                Thread.currentThread().interrupt();
            }
        }

        servers.clear();
        serverList.clear();

        LOGGER.info("All Protocol Service servers stopped");
    }

    /**
     * Gets the health status of the server manager.
     *
     * @return The health status
     */
    public HealthStatus getHealthStatus() {
        boolean healthy = running && !servers.isEmpty();
        boolean ready = healthy && servers.values().stream().allMatch(TrackerServer::isReady);

        String status = healthy ? "UP" : "DOWN";
        String details = String.format(
                "Running: %s, Servers: %d, Enabled Protocols: %d, Active Connections: %d",
                running, servers.size(), enabledProtocols.size(), getTotalActiveConnections());

        return new HealthStatus(healthy, ready, status, details);
    }

    /**
     * Gets the total number of active connections across all servers.
     *
     * @return The total number of active connections
     */
    public int getTotalActiveConnections() {
        return servers.values().stream()
                .mapToInt(TrackerServer::getActiveConnections)
                .sum();
    }

    /**
     * Gets the list of all servers.
     *
     * @return An unmodifiable list of all servers
     */
    public List<TrackerServer> getServers() {
        return Collections.unmodifiableList(serverList);
    }

    /**
     * Gets a server by protocol name.
     *
     * @param protocol The protocol name
     * @return The server, or null if not found
     */
    public TrackerServer getServer(String protocol) {
        return servers.get(protocol.toLowerCase());
    }

    /**
     * Gets the set of enabled protocols.
     *
     * @return An unmodifiable set of enabled protocols
     */
    public Set<String> getEnabledProtocols() {
        return Collections.unmodifiableSet(enabledProtocols);
    }

    /**
     * Gets whether the server manager is running.
     *
     * @return true if running, false otherwise
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * Helper class for scanning classpath for protocol classes.
     */
    private static class ClasspathHelper {

        /**
         * Gets all classes in a package.
         *
         * @param packageName The package name
         * @return An array of classes in the package
         * @throws ClassNotFoundException If a class cannot be found
         * @throws IOException If an I/O error occurs
         */
        public static Class<?>[] getClasses(String packageName) throws ClassNotFoundException, IOException {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            String path = packageName.replace('.', '/');
            java.util.Enumeration<java.net.URL> resources = classLoader.getResources(path);
            List<java.io.File> dirs = new ArrayList<>();
            while (resources.hasMoreElements()) {
                java.net.URL resource = resources.nextElement();
                dirs.add(new java.io.File(resource.getFile()));
            }
            List<Class<?>> classes = new ArrayList<>();
            for (java.io.File directory : dirs) {
                classes.addAll(findClasses(directory, packageName));
            }
            return classes.toArray(new Class[0]);
        }

        /**
         * Finds classes in a directory.
         *
         * @param directory The directory to search
         * @param packageName The package name
         * @return A list of classes in the directory
         * @throws ClassNotFoundException If a class cannot be found
         */
        private static List<Class<?>> findClasses(java.io.File directory, String packageName) throws ClassNotFoundException {
            List<Class<?>> classes = new ArrayList<>();
            if (!directory.exists()) {
                return classes;
            }
            java.io.File[] files = directory.listFiles();
            if (files == null) {
                return classes;
            }
            for (java.io.File file : files) {
                if (file.isDirectory()) {
                    classes.addAll(findClasses(file, packageName + "." + file.getName()));
                } else if (file.getName().endsWith(".class")) {
                    classes.add(Class.forName(packageName + '.' +
                            file.getName().substring(0, file.getName().length() - 6)));
                }
            }
            return classes;
        }
    }
}