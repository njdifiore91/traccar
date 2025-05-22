/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.session;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.health.model.HealthService;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.ecwid.consul.v1.session.model.Session;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages service registration and discovery for the session management components.
 * This class integrates with Consul or Kubernetes for service registration, health checking,
 * and endpoint resolution. It enables dynamic discovery of other service instances for
 * coordination of distributed sessions.
 */
@Singleton
public class ServiceDiscoveryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryManager.class);

    private static final String SERVICE_NAME = "traccar-protocol-service";
    private static final String HEALTH_CHECK_PATH = "/actuator/health/liveness";
    private static final String LEADER_KEY_PREFIX = "service/leader/";
    private static final int HEALTH_CHECK_INTERVAL_SECONDS = 10;
    private static final int SERVICE_TTL_SECONDS = 30;
    private static final int REFRESH_INTERVAL_SECONDS = 15;

    private final org.traccar.config.Config config;
    private final String instanceId;
    private final ScheduledExecutorService executorService;
    private final DiscoveryType discoveryType;

    private ConsulClient consulClient;
    private CoreV1Api kubernetesApi;
    private String consulSessionId;
    private boolean isLeader;

    /**
     * Enum representing the supported service discovery mechanisms.
     */
    public enum DiscoveryType {
        CONSUL,
        KUBERNETES,
        NONE
    }

    /**
     * Constructs a new ServiceDiscoveryManager with the specified configuration.
     *
     * @param config The application configuration
     */
    @Inject
    public ServiceDiscoveryManager(org.traccar.config.Config config) {
        this.config = config;
        this.instanceId = generateInstanceId();
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.discoveryType = determineDiscoveryType();

        if (discoveryType != DiscoveryType.NONE) {
            initialize();
        } else {
            LOGGER.info("Service discovery is disabled");
        }
    }

    /**
     * Determines the discovery type based on configuration.
     *
     * @return The discovery type to use
     */
    private DiscoveryType determineDiscoveryType() {
        String discoveryTypeStr = config.getString(Keys.SERVICE_DISCOVERY_TYPE);
        if (discoveryTypeStr == null || discoveryTypeStr.isEmpty() || "none".equalsIgnoreCase(discoveryTypeStr)) {
            return DiscoveryType.NONE;
        } else if ("consul".equalsIgnoreCase(discoveryTypeStr)) {
            return DiscoveryType.CONSUL;
        } else if ("kubernetes".equalsIgnoreCase(discoveryTypeStr)) {
            return DiscoveryType.KUBERNETES;
        } else {
            LOGGER.warn("Unknown discovery type: {}, defaulting to NONE", discoveryTypeStr);
            return DiscoveryType.NONE;
        }
    }

    /**
     * Initializes the service discovery manager based on the configured discovery type.
     */
    private void initialize() {
        try {
            switch (discoveryType) {
                case CONSUL:
                    initializeConsul();
                    break;
                case KUBERNETES:
                    initializeKubernetes();
                    break;
                default:
                    // No initialization needed
                    break;
            }

            // Schedule periodic refresh of service instances
            executorService.scheduleAtFixedRate(
                    this::refreshServiceInstances,
                    REFRESH_INTERVAL_SECONDS,
                    REFRESH_INTERVAL_SECONDS,
                    TimeUnit.SECONDS);

            // Register shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(this::deregister));

            LOGGER.info("Service discovery initialized with type: {}, instance ID: {}", discoveryType, instanceId);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize service discovery", e);
        }
    }

    /**
     * Initializes Consul client and registers the service with Consul.
     */
    private void initializeConsul() {
        String consulHost = config.getString(Keys.SERVICE_CONSUL_HOST, "localhost");
        int consulPort = config.getInteger(Keys.SERVICE_CONSUL_PORT, 8500);

        consulClient = new ConsulClient(consulHost, consulPort);
        registerWithConsul();
        createConsulSession();
        attemptLeaderElection();
    }

    /**
     * Registers the current service instance with Consul.
     */
    private void registerWithConsul() {
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            int port = config.getInteger(Keys.WEB_PORT, 8082);

            NewService service = new NewService();
            service.setId(instanceId);
            service.setName(SERVICE_NAME);
            service.setAddress(host);
            service.setPort(port);

            // Add metadata tags
            List<String> tags = new ArrayList<>();
            tags.add("protocol-service");
            tags.add("version=" + config.getString(Keys.VERSION, "unknown"));
            service.setTags(tags);

            // Configure health check
            NewService.Check check = new NewService.Check();
            check.setHttp("http://" + host + ":" + port + HEALTH_CHECK_PATH);
            check.setInterval(HEALTH_CHECK_INTERVAL_SECONDS + "s");
            check.setDeregisterCriticalServiceAfter(SERVICE_TTL_SECONDS + "s");
            service.setCheck(check);

            consulClient.agentServiceRegister(service);
            LOGGER.info("Registered service with Consul: {}", instanceId);
        } catch (UnknownHostException e) {
            LOGGER.error("Failed to register service with Consul", e);
        }
    }

    /**
     * Creates a Consul session for leader election.
     */
    private void createConsulSession() {
        try {
            NewSession newSession = new NewSession();
            newSession.setName("traccar-leader-election");
            newSession.setTtl(SERVICE_TTL_SECONDS + "s");
            newSession.setBehavior(Session.Behavior.RELEASE);
            Response<String> sessionResponse = consulClient.sessionCreate(newSession, QueryParams.DEFAULT);
            consulSessionId = sessionResponse.getValue();
            LOGGER.debug("Created Consul session: {}", consulSessionId);

            // Schedule session renewal
            executorService.scheduleAtFixedRate(
                    this::renewConsulSession,
                    SERVICE_TTL_SECONDS / 2,
                    SERVICE_TTL_SECONDS / 2,
                    TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.error("Failed to create Consul session", e);
        }
    }

    /**
     * Renews the Consul session to keep it active.
     */
    private void renewConsulSession() {
        try {
            if (consulSessionId != null) {
                consulClient.sessionRenew(consulSessionId, QueryParams.DEFAULT);
                LOGGER.debug("Renewed Consul session: {}", consulSessionId);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to renew Consul session", e);
            // Try to recreate the session
            createConsulSession();
        }
    }

    /**
     * Attempts to acquire leadership through Consul's distributed lock mechanism.
     */
    private void attemptLeaderElection() {
        if (consulSessionId == null) {
            LOGGER.warn("Cannot attempt leader election without a valid Consul session");
            return;
        }

        String leaderKey = LEADER_KEY_PREFIX + SERVICE_NAME;
        PutParams putParams = new PutParams();
        putParams.setAcquireSession(consulSessionId);

        try {
            // Try to acquire the lock
            boolean acquired = consulClient.setKVValue(leaderKey, instanceId, putParams).getValue();
            isLeader = acquired;

            if (isLeader) {
                LOGGER.info("Successfully acquired leadership for service: {}", SERVICE_NAME);
            } else {
                LOGGER.info("Failed to acquire leadership, will operate as follower");
            }

            // Schedule periodic leadership check
            executorService.scheduleAtFixedRate(
                    this::checkLeadership,
                    REFRESH_INTERVAL_SECONDS,
                    REFRESH_INTERVAL_SECONDS,
                    TimeUnit.SECONDS);
        } catch (Exception e) {
            LOGGER.error("Error during leader election", e);
        }
    }

    /**
     * Checks if this instance is still the leader.
     */
    private void checkLeadership() {
        if (consulSessionId == null) {
            return;
        }

        String leaderKey = LEADER_KEY_PREFIX + SERVICE_NAME;
        try {
            Response<String> response = consulClient.getKVValue(leaderKey);
            if (response.getValue() != null) {
                boolean wasLeader = isLeader;
                isLeader = instanceId.equals(response.getValue());

                if (wasLeader && !isLeader) {
                    LOGGER.info("Lost leadership for service: {}", SERVICE_NAME);
                } else if (!wasLeader && isLeader) {
                    LOGGER.info("Acquired leadership for service: {}", SERVICE_NAME);
                }
            } else {
                // Key doesn't exist, try to acquire leadership
                attemptLeaderElection();
            }
        } catch (Exception e) {
            LOGGER.error("Error checking leadership status", e);
        }
    }

    /**
     * Initializes Kubernetes client and registers with Kubernetes service discovery.
     */
    private void initializeKubernetes() {
        try {
            // Initialize the Kubernetes client
            ApiClient client = Config.defaultClient();
            Configuration.setDefaultApiClient(client);
            kubernetesApi = new CoreV1Api();

            // Kubernetes doesn't require explicit registration as it's handled by the platform
            LOGGER.info("Initialized Kubernetes client for service discovery");

            // For Kubernetes, we don't need to implement leader election here as it's typically
            // handled by Kubernetes primitives like StatefulSets or specialized controllers
        } catch (IOException e) {
            LOGGER.error("Failed to initialize Kubernetes client", e);
        }
    }

    /**
     * Refreshes the list of service instances from the service discovery mechanism.
     */
    private void refreshServiceInstances() {
        try {
            switch (discoveryType) {
                case CONSUL:
                    refreshConsulInstances();
                    break;
                case KUBERNETES:
                    refreshKubernetesInstances();
                    break;
                default:
                    // No refresh needed
                    break;
            }
        } catch (Exception e) {
            LOGGER.error("Failed to refresh service instances", e);
        }
    }

    /**
     * Refreshes the list of service instances from Consul.
     */
    private void refreshConsulInstances() {
        try {
            Response<List<HealthService>> response = consulClient.getHealthServices(
                    SERVICE_NAME, true, QueryParams.DEFAULT);
            List<HealthService> services = response.getValue();

            LOGGER.debug("Discovered {} healthy service instances from Consul", services.size());
        } catch (Exception e) {
            LOGGER.error("Failed to refresh Consul service instances", e);
        }
    }

    /**
     * Refreshes the list of service instances from Kubernetes.
     */
    private void refreshKubernetesInstances() {
        try {
            String namespace = config.getString(Keys.SERVICE_KUBERNETES_NAMESPACE, "default");
            V1ServiceList serviceList = kubernetesApi.listNamespacedService(
                    namespace, null, null, null,
                    null, "app=" + SERVICE_NAME, null,
                    null, null, null, null);

            LOGGER.debug("Discovered {} service instances from Kubernetes", serviceList.getItems().size());
        } catch (ApiException e) {
            LOGGER.error("Failed to refresh Kubernetes service instances: {}", e.getResponseBody(), e);
        }
    }

    /**
     * Deregisters the service from the service discovery mechanism.
     */
    public void deregister() {
        try {
            switch (discoveryType) {
                case CONSUL:
                    deregisterFromConsul();
                    break;
                case KUBERNETES:
                    // Kubernetes handles deregistration automatically
                    break;
                default:
                    // No deregistration needed
                    break;
            }
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        } catch (Exception e) {
            LOGGER.error("Error during service deregistration", e);
        }
    }

    /**
     * Deregisters the service from Consul.
     */
    private void deregisterFromConsul() {
        try {
            // Release leadership if we are the leader
            if (isLeader && consulSessionId != null) {
                String leaderKey = LEADER_KEY_PREFIX + SERVICE_NAME;
                PutParams putParams = new PutParams();
                putParams.setReleaseSession(consulSessionId);
                consulClient.setKVValue(leaderKey, "", putParams);
            }

            // Destroy the session
            if (consulSessionId != null) {
                consulClient.sessionDestroy(consulSessionId, QueryParams.DEFAULT);
            }

            // Deregister the service
            consulClient.agentServiceDeregister(instanceId);
            LOGGER.info("Deregistered service from Consul: {}", instanceId);
        } catch (Exception e) {
            LOGGER.error("Failed to deregister service from Consul", e);
        }
    }

    /**
     * Generates a unique instance ID for this service instance.
     *
     * @return A unique instance ID
     */
    private String generateInstanceId() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "unknown";
        }
        return SERVICE_NAME + "-" + hostname + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Checks if this instance is the current leader.
     *
     * @return true if this instance is the leader, false otherwise
     */
    public boolean isLeader() {
        return discoveryType == DiscoveryType.NONE || isLeader;
    }

    /**
     * Gets the list of service instances for the specified service.
     *
     * @param serviceName The name of the service to discover
     * @return A list of service instances
     */
    public List<ServiceInstance> getServiceInstances(String serviceName) {
        try {
            switch (discoveryType) {
                case CONSUL:
                    return getConsulServiceInstances(serviceName);
                case KUBERNETES:
                    return getKubernetesServiceInstances(serviceName);
                default:
                    return Collections.emptyList();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to get service instances for {}", serviceName, e);
            return Collections.emptyList();
        }
    }

    /**
     * Gets the list of service instances from Consul for the specified service.
     *
     * @param serviceName The name of the service to discover
     * @return A list of service instances
     */
    private List<ServiceInstance> getConsulServiceInstances(String serviceName) {
        List<ServiceInstance> instances = new ArrayList<>();
        try {
            Response<List<HealthService>> response = consulClient.getHealthServices(
                    serviceName, true, QueryParams.DEFAULT);
            List<HealthService> services = response.getValue();

            for (HealthService service : services) {
                HealthService.Service serviceInfo = service.getService();
                ServiceInstance instance = new ServiceInstance(
                        serviceInfo.getId(),
                        serviceInfo.getAddress(),
                        serviceInfo.getPort(),
                        serviceInfo.getTags());
                instances.add(instance);
            }
        } catch (Exception e) {
            LOGGER.error("Failed to get Consul service instances for {}", serviceName, e);
        }
        return instances;
    }

    /**
     * Gets the list of service instances from Kubernetes for the specified service.
     *
     * @param serviceName The name of the service to discover
     * @return A list of service instances
     */
    private List<ServiceInstance> getKubernetesServiceInstances(String serviceName) {
        List<ServiceInstance> instances = new ArrayList<>();
        try {
            String namespace = config.getString(Keys.SERVICE_KUBERNETES_NAMESPACE, "default");
            V1ServiceList serviceList = kubernetesApi.listNamespacedService(
                    namespace, null, null, null,
                    null, "app=" + serviceName, null,
                    null, null, null, null);

            for (V1Service service : serviceList.getItems()) {
                String serviceId = service.getMetadata().getName();
                String host = serviceId + "." + namespace + ".svc.cluster.local";
                Integer port = service.getSpec().getPorts().get(0).getPort();
                List<String> tags = new ArrayList<>();
                if (service.getMetadata().getLabels() != null) {
                    service.getMetadata().getLabels().forEach((k, v) -> tags.add(k + "=" + v));
                }

                ServiceInstance instance = new ServiceInstance(serviceId, host, port, tags);
                instances.add(instance);
            }
        } catch (ApiException e) {
            LOGGER.error("Failed to get Kubernetes service instances for {}: {}", 
                    serviceName, e.getResponseBody(), e);
        }
        return instances;
    }

    /**
     * Represents a discovered service instance.
     */
    public static class ServiceInstance {
        private final String id;
        private final String host;
        private final int port;
        private final List<String> tags;

        /**
         * Constructs a new ServiceInstance.
         *
         * @param id   The instance ID
         * @param host The host address
         * @param port The port number
         * @param tags The service tags/metadata
         */
        public ServiceInstance(String id, String host, int port, List<String> tags) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.tags = tags != null ? tags : Collections.emptyList();
        }

        /**
         * Gets the instance ID.
         *
         * @return The instance ID
         */
        public String getId() {
            return id;
        }

        /**
         * Gets the host address.
         *
         * @return The host address
         */
        public String getHost() {
            return host;
        }

        /**
         * Gets the port number.
         *
         * @return The port number
         */
        public int getPort() {
            return port;
        }

        /**
         * Gets the service tags/metadata.
         *
         * @return The service tags/metadata
         */
        public List<String> getTags() {
            return tags;
        }

        /**
         * Gets the full URL for this service instance.
         *
         * @param secure Whether to use HTTPS
         * @return The full URL
         */
        public String getUrl(boolean secure) {
            String protocol = secure ? "https" : "http";
            return protocol + "://" + host + ":" + port;
        }

        @Override
        public String toString() {
            return "ServiceInstance{" +
                    "id='" + id + '\'' +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", tags=" + tags +
                    '}';
        }
    }
}