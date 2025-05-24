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

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration class for service registry settings specific to the Event Processing Service.
 * This class centralizes all configuration related to service discovery and registration,
 * making it easier to manage and update service discovery settings.
 */
@Configuration
@ConfigurationProperties(prefix = "service.registry")
@Validated
public class ServiceRegistryConfig {

    /**
     * The type of service registry to use (CONSUL or KUBERNETES).
     */
    public enum RegistryType {
        CONSUL, KUBERNETES
    }

    /**
     * The type of service registry to use.
     */
    @NotNull(message = "Registry type must be specified (CONSUL or KUBERNETES)")
    private RegistryType type;

    /**
     * The host address of the service registry.
     */
    @NotBlank(message = "Registry host must not be empty")
    private String host;

    /**
     * The port of the service registry.
     */
    @Min(value = 1, message = "Registry port must be at least 1")
    @Max(value = 65535, message = "Registry port must be at most 65535")
    private int port;

    /**
     * Whether to use HTTPS for connecting to the service registry.
     */
    private boolean secure = false;

    /**
     * The service ID to use when registering with the service registry.
     * Defaults to "event-service" if not specified.
     */
    private String serviceId = "event-service";

    /**
     * The host address to register with the service registry.
     * If not specified, the system will attempt to determine the host automatically.
     */
    private String serviceHost;

    /**
     * The port to register with the service registry.
     */
    @Min(value = 1, message = "Service port must be at least 1")
    @Max(value = 65535, message = "Service port must be at most 65535")
    private int servicePort = 8080;

    /**
     * Whether the service uses HTTPS.
     */
    private boolean serviceSecure = false;

    /**
     * The interval at which to send health check requests.
     */
    private Duration healthCheckInterval = Duration.ofSeconds(10);

    /**
     * The timeout for health check requests.
     */
    private Duration healthCheckTimeout = Duration.ofSeconds(5);

    /**
     * The path to use for health check requests.
     */
    private String healthCheckPath = "/actuator/health";

    /**
     * Whether to deregister the service when it shuts down.
     */
    private boolean deregisterOnShutdown = true;

    /**
     * Whether to register the service automatically on startup.
     */
    private boolean autoRegister = true;

    /**
     * Additional metadata to include when registering the service.
     * This can include environment, version, and other service-specific information.
     */
    private Map<String, String> metadata = new HashMap<>();

    /**
     * Event service specific configuration properties.
     */
    private EventServiceConfig eventService = new EventServiceConfig();

    /**
     * Configuration specific to the Event Processing Service.
     */
    public static class EventServiceConfig {
        /**
         * The types of events this service instance can process.
         * If empty, the service can process all event types.
         */
        private String[] supportedEventTypes = {};

        /**
         * The maximum number of events this service instance can process concurrently.
         */
        @Min(value = 1, message = "Max concurrent events must be at least 1")
        private int maxConcurrentEvents = 100;

        /**
         * Whether this service instance should process geofence events.
         */
        private boolean processGeofenceEvents = true;

        /**
         * Whether this service instance should process device status events.
         */
        private boolean processDeviceStatusEvents = true;

        /**
         * Whether this service instance should process command result events.
         */
        private boolean processCommandResultEvents = true;

        /**
         * Whether this service instance should process driver events.
         */
        private boolean processDriverEvents = true;

        /**
         * Whether this service instance should process motion events.
         */
        private boolean processMotionEvents = true;

        /**
         * Whether this service instance should process overspeed events.
         */
        private boolean processOverspeedEvents = true;

        /**
         * Whether this service instance should process fuel events.
         */
        private boolean processFuelEvents = true;

        /**
         * Whether this service instance should process alarm events.
         */
        private boolean processAlarmEvents = true;

        public String[] getSupportedEventTypes() {
            return supportedEventTypes;
        }

        public void setSupportedEventTypes(String[] supportedEventTypes) {
            this.supportedEventTypes = supportedEventTypes;
        }

        public int getMaxConcurrentEvents() {
            return maxConcurrentEvents;
        }

        public void setMaxConcurrentEvents(int maxConcurrentEvents) {
            this.maxConcurrentEvents = maxConcurrentEvents;
        }

        public boolean isProcessGeofenceEvents() {
            return processGeofenceEvents;
        }

        public void setProcessGeofenceEvents(boolean processGeofenceEvents) {
            this.processGeofenceEvents = processGeofenceEvents;
        }

        public boolean isProcessDeviceStatusEvents() {
            return processDeviceStatusEvents;
        }

        public void setProcessDeviceStatusEvents(boolean processDeviceStatusEvents) {
            this.processDeviceStatusEvents = processDeviceStatusEvents;
        }

        public boolean isProcessCommandResultEvents() {
            return processCommandResultEvents;
        }

        public void setProcessCommandResultEvents(boolean processCommandResultEvents) {
            this.processCommandResultEvents = processCommandResultEvents;
        }

        public boolean isProcessDriverEvents() {
            return processDriverEvents;
        }

        public void setProcessDriverEvents(boolean processDriverEvents) {
            this.processDriverEvents = processDriverEvents;
        }

        public boolean isProcessMotionEvents() {
            return processMotionEvents;
        }

        public void setProcessMotionEvents(boolean processMotionEvents) {
            this.processMotionEvents = processMotionEvents;
        }

        public boolean isProcessOverspeedEvents() {
            return processOverspeedEvents;
        }

        public void setProcessOverspeedEvents(boolean processOverspeedEvents) {
            this.processOverspeedEvents = processOverspeedEvents;
        }

        public boolean isProcessFuelEvents() {
            return processFuelEvents;
        }

        public void setProcessFuelEvents(boolean processFuelEvents) {
            this.processFuelEvents = processFuelEvents;
        }

        public boolean isProcessAlarmEvents() {
            return processAlarmEvents;
        }

        public void setProcessAlarmEvents(boolean processAlarmEvents) {
            this.processAlarmEvents = processAlarmEvents;
        }
    }

    /**
     * Configuration specific to Consul service registry.
     */
    private ConsulConfig consul = new ConsulConfig();

    /**
     * Configuration specific to Kubernetes service registry.
     */
    private KubernetesConfig kubernetes = new KubernetesConfig();

    /**
     * Configuration specific to Consul service registry.
     */
    public static class ConsulConfig {
        /**
         * The ACL token to use when connecting to Consul.
         */
        private String aclToken;

        /**
         * The datacenter to use when connecting to Consul.
         */
        private String datacenter;

        /**
         * Whether to use Consul's catalog API instead of the agent API.
         */
        private boolean useCatalog = false;

        /**
         * The tags to apply to the service when registering with Consul.
         */
        private String[] tags = {"event-service", "traccar"};

        /**
         * Whether to register with Consul's connect service mesh.
         */
        private boolean connectEnabled = false;

        public String getAclToken() {
            return aclToken;
        }

        public void setAclToken(String aclToken) {
            this.aclToken = aclToken;
        }

        public String getDatacenter() {
            return datacenter;
        }

        public void setDatacenter(String datacenter) {
            this.datacenter = datacenter;
        }

        public boolean isUseCatalog() {
            return useCatalog;
        }

        public void setUseCatalog(boolean useCatalog) {
            this.useCatalog = useCatalog;
        }

        public String[] getTags() {
            return tags;
        }

        public void setTags(String[] tags) {
            this.tags = tags;
        }

        public boolean isConnectEnabled() {
            return connectEnabled;
        }

        public void setConnectEnabled(boolean connectEnabled) {
            this.connectEnabled = connectEnabled;
        }
    }

    /**
     * Configuration specific to Kubernetes service registry.
     */
    public static class KubernetesConfig {
        /**
         * The namespace to use when connecting to Kubernetes.
         */
        private String namespace = "default";

        /**
         * The service account token path to use when connecting to Kubernetes.
         */
        private String serviceAccountTokenPath = "/var/run/secrets/kubernetes.io/serviceaccount/token";

        /**
         * Whether to trust all certificates when connecting to Kubernetes.
         */
        private boolean trustCerts = false;

        /**
         * The labels to apply to the service when registering with Kubernetes.
         */
        private Map<String, String> labels = new HashMap<>();

        /**
         * The annotations to apply to the service when registering with Kubernetes.
         */
        private Map<String, String> annotations = new HashMap<>();

        /**
         * Whether to use the pod IP for registration instead of the service name.
         */
        private boolean usePodIP = false;

        public String getNamespace() {
            return namespace;
        }

        public void setNamespace(String namespace) {
            this.namespace = namespace;
        }

        public String getServiceAccountTokenPath() {
            return serviceAccountTokenPath;
        }

        public void setServiceAccountTokenPath(String serviceAccountTokenPath) {
            this.serviceAccountTokenPath = serviceAccountTokenPath;
        }

        public boolean isTrustCerts() {
            return trustCerts;
        }

        public void setTrustCerts(boolean trustCerts) {
            this.trustCerts = trustCerts;
        }

        public Map<String, String> getLabels() {
            return labels;
        }

        public void setLabels(Map<String, String> labels) {
            this.labels = labels;
        }

        public Map<String, String> getAnnotations() {
            return annotations;
        }

        public void setAnnotations(Map<String, String> annotations) {
            this.annotations = annotations;
        }

        public boolean isUsePodIP() {
            return usePodIP;
        }

        public void setUsePodIP(boolean usePodIP) {
            this.usePodIP = usePodIP;
        }
    }

    // Getters and setters

    public RegistryType getType() {
        return type;
    }

    public void setType(RegistryType type) {
        this.type = type;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public boolean isSecure() {
        return secure;
    }

    public void setSecure(boolean secure) {
        this.secure = secure;
    }

    public String getServiceId() {
        return serviceId;
    }

    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    public String getServiceHost() {
        return serviceHost;
    }

    public void setServiceHost(String serviceHost) {
        this.serviceHost = serviceHost;
    }

    public int getServicePort() {
        return servicePort;
    }

    public void setServicePort(int servicePort) {
        this.servicePort = servicePort;
    }

    public boolean isServiceSecure() {
        return serviceSecure;
    }

    public void setServiceSecure(boolean serviceSecure) {
        this.serviceSecure = serviceSecure;
    }

    public Duration getHealthCheckInterval() {
        return healthCheckInterval;
    }

    public void setHealthCheckInterval(Duration healthCheckInterval) {
        this.healthCheckInterval = healthCheckInterval;
    }

    public Duration getHealthCheckTimeout() {
        return healthCheckTimeout;
    }

    public void setHealthCheckTimeout(Duration healthCheckTimeout) {
        this.healthCheckTimeout = healthCheckTimeout;
    }

    public String getHealthCheckPath() {
        return healthCheckPath;
    }

    public void setHealthCheckPath(String healthCheckPath) {
        this.healthCheckPath = healthCheckPath;
    }

    public boolean isDeregisterOnShutdown() {
        return deregisterOnShutdown;
    }

    public void setDeregisterOnShutdown(boolean deregisterOnShutdown) {
        this.deregisterOnShutdown = deregisterOnShutdown;
    }

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    public EventServiceConfig getEventService() {
        return eventService;
    }

    public void setEventService(EventServiceConfig eventService) {
        this.eventService = eventService;
    }

    public ConsulConfig getConsul() {
        return consul;
    }

    public void setConsul(ConsulConfig consul) {
        this.consul = consul;
    }

    public KubernetesConfig getKubernetes() {
        return kubernetes;
    }

    public void setKubernetes(KubernetesConfig kubernetes) {
        this.kubernetes = kubernetes;
    }
}