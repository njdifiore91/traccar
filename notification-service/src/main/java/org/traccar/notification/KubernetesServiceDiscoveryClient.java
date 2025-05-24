package org.traccar.notification;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of ServiceDiscoveryClient for Kubernetes service discovery.
 * <p>
 * This implementation uses the Kubernetes API to register, renew, and deregister services.
 * It is activated when service.registration.type is set to "kubernetes".
 */
@Component
@ConditionalOnProperty(name = "service.registration.type", havingValue = "kubernetes")
public class KubernetesServiceDiscoveryClient implements ServiceRegistration.ServiceDiscoveryClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesServiceDiscoveryClient.class);

    private final CoreV1Api kubernetesApi;

    @Value("${kubernetes.namespace:default}")
    private String namespace;

    /**
     * Creates a new KubernetesServiceDiscoveryClient with the specified Kubernetes API client.
     *
     * @param apiClient The Kubernetes API client
     */
    @Autowired
    public KubernetesServiceDiscoveryClient(ApiClient apiClient) {
        this.kubernetesApi = new CoreV1Api(apiClient);
        LOGGER.info("Initialized Kubernetes service discovery client for namespace: {}", namespace);
    }

    /**
     * Registers a service with Kubernetes.
     * <p>
     * Note: In Kubernetes, services are typically defined in YAML and managed by the cluster.
     * This method creates a Kubernetes Service resource programmatically, which is less common
     * but can be useful for dynamic service registration.
     *
     * @param serviceId           The unique ID for this service instance
     * @param serviceName         The name of the service
     * @param host                The host address where the service is running
     * @param port                The port on which the service is listening
     * @param healthCheckPath     The path for health check requests (used in annotations)
     * @param healthCheckInterval The interval for health checks (used in annotations)
     * @param tags                Tags for service categorization (used in labels)
     * @param metadata            Additional service metadata (used in annotations)
     */
    @Override
    public void register(String serviceId, String serviceName, String host, int port,
                         String healthCheckPath, String healthCheckInterval,
                         String[] tags, Map<String, String> metadata) {
        try {
            // Check if service already exists
            try {
                kubernetesApi.readNamespacedService(serviceId, namespace, null);
                LOGGER.info("Service already exists in Kubernetes: {}", serviceId);
                return;
            } catch (ApiException e) {
                if (e.getCode() != 404) {
                    throw e;
                }
                // Service doesn't exist, continue with creation
            }

            // Create service labels
            Map<String, String> labels = new HashMap<>();
            labels.put("app", serviceName);
            labels.put("service-id", serviceId);
            for (String tag : tags) {
                labels.put("tag-" + tag.trim(), "true");
            }

            // Create service annotations
            Map<String, String> annotations = new HashMap<>(metadata);
            annotations.put("health-check-path", healthCheckPath);
            annotations.put("health-check-interval", healthCheckInterval);

            // Create service metadata
            V1ObjectMeta objectMeta = new V1ObjectMeta()
                    .name(serviceId)
                    .namespace(namespace)
                    .labels(labels)
                    .annotations(annotations);

            // Create service port
            V1ServicePort servicePort = new V1ServicePort()
                    .name("http")
                    .port(port)
                    .targetPort(new io.kubernetes.client.custom.IntOrString(port));

            // Create service selector
            Map<String, String> selector = new HashMap<>();
            selector.put("app", serviceName);

            // Create service spec
            V1ServiceSpec serviceSpec = new V1ServiceSpec()
                    .ports(Collections.singletonList(servicePort))
                    .selector(selector)
                    .type("ClusterIP");

            // Create service
            V1Service service = new V1Service()
                    .metadata(objectMeta)
                    .spec(serviceSpec);

            // Create the service in Kubernetes
            kubernetesApi.createNamespacedService(namespace, service, null, null, null);
            LOGGER.info("Registered service with Kubernetes: {} (ID: {})", serviceName, serviceId);
        } catch (Exception e) {
            LOGGER.error("Failed to register service with Kubernetes", e);
            throw new RuntimeException("Failed to register service with Kubernetes", e);
        }
    }

    /**
     * Renews the service registration with Kubernetes.
     * <p>
     * Note: Kubernetes doesn't require explicit renewal as it manages service lifecycle.
     * This method performs a check to ensure the service still exists.
     *
     * @param serviceId The unique ID for this service instance
     */
    @Override
    public void renew(String serviceId) {
        try {
            // Kubernetes doesn't require explicit renewal, but we can check if the service still exists
            kubernetesApi.readNamespacedService(serviceId, namespace, null);
            LOGGER.debug("Service registration is active in Kubernetes: {}", serviceId);
        } catch (ApiException e) {
            LOGGER.warn("Service registration check failed, service may need to be re-registered", e);
            throw new RuntimeException("Failed to verify service registration in Kubernetes", e);
        }
    }

    /**
     * Deregisters a service from Kubernetes.
     *
     * @param serviceId The unique ID for this service instance
     */
    @Override
    public void deregister(String serviceId) {
        try {
            kubernetesApi.deleteNamespacedService(serviceId, namespace, null, null, null, null, null, null);
            LOGGER.info("Deregistered service from Kubernetes: {}", serviceId);
        } catch (ApiException e) {
            if (e.getCode() == 404) {
                LOGGER.info("Service already removed from Kubernetes: {}", serviceId);
            } else {
                LOGGER.error("Failed to deregister service from Kubernetes", e);
                throw new RuntimeException("Failed to deregister service from Kubernetes", e);
            }
        }
    }
}