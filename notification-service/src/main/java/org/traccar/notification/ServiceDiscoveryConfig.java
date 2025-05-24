package org.traccar.notification;

import com.ecwid.consul.v1.ConsulClient;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * Configuration class for service discovery clients.
 * <p>
 * This class provides beans for Consul and Kubernetes service discovery clients
 * based on configuration properties.
 */
@Configuration
public class ServiceDiscoveryConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryConfig.class);

    @Value("${consul.host:localhost}")
    private String consulHost;

    @Value("${consul.port:8500}")
    private int consulPort;

    /**
     * Creates a ConsulClient bean for Consul service discovery.
     *
     * @return A configured ConsulClient instance
     */
    @Bean
    @ConditionalOnProperty(name = "service.registration.type", havingValue = "consul", matchIfMissing = true)
    public ConsulClient consulClient() {
        LOGGER.info("Creating Consul client with host: {}:{}", consulHost, consulPort);
        return new ConsulClient(consulHost, consulPort);
    }

    /**
     * Creates an ApiClient bean for Kubernetes service discovery.
     *
     * @return A configured Kubernetes ApiClient instance
     * @throws IOException If the Kubernetes client configuration cannot be loaded
     */
    @Bean
    @ConditionalOnProperty(name = "service.registration.type", havingValue = "kubernetes")
    public ApiClient kubernetesClient() throws IOException {
        LOGGER.info("Creating Kubernetes API client");
        ApiClient client;
        try {
            // Try to use in-cluster config first
            client = Config.fromCluster();
            LOGGER.info("Using in-cluster Kubernetes configuration");
        } catch (Exception e) {
            // Fall back to kubeconfig file
            LOGGER.info("In-cluster configuration not available, using default kubeconfig");
            client = Config.defaultClient();
        }
        return client;
    }

    /**
     * Creates a fallback ServiceDiscoveryClient when no specific implementation is available.
     * This is used when service registration is disabled or when running in an environment
     * without service discovery support.
     *
     * @return A no-op ServiceDiscoveryClient implementation
     */
    @Bean
    @ConditionalOnMissingBean(ServiceRegistration.ServiceDiscoveryClient.class)
    public ServiceRegistration.ServiceDiscoveryClient noopServiceDiscoveryClient() {
        LOGGER.info("Creating no-op service discovery client");
        return new ServiceRegistration.ServiceDiscoveryClient() {
            @Override
            public void register(String serviceId, String serviceName, String host, int port,
                               String healthCheckPath, String healthCheckInterval,
                               String[] tags, Map<String, String> metadata) {
                LOGGER.info("No-op service registration for service: {} (ID: {})", serviceName, serviceId);
            }

            @Override
            public void renew(String serviceId) {
                LOGGER.debug("No-op service renewal for service ID: {}", serviceId);
            }

            @Override
            public void deregister(String serviceId) {
                LOGGER.info("No-op service deregistration for service ID: {}", serviceId);
            }
        };
    }
}