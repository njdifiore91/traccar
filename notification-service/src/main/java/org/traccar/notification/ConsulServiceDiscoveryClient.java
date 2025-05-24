package org.traccar.notification;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.agent.model.NewService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of ServiceDiscoveryClient for Consul service discovery.
 * <p>
 * This implementation uses the Consul API to register, renew, and deregister services.
 * It is activated when service.registration.type is set to "consul" or "auto" and
 * Consul is detected in the environment.
 */
@Component
@ConditionalOnProperty(name = "service.registration.type", havingValue = "consul", matchIfMissing = true)
public class ConsulServiceDiscoveryClient implements ServiceRegistration.ServiceDiscoveryClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsulServiceDiscoveryClient.class);

    private final ConsulClient consulClient;

    @Value("${consul.host:localhost}")
    private String consulHost;

    @Value("${consul.port:8500}")
    private int consulPort;

    /**
     * Creates a new ConsulServiceDiscoveryClient with the specified ConsulClient.
     *
     * @param consulClient The Consul client to use for API calls
     */
    @Autowired
    public ConsulServiceDiscoveryClient(ConsulClient consulClient) {
        this.consulClient = consulClient;
        LOGGER.info("Initialized Consul service discovery client with host: {}:{}", consulHost, consulPort);
    }

    /**
     * Registers a service with Consul.
     *
     * @param serviceId           The unique ID for this service instance
     * @param serviceName         The name of the service
     * @param host                The host address where the service is running
     * @param port                The port on which the service is listening
     * @param healthCheckPath     The path for health check requests
     * @param healthCheckInterval The interval for health checks
     * @param tags                Tags for service categorization
     * @param metadata            Additional service metadata
     */
    @Override
    public void register(String serviceId, String serviceName, String host, int port,
                         String healthCheckPath, String healthCheckInterval,
                         String[] tags, Map<String, String> metadata) {
        try {
            NewService newService = new NewService();
            newService.setId(serviceId);
            newService.setName(serviceName);
            newService.setAddress(host);
            newService.setPort(port);

            // Set tags
            List<String> tagList = new ArrayList<>();
            for (String tag : tags) {
                tagList.add(tag.trim());
            }
            newService.setTags(tagList);

            // Set metadata
            newService.setMeta(metadata);

            // Configure health check
            if (healthCheckPath != null && !healthCheckPath.isEmpty()) {
                NewService.Check check = new NewService.Check();
                String protocol = metadata.getOrDefault("secure", "false").equals("true") ? "https" : "http";
                check.setHttp(String.format("%s://%s:%d%s", protocol, host, port, healthCheckPath));
                check.setInterval(healthCheckInterval);
                check.setDeregisterCriticalServiceAfter("90m");
                newService.setCheck(check);
            }

            consulClient.agentServiceRegister(newService);
            LOGGER.info("Registered service with Consul: {} (ID: {})", serviceName, serviceId);
        } catch (Exception e) {
            LOGGER.error("Failed to register service with Consul", e);
            throw e;
        }
    }

    /**
     * Renews the service registration with Consul.
     * <p>
     * Note: Consul doesn't require explicit renewal as it uses health checks to determine service status.
     * This method performs a check to ensure the service is still registered.
     *
     * @param serviceId The unique ID for this service instance
     */
    @Override
    public void renew(String serviceId) {
        try {
            // Consul doesn't require explicit renewal, but we can check if the service is still registered
            consulClient.getAgentServices().getValue().get(serviceId);
            LOGGER.debug("Service registration is active in Consul: {}", serviceId);
        } catch (Exception e) {
            LOGGER.warn("Service registration check failed, service may need to be re-registered", e);
            throw e;
        }
    }

    /**
     * Deregisters a service from Consul.
     *
     * @param serviceId The unique ID for this service instance
     */
    @Override
    public void deregister(String serviceId) {
        try {
            consulClient.agentServiceDeregister(serviceId);
            LOGGER.info("Deregistered service from Consul: {}", serviceId);
        } catch (Exception e) {
            LOGGER.error("Failed to deregister service from Consul", e);
            throw e;
        }
    }
}