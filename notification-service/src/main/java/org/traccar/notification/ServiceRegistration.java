package org.traccar.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Handles service registration with the service discovery system (Consul/Kubernetes),
 * enabling other services to locate and communicate with the Notification Service.
 * <p>
 * This component registers service endpoints, health check URLs, and metadata during
 * startup and maintains registration through periodic heartbeats. It also handles
 * graceful deregistration during service shutdown.
 */
@Component
public class ServiceRegistration implements ApplicationListener<ApplicationReadyEvent> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistration.class);

    private final Environment environment;
    private final ServiceDiscoveryClient discoveryClient;
    private final String serviceId;

    @Value("${spring.application.name:notification-service}")
    private String serviceName;

    @Value("${server.port:8080}")
    private int serverPort;

    @Value("${management.server.port:${server.port}}")
    private int managementPort;

    @Value("${service.registration.enabled:true}")
    private boolean registrationEnabled;

    @Value("${service.registration.type:auto}")
    private String registrationType;

    @Value("${service.registration.tags:notification,traccar}")
    private String serviceTags;

    @Value("${service.registration.healthCheckPath:/actuator/health}")
    private String healthCheckPath;

    @Value("${service.registration.healthCheckInterval:10s}")
    private String healthCheckInterval;

    private boolean registered = false;

    /**
     * Creates a new ServiceRegistration instance.
     *
     * @param environment     The Spring environment for accessing configuration properties
     * @param discoveryClient The service discovery client implementation
     */
    @Autowired
    public ServiceRegistration(Environment environment, ServiceDiscoveryClient discoveryClient) {
        this.environment = environment;
        this.discoveryClient = discoveryClient;
        this.serviceId = generateServiceId();
        LOGGER.info("Initialized service registration with ID: {}", serviceId);
    }

    /**
     * Generates a unique service ID for this instance.
     *
     * @return A unique service ID string
     */
    private String generateServiceId() {
        String hostName;
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.warn("Unable to determine hostname, using random UUID", e);
            hostName = "unknown-host";
        }
        return serviceName + "-" + hostName + "-" + UUID.randomUUID().toString().substring(0, 8);
    }

    /**
     * Handles the application ready event to register the service.
     *
     * @param event The application ready event
     */
    @Override
    public void onApplicationEvent(ApplicationReadyEvent event) {
        if (registrationEnabled) {
            registerService();
        } else {
            LOGGER.info("Service registration is disabled");
        }
    }

    /**
     * Registers the service with the service discovery system.
     */
    public void registerService() {
        if (!registrationEnabled || registered) {
            return;
        }

        try {
            String host = determineServiceHost();
            Map<String, String> metadata = buildServiceMetadata();
            String[] tags = serviceTags.split(",");

            LOGGER.info("Registering service: {} (ID: {}) at {}:{} with tags: {}",
                    serviceName, serviceId, host, serverPort, serviceTags);

            discoveryClient.register(serviceId, serviceName, host, serverPort, healthCheckPath,
                    healthCheckInterval, tags, metadata);

            registered = true;
            LOGGER.info("Service registered successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to register service", e);
        }
    }

    /**
     * Determines the host address for service registration.
     *
     * @return The host address as a string
     */
    private String determineServiceHost() {
        String host = environment.getProperty("service.registration.host");
        if (host != null && !host.isEmpty()) {
            return host;
        }

        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            LOGGER.warn("Unable to determine host address, using localhost", e);
            return "localhost";
        }
    }

    /**
     * Builds the service metadata map for registration.
     *
     * @return A map of service metadata
     */
    private Map<String, String> buildServiceMetadata() {
        Map<String, String> metadata = new HashMap<>();
        metadata.put("version", environment.getProperty("info.app.version", "unknown"));
        metadata.put("environment", environment.getActiveProfiles().length > 0 ?
                environment.getActiveProfiles()[0] : "default");
        metadata.put("managementPort", String.valueOf(managementPort));
        metadata.put("healthCheckPath", healthCheckPath);

        // Add any additional metadata from configuration
        String additionalMetadata = environment.getProperty("service.registration.metadata", "");
        if (!additionalMetadata.isEmpty()) {
            for (String item : additionalMetadata.split(",")) {
                String[] parts = item.split("=");
                if (parts.length == 2) {
                    metadata.put(parts[0].trim(), parts[1].trim());
                }
            }
        }

        return metadata;
    }

    /**
     * Periodically renews the service registration to maintain availability.
     */
    @Scheduled(fixedDelayString = "${service.registration.renewalInterval:30000}")
    public void renewRegistration() {
        if (registrationEnabled && registered) {
            try {
                LOGGER.debug("Renewing service registration for ID: {}", serviceId);
                discoveryClient.renew(serviceId);
            } catch (Exception e) {
                LOGGER.warn("Failed to renew service registration, attempting to re-register", e);
                registered = false;
                registerService();
            }
        }
    }

    /**
     * Handles application shutdown event to deregister the service.
     *
     * @param event The context closed event
     */
    @EventListener
    public void onApplicationEvent(ContextClosedEvent event) {
        deregisterService();
    }

    /**
     * Deregisters the service before shutdown.
     */
    @PreDestroy
    public void deregisterService() {
        if (registrationEnabled && registered) {
            try {
                LOGGER.info("Deregistering service: {} (ID: {})", serviceName, serviceId);
                discoveryClient.deregister(serviceId);
                registered = false;
                LOGGER.info("Service deregistered successfully");
            } catch (Exception e) {
                LOGGER.error("Failed to deregister service", e);
            }
        }
    }

    /**
     * Interface for service discovery client implementations.
     * This allows for different implementations based on the environment (Consul, Kubernetes, etc.).
     */
    public interface ServiceDiscoveryClient {

        /**
         * Registers a service with the discovery system.
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
        void register(String serviceId, String serviceName, String host, int port,
                      String healthCheckPath, String healthCheckInterval,
                      String[] tags, Map<String, String> metadata);

        /**
         * Renews the service registration.
         *
         * @param serviceId The unique ID for this service instance
         */
        void renew(String serviceId);

        /**
         * Deregisters a service from the discovery system.
         *
         * @param serviceId The unique ID for this service instance
         */
        void deregister(String serviceId);
    }
}