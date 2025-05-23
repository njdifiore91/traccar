package org.traccar;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Configuration class for the Protocol Service that loads and manages service-specific settings.
 * Handles configuration for protocol servers, message broker integration, service discovery,
 * and health checks. Replaces the monolithic configuration approach with a focused configuration
 * specific to the Protocol Service's needs in the microservices architecture.
 */
@Singleton
public class ProtocolServiceConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProtocolServiceConfig.class);

    private final Map<String, String> properties = new ConcurrentHashMap<>();

    /**
     * Default constructor that loads configuration from various sources with the following precedence:
     * 1. Environment variables
     * 2. System properties
     * 3. Configuration file specified by the "config.file" system property
     * 4. Default configuration file at "./conf/traccar.xml"
     */
    @Inject
    public ProtocolServiceConfig() {
        // Load system properties first
        System.getProperties().forEach((key, value) -> {
            if (key instanceof String && value instanceof String) {
                properties.put((String) key, (String) value);
            }
        });
        
        // Then load from configuration file
        String configFile = System.getProperty("config.file", "./conf/traccar.xml");
        loadFile(configFile);
        
        // Finally load environment variables (highest priority)
        loadEnvironmentVariables();
        
        // Log the final configuration
        logConfiguration();
    }

    /**
     * Loads configuration from the specified file.
     *
     * @param file Path to the configuration file
     */
    private void loadFile(String file) {
        try {
            Path filePath = Paths.get(file);
            if (Files.exists(filePath)) {
                LOGGER.info("Loading configuration from file: {}", file);
                try (InputStream inputStream = new FileInputStream(file)) {
                    Properties fileProperties = new Properties();
                    fileProperties.loadFromXML(inputStream);
                    fileProperties.forEach((key, value) -> properties.put(key.toString(), value.toString()));
                }
            } else {
                LOGGER.info("Configuration file not found: {}", file);
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to load configuration file: {}", file, e);
        }
    }

    /**
     * Loads configuration from environment variables.
     * Environment variables are converted to property keys by:
     * 1. Converting to lowercase
     * 2. Replacing underscores with dots
     * For example, PROTOCOL_PORT_OSMAND=5055 becomes protocol.port.osmand=5055
     * 
     * Additionally, special handling is provided for common Kubernetes environment variables:
     * - KUBERNETES_SERVICE_HOST and KUBERNETES_SERVICE_PORT are used to configure service discovery
     * - POD_NAME and POD_NAMESPACE are used for service instance identification
     */
    private void loadEnvironmentVariables() {
        LOGGER.info("Loading configuration from environment variables");
        System.getenv().forEach((key, value) -> {
            // Convert environment variable name to property key
            String propertyKey = key.toLowerCase().replace('_', '.');
            properties.put(propertyKey, value);
            
            // Special handling for Kubernetes environment variables
            if (key.equals("KUBERNETES_SERVICE_HOST") && !properties.containsKey("service.discovery.type")) {
                properties.put("service.discovery.type", "kubernetes");
            }
            
            if (key.equals("POD_NAMESPACE") && !properties.containsKey("service.discovery.namespace")) {
                properties.put("service.discovery.namespace", value);
            }
            
            if (key.equals("POD_NAME") && !properties.containsKey("service.instance.id")) {
                properties.put("service.instance.id", getServiceName() + "-" + value);
            }
        });
    }

    /**
     * Logs the current configuration (excluding sensitive information).
     */
    private void logConfiguration() {
        LOGGER.info("Protocol Service Configuration:");
        properties.entrySet().stream()
                .filter(entry -> !entry.getKey().contains("password") && !entry.getKey().contains("secret"))
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> LOGGER.info("{} = {}", entry.getKey(), entry.getValue()));
    }

    /**
     * Gets a property as a string.
     *
     * @param key Property key
     * @return Property value or null if not found
     */
    public String getString(String key) {
        return properties.get(key);
    }

    /**
     * Gets a property as a string with a default value.
     *
     * @param key Property key
     * @param defaultValue Default value to return if the property is not found
     * @return Property value or the default value if not found
     */
    public String getString(String key, String defaultValue) {
        return properties.getOrDefault(key, defaultValue);
    }

    /**
     * Gets a property as an integer.
     *
     * @param key Property key
     * @return Property value as an integer or null if not found or not a valid integer
     */
    public Integer getInteger(String key) {
        String value = properties.get(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid integer value for key: {}", key);
            }
        }
        return null;
    }

    /**
     * Gets a property as an integer with a default value.
     *
     * @param key Property key
     * @param defaultValue Default value to return if the property is not found or not a valid integer
     * @return Property value as an integer or the default value if not found or not a valid integer
     */
    public int getInteger(String key, int defaultValue) {
        Integer value = getInteger(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Gets a property as a boolean.
     *
     * @param key Property key
     * @return Property value as a boolean or null if not found
     */
    public Boolean getBoolean(String key) {
        String value = properties.get(key);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return null;
    }

    /**
     * Gets a property as a boolean with a default value.
     *
     * @param key Property key
     * @param defaultValue Default value to return if the property is not found
     * @return Property value as a boolean or the default value if not found
     */
    public boolean getBoolean(String key, boolean defaultValue) {
        Boolean value = getBoolean(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Gets a property as a double.
     *
     * @param key Property key
     * @return Property value as a double or null if not found or not a valid double
     */
    public Double getDouble(String key) {
        String value = properties.get(key);
        if (value != null) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid double value for key: {}", key);
            }
        }
        return null;
    }

    /**
     * Gets a property as a double with a default value.
     *
     * @param key Property key
     * @param defaultValue Default value to return if the property is not found or not a valid double
     * @return Property value as a double or the default value if not found or not a valid double
     */
    public double getDouble(String key, double defaultValue) {
        Double value = getDouble(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Gets the message broker type (kafka or rabbitmq).
     *
     * @return Message broker type (defaults to "kafka" if not specified)
     */
    public String getMessageBrokerType() {
        return getString("message.broker.type", "kafka").toLowerCase();
    }

    /**
     * Gets the message broker connection string.
     *
     * @return Message broker connection string
     */
    public String getMessageBrokerConnection() {
        return getString("message.broker.connection", "localhost:9092");
    }
    
    /**
     * Gets additional message broker properties as a map.
     * Properties are expected to be prefixed with "message.broker.property."
     * For example, "message.broker.property.acks=all" would be returned as {"acks": "all"}
     *
     * @return Map of additional message broker properties
     */
    public Map<String, String> getMessageBrokerProperties() {
        Map<String, String> result = new ConcurrentHashMap<>();
        String prefix = "message.broker.property.";
        properties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(prefix))
                .forEach(entry -> {
                    String key = entry.getKey().substring(prefix.length());
                    result.put(key, entry.getValue());
                });
        return result;
    }

    /**
     * Gets the message broker topic for publishing position data.
     *
     * @return Message broker topic for positions
     */
    public String getPositionTopic() {
        return getString("message.topic.position", "raw-positions");
    }
    
    /**
     * Gets the message broker producer batch size.
     * This is the number of messages to batch before sending to the broker.
     *
     * @return Message broker producer batch size
     */
    public int getMessageProducerBatchSize() {
        return getInteger("message.producer.batch.size", 16384);
    }
    
    /**
     * Gets the message broker producer linger time in milliseconds.
     * This is the time to wait for more messages before sending a batch.
     *
     * @return Message broker producer linger time in milliseconds
     */
    public int getMessageProducerLingerMs() {
        return getInteger("message.producer.linger.ms", 5);
    }
    
    /**
     * Gets the message broker producer buffer memory in bytes.
     * This is the total memory the producer can use for buffering messages.
     *
     * @return Message broker producer buffer memory in bytes
     */
    public int getMessageProducerBufferMemory() {
        return getInteger("message.producer.buffer.memory", 33554432); // 32MB
    }

    /**
     * Gets the message broker topic for publishing command responses.
     *
     * @return Message broker topic for command responses
     */
    public String getCommandResponseTopic() {
        return getString("message.topic.command.response", "command-responses");
    }

    /**
     * Gets the message broker topic for receiving device commands.
     *
     * @return Message broker topic for device commands
     */
    public String getCommandTopic() {
        return getString("message.topic.command", "device-commands");
    }

    /**
     * Gets the service discovery type (consul or kubernetes).
     *
     * @return Service discovery type (defaults to "kubernetes" if not specified)
     */
    public String getServiceDiscoveryType() {
        return getString("service.discovery.type", "kubernetes").toLowerCase();
    }

    /**
     * Gets the service discovery connection string.
     *
     * @return Service discovery connection string
     */
    public String getServiceDiscoveryConnection() {
        return getString("service.discovery.connection", "localhost:8500");
    }
    
    /**
     * Gets the service discovery namespace/datacenter.
     *
     * @return Service discovery namespace/datacenter
     */
    public String getServiceDiscoveryNamespace() {
        return getString("service.discovery.namespace", "default");
    }
    
    /**
     * Gets the service discovery registration interval in seconds.
     *
     * @return Service discovery registration interval in seconds
     */
    public int getServiceDiscoveryRegistrationInterval() {
        return getInteger("service.discovery.registration.interval", 30);
    }
    
    /**
     * Gets the service discovery health check interval in seconds.
     *
     * @return Service discovery health check interval in seconds
     */
    public int getServiceDiscoveryHealthCheckInterval() {
        return getInteger("service.discovery.health.interval", 10);
    }
    
    /**
     * Gets the service discovery health check timeout in seconds.
     *
     * @return Service discovery health check timeout in seconds
     */
    public int getServiceDiscoveryHealthCheckTimeout() {
        return getInteger("service.discovery.health.timeout", 5);
    }

    /**
     * Gets the service name for registration with service discovery.
     *
     * @return Service name
     */
    public String getServiceName() {
        return getString("service.name", "protocol-service");
    }
    
    /**
     * Gets the service version for registration with service discovery.
     *
     * @return Service version
     */
    public String getServiceVersion() {
        return getString("service.version", "1.0.0");
    }
    
    /**
     * Gets the service tags for registration with service discovery.
     *
     * @return Service tags as a list
     */
    public List<String> getServiceTags() {
        String tagsStr = getString("service.tags", "protocol,traccar");
        return Arrays.asList(tagsStr.split(","));
    }

    /**
     * Gets the service instance ID for registration with service discovery.
     * Defaults to service name + hostname + random UUID if not specified.
     *
     * @return Service instance ID
     */
    public String getServiceInstanceId() {
        if (properties.containsKey("service.instance.id")) {
            return getString("service.instance.id");
        }
        try {
            String hostname = java.net.InetAddress.getLocalHost().getHostName();
            return getServiceName() + "-" + hostname + "-" + java.util.UUID.randomUUID().toString().substring(0, 8);
        } catch (Exception e) {
            return getServiceName() + "-" + java.util.UUID.randomUUID().toString();
        }
    }

    /**
     * Gets the health check port.
     *
     * @return Health check port
     */
    public int getHealthPort() {
        return getInteger("health.port", 8080);
    }

    /**
     * Gets the health check path.
     *
     * @return Health check path
     */
    public String getHealthPath() {
        return getString("health.path", "/health");
    }
    
    /**
     * Gets the liveness check path.
     *
     * @return Liveness check path
     */
    public String getLivenessPath() {
        return getString("health.liveness.path", "/health/liveness");
    }
    
    /**
     * Gets the readiness check path.
     *
     * @return Readiness check path
     */
    public String getReadinessPath() {
        return getString("health.readiness.path", "/health/readiness");
    }

    /**
     * Gets the metrics port.
     *
     * @return Metrics port
     */
    public int getMetricsPort() {
        return getInteger("metrics.port", 8080);
    }

    /**
     * Gets the metrics path.
     *
     * @return Metrics path
     */
    public String getMetricsPath() {
        return getString("metrics.path", "/metrics");
    }
    
    /**
     * Checks if OpenTelemetry tracing is enabled.
     *
     * @return True if OpenTelemetry tracing is enabled, false otherwise
     */
    public boolean isTracingEnabled() {
        return getBoolean("tracing.enable", true);
    }
    
    /**
     * Gets the OpenTelemetry exporter type (jaeger, zipkin, otlp).
     *
     * @return OpenTelemetry exporter type
     */
    public String getTracingExporter() {
        return getString("tracing.exporter", "jaeger").toLowerCase();
    }
    
    /**
     * Gets the OpenTelemetry exporter endpoint.
     *
     * @return OpenTelemetry exporter endpoint
     */
    public String getTracingEndpoint() {
        return getString("tracing.endpoint", "http://localhost:14250");
    }

    /**
     * Gets the protocols directory path.
     *
     * @return Protocols directory path
     */
    public String getProtocolsPath() {
        return getString("protocols.path", "./protocols");
    }
    
    /**
     * Gets a list of enabled protocols.
     * If specific protocols are enabled/disabled via configuration, returns only the enabled ones.
     * Otherwise, returns all available protocols.
     *
     * @return List of enabled protocol names
     */
    public List<String> getEnabledProtocols() {
        String enabledProtocolsStr = getString("protocols.enable");
        if (enabledProtocolsStr != null && !enabledProtocolsStr.isEmpty()) {
            return Arrays.asList(enabledProtocolsStr.split(","));
        }
        
        // If no specific protocols are enabled, check for disabled protocols
        String disabledProtocolsStr = getString("protocols.disable");
        if (disabledProtocolsStr != null && !disabledProtocolsStr.isEmpty()) {
            List<String> disabledProtocols = Arrays.asList(disabledProtocolsStr.split(","));
            return getAllProtocols().stream()
                    .filter(protocol -> !disabledProtocols.contains(protocol))
                    .toList();
        }
        
        // If neither enabled nor disabled protocols are specified, return all protocols
        return getAllProtocols();
    }
    
    /**
     * Gets a list of all available protocols.
     * This is a placeholder method that should be implemented to discover available protocols.
     *
     * @return List of all available protocol names
     */
    private List<String> getAllProtocols() {
        // This would typically scan the classpath or a directory for protocol implementations
        // For now, we return a default list of common protocols
        return Arrays.asList(
                "osmand", "teltonika", "meitrack", "suntech", "h02", "tk103", "gl200", "totem",
                "xexun", "atrack", "skypatrol", "gt06", "cartrack", "minifinder", "haicom", "eelink",
                "meiligao", "coban", "fifotrack", "queclink", "huabao", "v680", "pt502", "tr20",
                "navis", "meitrack", "megastek", "navigil", "gpsgate", "enfora", "globalsat", "intellitrac"
        );
    }

    /**
     * Checks if a specific protocol is enabled.
     *
     * @param protocol Protocol name
     * @return True if the protocol is enabled, false otherwise
     */
    public boolean isProtocolEnabled(String protocol) {
        String key = "protocol." + protocol + ".enable";
        return getBoolean(key, true);
    }

    /**
     * Gets the port for a specific protocol.
     *
     * @param protocol Protocol name
     * @param defaultPort Default port to use if not specified
     * @return Protocol port
     */
    public int getProtocolPort(String protocol, int defaultPort) {
        String key = "protocol.port." + protocol;
        return getInteger(key, defaultPort);
    }

    /**
     * Gets the connection timeout in seconds.
     *
     * @return Connection timeout in seconds
     */
    public int getConnectionTimeout() {
        return getInteger("connection.timeout", 300);
    }
    
    /**
     * Gets the maximum number of connections per protocol server.
     * A value of 0 means unlimited.
     *
     * @return Maximum number of connections
     */
    public int getMaxConnections() {
        return getInteger("connection.max", 0);
    }
    
    /**
     * Gets the maximum number of connections per IP address.
     * A value of 0 means unlimited.
     *
     * @return Maximum number of connections per IP address
     */
    public int getMaxConnectionsPerIp() {
        return getInteger("connection.max.per.ip", 0);
    }
    
    /**
     * Gets the connection backlog size.
     * This is the maximum number of pending connections in the queue.
     *
     * @return Connection backlog size
     */
    public int getConnectionBacklog() {
        return getInteger("connection.backlog", 1000);
    }

    /**
     * Gets the number of worker threads for the protocol servers.
     * A value of 0 means use the default (number of available processors * 2).
     *
     * @return Number of worker threads
     */
    public int getWorkerThreads() {
        return getInteger("worker.threads", 0);
    }

    /**
     * Gets the number of boss threads for the protocol servers.
     * A value of 0 means use the default (1).
     *
     * @return Number of boss threads
     */
    public int getBossThreads() {
        return getInteger("boss.threads", 0);
    }

    /**
     * Checks if SSL is enabled for a specific protocol.
     *
     * @param protocol Protocol name
     * @return True if SSL is enabled for the protocol, false otherwise
     */
    public boolean isProtocolSslEnabled(String protocol) {
        String key = "protocol." + protocol + ".ssl.enable";
        return getBoolean(key, false);
    }

    /**
     * Gets the SSL certificate path for a specific protocol.
     *
     * @param protocol Protocol name
     * @return SSL certificate path or null if not specified
     */
    public String getProtocolSslCertificate(String protocol) {
        String key = "protocol." + protocol + ".ssl.certificate";
        return getString(key);
    }

    /**
     * Gets the SSL certificate password for a specific protocol.
     *
     * @param protocol Protocol name
     * @return SSL certificate password or null if not specified
     */
    public String getProtocolSslCertificatePassword(String protocol) {
        String key = "protocol." + protocol + ".ssl.certificate.password";
        return getString(key);
    }

    /**
     * Gets the SSL key store path for a specific protocol.
     *
     * @param protocol Protocol name
     * @return SSL key store path or null if not specified
     */
    public String getProtocolSslKeyStore(String protocol) {
        String key = "protocol." + protocol + ".ssl.keystore";
        return getString(key);
    }

    /**
     * Gets the SSL key store password for a specific protocol.
     *
     * @param protocol Protocol name
     * @return SSL key store password or null if not specified
     */
    public String getProtocolSslKeyStorePassword(String protocol) {
        String key = "protocol." + protocol + ".ssl.keystore.password";
        return getString(key);
    }
}