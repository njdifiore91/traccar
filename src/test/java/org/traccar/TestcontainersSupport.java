package org.traccar;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Provides comprehensive support for using Testcontainers in the Traccar microservices architecture.
 * This class offers utilities for setting up and managing containers for various infrastructure components
 * such as Kafka, RabbitMQ, PostgreSQL, MySQL, and service discovery tools.
 * 
 * It supports both local development testing and CI/CD pipeline integration with JUnit 5.
 */
public class TestcontainersSupport {

    private static final Network SHARED_NETWORK = Network.newNetwork();
    
    /**
     * Creates a Kafka container with default configuration.
     * 
     * @return configured KafkaContainer instance
     */
    public static KafkaContainer createKafkaContainer() {
        return new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"))
                .withNetwork(SHARED_NETWORK)
                .withNetworkAliases("kafka")
                .withExposedPorts(9092, 9093);
    }
    
    /**
     * Creates a Kafka container with custom topics.
     * 
     * @param topics list of topics to create on startup
     * @return configured KafkaContainer instance
     */
    public static KafkaContainer createKafkaContainer(List<String> topics) {
        KafkaContainer container = createKafkaContainer();
        container.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");
        
        // Create a comma-separated list of topics
        if (topics != null && !topics.isEmpty()) {
            String topicList = String.join(",", topics);
            container.withEnv("KAFKA_CREATE_TOPICS", topicList);
        }
        
        return container;
    }
    
    /**
     * Creates a RabbitMQ container with default configuration.
     * 
     * @return configured RabbitMQContainer instance
     */
    public static RabbitMQContainer createRabbitMQContainer() {
        return new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.12.12-management"))
                .withNetwork(SHARED_NETWORK)
                .withNetworkAliases("rabbitmq")
                .withExposedPorts(5672, 15672);
    }
    
    /**
     * Creates a RabbitMQ container with custom exchanges and queues.
     * 
     * @param exchanges list of exchanges to create on startup
     * @param queues list of queues to create on startup
     * @param bindings map of queue to exchange bindings with routing keys
     * @return configured RabbitMQContainer instance
     */
    public static RabbitMQContainer createRabbitMQContainer(
            List<String> exchanges, 
            List<String> queues, 
            Map<String, Map<String, String>> bindings) {
        
        RabbitMQContainer container = createRabbitMQContainer();
        
        // Add initialization script for exchanges and queues
        StringBuilder initScript = new StringBuilder();
        
        if (exchanges != null) {
            for (String exchange : exchanges) {
                initScript.append(String.format(
                        "rabbitmqadmin declare exchange name=%s type=topic durable=true\n", 
                        exchange));
            }
        }
        
        if (queues != null) {
            for (String queue : queues) {
                initScript.append(String.format(
                        "rabbitmqadmin declare queue name=%s durable=true\n", 
                        queue));
            }
        }
        
        if (bindings != null) {
            for (Map.Entry<String, Map<String, String>> binding : bindings.entrySet()) {
                String queue = binding.getKey();
                for (Map.Entry<String, String> exchangeRouting : binding.getValue().entrySet()) {
                    String exchange = exchangeRouting.getKey();
                    String routingKey = exchangeRouting.getValue();
                    initScript.append(String.format(
                            "rabbitmqadmin declare binding source=%s destination=%s routing_key=%s\n",
                            exchange, queue, routingKey));
                }
            }
        }
        
        if (initScript.length() > 0) {
            container.withCommand("/bin/bash", "-c", "rabbitmq-server & " +
                    "sleep 10 && " + initScript.toString() + " && tail -f /dev/null");
        }
        
        return container;
    }
    
    /**
     * Creates a PostgreSQL container with default configuration.
     * 
     * @return configured PostgreSQLContainer instance
     */
    public static PostgreSQLContainer<?> createPostgreSQLContainer() {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                .withNetwork(SHARED_NETWORK)
                .withNetworkAliases("postgres")
                .withDatabaseName("traccar")
                .withUsername("traccar")
                .withPassword("traccar")
                .withExposedPorts(5432);
    }
    
    /**
     * Creates a PostgreSQL container with custom configuration.
     * 
     * @param databaseName name of the database to create
     * @param username database username
     * @param password database password
     * @param initScriptPath path to initialization SQL script
     * @return configured PostgreSQLContainer instance
     */
    public static PostgreSQLContainer<?> createPostgreSQLContainer(
            String databaseName, 
            String username, 
            String password, 
            String initScriptPath) {
        
        PostgreSQLContainer<?> container = new PostgreSQLContainer<>(DockerImageName.parse("postgres:14-alpine"))
                .withNetwork(SHARED_NETWORK)
                .withNetworkAliases("postgres")
                .withDatabaseName(databaseName)
                .withUsername(username)
                .withPassword(password)
                .withExposedPorts(5432);
        
        if (initScriptPath != null && !initScriptPath.isEmpty()) {
            container.withInitScript(initScriptPath);
        }
        
        return container;
    }
    
    /**
     * Creates a MySQL container with default configuration.
     * 
     * @return configured MySQLContainer instance
     */
    public static MySQLContainer<?> createMySQLContainer() {
        return new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                .withNetwork(SHARED_NETWORK)
                .withNetworkAliases("mysql")
                .withDatabaseName("traccar")
                .withUsername("traccar")
                .withPassword("traccar")
                .withExposedPorts(3306);
    }
    
    /**
     * Creates a MySQL container with custom configuration.
     * 
     * @param databaseName name of the database to create
     * @param username database username
     * @param password database password
     * @param initScriptPath path to initialization SQL script
     * @return configured MySQLContainer instance
     */
    public static MySQLContainer<?> createMySQLContainer(
            String databaseName, 
            String username, 
            String password, 
            String initScriptPath) {
        
        MySQLContainer<?> container = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
                .withNetwork(SHARED_NETWORK)
                .withNetworkAliases("mysql")
                .withDatabaseName(databaseName)
                .withUsername(username)
                .withPassword(password)
                .withExposedPorts(3306);
        
        if (initScriptPath != null && !initScriptPath.isEmpty()) {
            container.withInitScript(initScriptPath);
        }
        
        return container;
    }
    
    /**
     * Creates a Consul container for service discovery.
     * 
     * @return configured GenericContainer instance for Consul
     */
    public static GenericContainer<?> createConsulContainer() {
        return new GenericContainer<>(DockerImageName.parse("consul:1.17.0"))
                .withNetwork(SHARED_NETWORK)
                .withNetworkAliases("consul")
                .withExposedPorts(8500, 8600)
                .withEnv("CONSUL_BIND_INTERFACE", "eth0")
                .withCommand("agent", "-dev", "-client", "0.0.0.0", "-ui")
                .waitingFor(Wait.forHttp("/v1/status/leader").forStatusCode(200));
    }
    
    /**
     * Creates a Kubernetes mock container for service discovery testing.
     * 
     * @return configured GenericContainer instance for Kubernetes mock
     */
    public static GenericContainer<?> createKubernetesMockContainer() {
        return new GenericContainer<>(DockerImageName.parse("bitnami/kubectl:1.28"))
                .withNetwork(SHARED_NETWORK)
                .withNetworkAliases("kubernetes")
                .withCommand("proxy", "--port=8080")
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/").forStatusCode(200));
    }
    
    /**
     * Creates a container for the specified image with default configuration.
     * 
     * @param imageName Docker image name
     * @param networkAlias network alias for the container
     * @param exposedPorts ports to expose
     * @return configured GenericContainer instance
     */
    public static GenericContainer<?> createGenericContainer(
            String imageName, 
            String networkAlias, 
            int... exposedPorts) {
        
        GenericContainer<?> container = new GenericContainer<>(DockerImageName.parse(imageName))
                .withNetwork(SHARED_NETWORK)
                .withExposedPorts(exposedPorts);
        
        if (networkAlias != null && !networkAlias.isEmpty()) {
            container.withNetworkAliases(networkAlias);
        }
        
        return container;
    }
    
    /**
     * Creates a container for the specified image with custom environment variables.
     * 
     * @param imageName Docker image name
     * @param networkAlias network alias for the container
     * @param env environment variables
     * @param exposedPorts ports to expose
     * @return configured GenericContainer instance
     */
    public static GenericContainer<?> createGenericContainer(
            String imageName, 
            String networkAlias, 
            Map<String, String> env, 
            int... exposedPorts) {
        
        GenericContainer<?> container = createGenericContainer(imageName, networkAlias, exposedPorts);
        
        if (env != null && !env.isEmpty()) {
            container.withEnv(env);
        }
        
        return container;
    }
    
    /**
     * Creates a new network for container communication.
     * 
     * @return Network instance
     */
    public static Network createNetwork() {
        return Network.newNetwork();
    }
    
    /**
     * Gets the shared network for container communication.
     * 
     * @return shared Network instance
     */
    public static Network getSharedNetwork() {
        return SHARED_NETWORK;
    }
    
    /**
     * JUnit 5 extension for managing container lifecycle.
     * This extension starts containers before all tests and stops them after all tests.
     */
    public static class ContainerExtension implements BeforeAllCallback, AfterAllCallback {
        
        private final List<GenericContainer<?>> containers = new ArrayList<>();
        
        /**
         * Adds a container to be managed by this extension.
         * 
         * @param container container to manage
         * @return this extension instance for method chaining
         */
        public ContainerExtension withContainer(GenericContainer<?> container) {
            containers.add(container);
            return this;
        }
        
        @Override
        public void beforeAll(ExtensionContext context) {
            for (GenericContainer<?> container : containers) {
                if (!container.isRunning()) {
                    container.start();
                }
            }
        }
        
        @Override
        public void afterAll(ExtensionContext context) {
            for (GenericContainer<?> container : containers) {
                if (container.isRunning()) {
                    container.stop();
                }
            }
        }
    }
    
    /**
     * Builder for creating a Kafka configuration with multiple topics.
     */
    public static class KafkaConfigBuilder {
        private final List<String> topics = new ArrayList<>();
        
        /**
         * Adds a topic to the Kafka configuration.
         * 
         * @param topic topic name
         * @return this builder instance for method chaining
         */
        public KafkaConfigBuilder withTopic(String topic) {
            topics.add(topic);
            return this;
        }
        
        /**
         * Adds multiple topics to the Kafka configuration.
         * 
         * @param topics topic names
         * @return this builder instance for method chaining
         */
        public KafkaConfigBuilder withTopics(String... topics) {
            for (String topic : topics) {
                this.topics.add(topic);
            }
            return this;
        }
        
        /**
         * Creates a Kafka container with the configured topics.
         * 
         * @return configured KafkaContainer instance
         */
        public KafkaContainer build() {
            return createKafkaContainer(topics);
        }
    }
    
    /**
     * Builder for creating a RabbitMQ configuration with exchanges, queues, and bindings.
     */
    public static class RabbitMQConfigBuilder {
        private final List<String> exchanges = new ArrayList<>();
        private final List<String> queues = new ArrayList<>();
        private final Map<String, Map<String, String>> bindings = new HashMap<>();
        
        /**
         * Adds an exchange to the RabbitMQ configuration.
         * 
         * @param exchange exchange name
         * @return this builder instance for method chaining
         */
        public RabbitMQConfigBuilder withExchange(String exchange) {
            exchanges.add(exchange);
            return this;
        }
        
        /**
         * Adds a queue to the RabbitMQ configuration.
         * 
         * @param queue queue name
         * @return this builder instance for method chaining
         */
        public RabbitMQConfigBuilder withQueue(String queue) {
            queues.add(queue);
            return this;
        }
        
        /**
         * Adds a binding between an exchange and a queue with a routing key.
         * 
         * @param queue queue name
         * @param exchange exchange name
         * @param routingKey routing key
         * @return this builder instance for method chaining
         */
        public RabbitMQConfigBuilder withBinding(String queue, String exchange, String routingKey) {
            if (!exchanges.contains(exchange)) {
                exchanges.add(exchange);
            }
            
            if (!queues.contains(queue)) {
                queues.add(queue);
            }
            
            Map<String, String> exchangeBindings = bindings.computeIfAbsent(queue, k -> new HashMap<>());
            exchangeBindings.put(exchange, routingKey);
            
            return this;
        }
        
        /**
         * Creates a RabbitMQ container with the configured exchanges, queues, and bindings.
         * 
         * @return configured RabbitMQContainer instance
         */
        public RabbitMQContainer build() {
            return createRabbitMQContainer(exchanges, queues, bindings);
        }
    }
    
    /**
     * Builder for creating a database configuration.
     * 
     * @param <T> database container type
     */
    public static class DatabaseConfigBuilder<T> {
        private String databaseName = "traccar";
        private String username = "traccar";
        private String password = "traccar";
        private String initScriptPath;
        private final boolean isPostgres;
        
        /**
         * Creates a new database configuration builder.
         * 
         * @param isPostgres true for PostgreSQL, false for MySQL
         */
        public DatabaseConfigBuilder(boolean isPostgres) {
            this.isPostgres = isPostgres;
        }
        
        /**
         * Sets the database name.
         * 
         * @param databaseName database name
         * @return this builder instance for method chaining
         */
        public DatabaseConfigBuilder<T> withDatabaseName(String databaseName) {
            this.databaseName = databaseName;
            return this;
        }
        
        /**
         * Sets the database username.
         * 
         * @param username database username
         * @return this builder instance for method chaining
         */
        public DatabaseConfigBuilder<T> withUsername(String username) {
            this.username = username;
            return this;
        }
        
        /**
         * Sets the database password.
         * 
         * @param password database password
         * @return this builder instance for method chaining
         */
        public DatabaseConfigBuilder<T> withPassword(String password) {
            this.password = password;
            return this;
        }
        
        /**
         * Sets the initialization script path.
         * 
         * @param initScriptPath path to initialization SQL script
         * @return this builder instance for method chaining
         */
        public DatabaseConfigBuilder<T> withInitScript(String initScriptPath) {
            this.initScriptPath = initScriptPath;
            return this;
        }
        
        /**
         * Creates a database container with the configured settings.
         * 
         * @return configured database container instance
         */
        @SuppressWarnings("unchecked")
        public T build() {
            if (isPostgres) {
                return (T) createPostgreSQLContainer(databaseName, username, password, initScriptPath);
            } else {
                return (T) createMySQLContainer(databaseName, username, password, initScriptPath);
            }
        }
    }
    
    /**
     * Creates a builder for PostgreSQL configuration.
     * 
     * @return PostgreSQL configuration builder
     */
    public static DatabaseConfigBuilder<PostgreSQLContainer<?>> postgresBuilder() {
        return new DatabaseConfigBuilder<>(true);
    }
    
    /**
     * Creates a builder for MySQL configuration.
     * 
     * @return MySQL configuration builder
     */
    public static DatabaseConfigBuilder<MySQLContainer<?>> mysqlBuilder() {
        return new DatabaseConfigBuilder<>(false);
    }
    
    /**
     * Creates a builder for Kafka configuration.
     * 
     * @return Kafka configuration builder
     */
    public static KafkaConfigBuilder kafkaBuilder() {
        return new KafkaConfigBuilder();
    }
    
    /**
     * Creates a builder for RabbitMQ configuration.
     * 
     * @return RabbitMQ configuration builder
     */
    public static RabbitMQConfigBuilder rabbitmqBuilder() {
        return new RabbitMQConfigBuilder();
    }
}