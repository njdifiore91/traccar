import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import org.apache.qpid.server.Broker;
import org.apache.qpid.server.BrokerOptions;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer as JavaConsumer;

/**
 * Utility class for testing message broker integration in microservices architecture.
 * Provides support for both Kafka and RabbitMQ testing.
 */
public class MessageBrokerTestUtils {

    private static final int DEFAULT_TIMEOUT_SECONDS = 10;
    
    /**
     * Creates a Kafka producer for testing with default serializers.
     *
     * @param broker The embedded Kafka broker
     * @return A configured Kafka producer
     */
    public static Producer<String, String> createKafkaProducer(EmbeddedKafkaBroker broker) {
        return createKafkaProducer(broker, new StringSerializer(), new StringSerializer());
    }
    
    /**
     * Creates a Kafka producer for testing with custom serializers.
     *
     * @param broker The embedded Kafka broker
     * @param keySerializer The key serializer
     * @param valueSerializer The value serializer
     * @param <K> The key type
     * @param <V> The value type
     * @return A configured Kafka producer
     */
    public static <K, V> Producer<K, V> createKafkaProducer(
            EmbeddedKafkaBroker broker,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer) {
        
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        // Add additional producer configuration
        producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
        producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        producerProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        
        DefaultKafkaProducerFactory<K, V> producerFactory = 
                new DefaultKafkaProducerFactory<>(producerProps, keySerializer, valueSerializer);
        return producerFactory.createProducer();
    }
    
    /**
     * Creates a Kafka consumer for testing with default deserializers.
     *
     * @param broker The embedded Kafka broker
     * @param group The consumer group ID
     * @return A configured Kafka consumer
     */
    public static Consumer<String, String> createKafkaConsumer(EmbeddedKafkaBroker broker, String group) {
        return createKafkaConsumer(broker, group, new StringDeserializer(), new StringDeserializer());
    }
    
    /**
     * Creates a Kafka consumer for testing with custom deserializers.
     *
     * @param broker The embedded Kafka broker
     * @param group The consumer group ID
     * @param keyDeserializer The key deserializer
     * @param valueDeserializer The value deserializer
     * @param <K> The key type
     * @param <V> The value type
     * @return A configured Kafka consumer
     */
    public static <K, V> Consumer<K, V> createKafkaConsumer(
            EmbeddedKafkaBroker broker,
            String group,
            Deserializer<K> keyDeserializer,
            Deserializer<V> valueDeserializer) {
        
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(group, "true", broker);
        // Add additional consumer configuration
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        
        DefaultKafkaConsumerFactory<K, V> consumerFactory = 
                new DefaultKafkaConsumerFactory<>(consumerProps, keyDeserializer, valueDeserializer);
        return consumerFactory.createConsumer();
    }
    
    /**
     * Creates a KafkaTemplate for testing with default serializers.
     *
     * @param broker The embedded Kafka broker
     * @return A configured KafkaTemplate
     */
    public static KafkaTemplate<String, String> createKafkaTemplate(EmbeddedKafkaBroker broker) {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker);
        DefaultKafkaProducerFactory<String, String> producerFactory = 
                new DefaultKafkaProducerFactory<>(producerProps);
        return new KafkaTemplate<>(producerFactory);
    }
    
    /**
     * Sends a message to a Kafka topic and waits for it to be consumed.
     *
     * @param producer The Kafka producer
     * @param topic The topic to send to
     * @param key The message key
     * @param value The message value
     * @param <K> The key type
     * @param <V> The value type
     * @return The producer record that was sent
     */
    public static <K, V> ProducerRecord<K, V> sendKafkaMessage(
            Producer<K, V> producer, 
            String topic, 
            K key, 
            V value) {
        
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
        producer.flush();
        return record;
    }
    
    /**
     * Waits for a message to be received on a Kafka topic.
     *
     * @param consumer The Kafka consumer
     * @param topic The topic to consume from
     * @param timeoutSeconds The timeout in seconds
     * @param <K> The key type
     * @param <V> The value type
     * @return The consumed record, or null if no record was received within the timeout
     */
    public static <K, V> ConsumerRecord<K, V> waitForKafkaMessage(
            Consumer<K, V> consumer, 
            String topic, 
            int timeoutSeconds) {
        
        consumer.subscribe(Collections.singletonList(topic));
        ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(timeoutSeconds));
        if (records.isEmpty()) {
            return null;
        }
        return records.iterator().next();
    }
    
    /**
     * Verifies that a message is received on a Kafka topic with the expected key and value.
     *
     * @param consumer The Kafka consumer
     * @param topic The topic to consume from
     * @param expectedKey The expected key
     * @param expectedValue The expected value
     * @param timeoutSeconds The timeout in seconds
     * @param <K> The key type
     * @param <V> The value type
     * @return true if a matching message was received, false otherwise
     */
    public static <K, V> boolean verifyKafkaMessage(
            Consumer<K, V> consumer, 
            String topic, 
            K expectedKey, 
            V expectedValue, 
            int timeoutSeconds) {
        
        ConsumerRecord<K, V> record = waitForKafkaMessage(consumer, topic, timeoutSeconds);
        if (record == null) {
            return false;
        }
        
        boolean keyMatches = (expectedKey == null && record.key() == null) || 
                (expectedKey != null && expectedKey.equals(record.key()));
        boolean valueMatches = (expectedValue == null && record.value() == null) || 
                (expectedValue != null && expectedValue.equals(record.value()));
        
        return keyMatches && valueMatches;
    }
    
    /**
     * Starts an embedded Qpid broker for RabbitMQ testing.
     *
     * @param port The port to use for the broker
     * @return The started broker instance
     * @throws Exception If the broker fails to start
     */
    public static Broker startEmbeddedQpidBroker(int port) throws Exception {
        Broker broker = new Broker();
        BrokerOptions brokerOptions = new BrokerOptions();
        
        // Create a temporary work directory
        File workDir = Files.createTempDirectory("qpid-work").toFile();
        workDir.deleteOnExit();
        
        // Configure the broker
        brokerOptions.setConfigProperty("qpid.amqp_port", String.valueOf(port));
        brokerOptions.setConfigProperty("qpid.work_dir", workDir.getAbsolutePath());
        
        // Use a config file from the classpath
        String configFileName = "qpid-config.json";
        brokerOptions.setInitialConfigurationLocation(configFileName);
        
        // Start the broker
        broker.startup(brokerOptions);
        
        return broker;
    }
    
    /**
     * Creates a RabbitTemplate for testing with the embedded Qpid broker.
     *
     * @param port The port of the embedded broker
     * @return A configured RabbitTemplate
     */
    public static RabbitTemplate createRabbitTemplate(int port) {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory("localhost", port);
        connectionFactory.setUsername("guest");
        connectionFactory.setPassword("guest");
        
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(new Jackson2JsonMessageConverter());
        return template;
    }
    
    /**
     * Creates a RabbitAdmin for managing RabbitMQ resources in tests.
     *
     * @param template The RabbitTemplate to use
     * @return A configured RabbitAdmin
     */
    public static RabbitAdmin createRabbitAdmin(RabbitTemplate template) {
        return new RabbitAdmin(template.getConnectionFactory());
    }
    
    /**
     * Creates a message listener container for consuming messages in tests.
     *
     * @param connectionFactory The connection factory
     * @param queueName The queue to listen to
     * @param listener The message listener
     * @param messageConverter The message converter to use
     * @return A configured SimpleMessageListenerContainer
     */
    public static SimpleMessageListenerContainer createMessageListenerContainer(
            CachingConnectionFactory connectionFactory,
            String queueName,
            Object listener,
            MessageConverter messageConverter) {
        
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(queueName);
        
        MessageListenerAdapter adapter = new MessageListenerAdapter(listener);
        adapter.setMessageConverter(messageConverter);
        container.setMessageListener(adapter);
        
        return container;
    }
    
    /**
     * Sends a message to a RabbitMQ queue and waits for acknowledgment.
     *
     * @param template The RabbitTemplate to use
     * @param exchange The exchange to send to
     * @param routingKey The routing key
     * @param message The message to send
     */
    public static void sendRabbitMessage(RabbitTemplate template, String exchange, String routingKey, Object message) {
        template.convertAndSend(exchange, routingKey, message);
    }
    
    /**
     * Creates a test message listener that counts down a latch when a message is received.
     *
     * @param latch The latch to count down
     * @param messageHandler Optional handler to process the received message
     * @param <T> The message type
     * @return A message listener object
     */
    public static <T> Object createTestMessageListener(CountDownLatch latch, JavaConsumer<T> messageHandler) {
        return new Object() {
            @SuppressWarnings("unused") // Called by reflection from MessageListenerAdapter
            public void handleMessage(T message) {
                if (messageHandler != null) {
                    messageHandler.accept(message);
                }
                latch.countDown();
            }
        };
    }
    
    /**
     * Waits for a specified number of messages to be received within a timeout period.
     *
     * @param latch The latch to wait on
     * @param timeoutSeconds The timeout in seconds
     * @return true if all expected messages were received, false if the timeout was reached
     * @throws InterruptedException If the thread is interrupted while waiting
     */
    public static boolean waitForMessages(CountDownLatch latch, int timeoutSeconds) throws InterruptedException {
        return latch.await(timeoutSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * Waits for a specified number of messages to be received with the default timeout.
     *
     * @param latch The latch to wait on
     * @return true if all expected messages were received, false if the timeout was reached
     * @throws InterruptedException If the thread is interrupted while waiting
     */
    public static boolean waitForMessages(CountDownLatch latch) throws InterruptedException {
        return waitForMessages(latch, DEFAULT_TIMEOUT_SECONDS);
    }
    
    /**
     * Creates a test fixture for a common message type with random data.
     *
     * @param baseMessage The base message to modify
     * @param <T> The message type
     * @return The modified message with random data
     */
    public static <T> T createTestFixture(T baseMessage) {
        // This is a placeholder - in a real implementation, this would modify the message
        // with random but valid test data appropriate for the message type
        return baseMessage;
    }
    
    /**
     * Simulates a broker failure by stopping the embedded Kafka broker.
     *
     * @param broker The embedded Kafka broker to stop
     */
    public static void simulateKafkaBrokerFailure(EmbeddedKafkaBroker broker) {
        broker.destroy();
    }
    
    /**
     * Simulates a broker failure by stopping the embedded Qpid broker.
     *
     * @param broker The embedded Qpid broker to stop
     */
    public static void simulateRabbitMQBrokerFailure(Broker broker) {
        broker.shutdown();
    }
    
    /**
     * Simulates a network partition between the client and the broker.
     * This is a simplified simulation that just closes the consumer's connection.
     *
     * @param consumer The Kafka consumer
     */
    public static void simulateNetworkPartition(Consumer<?, ?> consumer) {
        consumer.close(Duration.ofSeconds(1));
    }
    
    /**
     * Simulates a network partition between the client and the broker.
     * This is a simplified simulation that just closes the connection factory.
     *
     * @param connectionFactory The RabbitMQ connection factory
     */
    public static void simulateNetworkPartition(CachingConnectionFactory connectionFactory) {
        connectionFactory.destroy();
    }
    
    /**
     * Creates a sample Qpid configuration file for testing.
     * This method generates a basic configuration file that can be used with the embedded Qpid broker.
     *
     * @param configPath The path where the configuration file should be created
     * @throws IOException If the file cannot be created
     */
    public static void createSampleQpidConfig(String configPath) throws IOException {
        String config = "{"
                + "\n  \"name\": \"EmbeddedBroker\","
                + "\n  \"modelVersion\": \"7.0\","
                + "\n  \"authenticationproviders\": ["
                + "\n    {"
                + "\n      \"name\": \"passwordFile\","
                + "\n      \"type\": \"Plain\","
                + "\n      \"users\": ["
                + "\n        {"
                + "\n          \"name\": \"guest\","
                + "\n          \"password\": \"guest\","
                + "\n          \"type\": \"managed\""
                + "\n        }"
                + "\n      ]"
                + "\n    }"
                + "\n  ],"
                + "\n  \"ports\": ["
                + "\n    {"
                + "\n      \"name\": \"AMQP\","
                + "\n      \"port\": \"${qpid.amqp_port}\","
                + "\n      \"authenticationProvider\": \"passwordFile\","
                + "\n      \"protocols\": [ \"AMQP_0_9_1\" ]"
                + "\n    }"
                + "\n  ],"
                + "\n  \"virtualhostnodes\": ["
                + "\n    {"
                + "\n      \"name\": \"default\","
                + "\n      \"type\": \"Memory\","
                + "\n      \"defaultVirtualHostNode\": \"true\","
                + "\n      \"virtualHostInitialConfiguration\": \"{\\\"type\\\": \\\"Memory\\\", \\\"name\\\": \\\"default\\\"}\""
                + "\n    }"
                + "\n  ]"
                + "\n}"; 
        
        Files.write(new File(configPath).toPath(), config.getBytes());
    }
    
    /**
     * Utility method to create a unique topic name for testing.
     *
     * @param prefix The prefix for the topic name
     * @return A unique topic name
     */
    public static String createUniqueTopicName(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }
    
    /**
     * Utility method to create a unique queue name for testing.
     *
     * @param prefix The prefix for the queue name
     * @return A unique queue name
     */
    public static String createUniqueQueueName(String prefix) {
        return prefix + "-" + UUID.randomUUID().toString().substring(0, 8);
    }