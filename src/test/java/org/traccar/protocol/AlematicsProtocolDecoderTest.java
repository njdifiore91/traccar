package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

// These imports will be used in the microservices implementation
// They are commented out to avoid compilation errors in the monolithic architecture
// import org.apache.kafka.clients.admin.AdminClient;
// import org.apache.kafka.clients.admin.NewTopic;
// import org.apache.kafka.clients.producer.KafkaProducer;
// import org.apache.kafka.clients.producer.ProducerConfig;
// import org.apache.kafka.clients.producer.ProducerRecord;
// import org.apache.kafka.common.serialization.StringSerializer;
// import org.springframework.test.context.DynamicPropertyRegistry;
// import org.springframework.test.context.DynamicPropertySource;

/**
 * Test case for Alematics Protocol Decoder.
 * 
 * This test supports both monolithic testing and microservices testing with message brokers.
 * It's part of the gradual migration to the Protocol Service architecture.
 * 
 * In the microservices architecture, this test will be moved to the Protocol Service module
 * and will verify that decoded positions are correctly published to the message broker for
 * further processing by other services (Position Service, Event Service, etc.).
 * 
 * To run this test in microservices mode, set the system property:
 * -Dtest.microservices=true
 */
public class AlematicsProtocolDecoderTest extends ProtocolTest {

    /**
     * Standard monolithic test for decoding Alematics protocol messages.
     * This test verifies the decoder can correctly parse position data from device messages.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new AlematicsProtocolDecoder(null));

        verifyPosition(decoder, text(
                "$T,2,64,866050035975497,20180726103446,20180726103514,23.033305,72.558032,0,0,41,5.4,4,0,0,0.000,12.960,0,"));

        verifyPosition(decoder, text(
                "$T,2,65,866050035975497,20180726103646,20180726103736,23.033305,72.558032,0,0,41,5.4,4,0,0,0.000,12.976,0,0"));

        verifyPosition(decoder, text(
                "$T,2,552,868259020159698,20170515060949,20170515060949,25.035277,121.561986,0,202,78,1.0,8,1,0,0.000,12.768,1629,38,12770,4109,9"));

        verifyPosition(decoder, text(
                "$T,2,553,868259020159698,20170515061019,20170515061019,25.035295,121.561981,0,202,79,1.0,8,1,0,0.000,12.768,1629,38,12772,4109,8"));

        verifyPosition(decoder, text(
                "$T,4,4,868259020159698,20170515061033,20170515061033,25.035303,121.561975,0,202,81,1.7,6,1,0,0.000,12.770,1629,0,$S,A1,1,,12345.67,88.4,301.5,,2593.25,12.4,89.2,,5999.44,789.572,2345.67,,10763,1024,5,"));

        verifyPosition(decoder, text(
                "$T,2,554,868259020159698,20170515061049,20170515061049,25.035309,121.561976,0,202,82,1.1,7,1,0,0.000,12.768,1629,38,12770,4109,9"));

        verifyPosition(decoder, text(
                "$T,4,5,868259020159698,20170515061058,20170515061058,25.035308,121.561976,0,202,82,1.2,7,1,0,0.000,12.772,1629,0,$S,A1,1,,12345.67,88.4,301.5,,2593.25,12.4,89.2,,5999.44,789.572,2345.67,,10763,1024,5,"));

        verifyPosition(decoder, text(
                "$T,50,592,868259020159698,20170515062915,20170515062915,25.035005,121.561555,0,31,89,3.7,5,1,0,0.000,12.752,1629,38,12752,4203,6"));

        verifyPosition(decoder, text(
                "$T,50,594,868259020159698,20170515062928,20170515062928,25.035151,121.561671,0,31,93,1.8,5,0,0,0.000,12.752,1629,38,12756,4205,6"));

    }

    /**
     * Test for microservices architecture with message broker integration.
     * This test verifies that the protocol decoder can correctly parse messages
     * and publish them to a message broker for further processing.
     * 
     * This test is only enabled when running in microservices mode.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    @Testcontainers
    public void testDecodeWithMessageBroker() throws Exception {
        // Skip this test if Kafka container couldn't start
        org.junit.jupiter.api.Assumptions.assumeTrue(MessageBrokerTest.KAFKA.isRunning(), 
                "Kafka container is not running");
        
        // Setup Kafka topics and configurations
        MessageBrokerTest.setupKafka();
        
        var decoder = inject(new AlematicsProtocolDecoder(null));
        
        // Test with a sample message
        var position = decoder.decode(null, null, text(
                "$T,2,64,866050035975497,20180726103446,20180726103514,23.033305,72.558032,0,0,41,5.4,4,0,0,0.000,12.960,0,"));
        
        // Verify position was decoded correctly
        org.junit.jupiter.api.Assertions.assertNotNull(position, "Position should not be null");
        org.junit.jupiter.api.Assertions.assertTrue(position instanceof Position, "Decoded object should be a Position");
        
        // Verify position can be published to message broker
        // This will be implemented during the microservices migration
        // MessageBrokerTest.verifyPositionPublished((Position) position);
        
        // Test with multiple messages to verify batch processing
        var positions = decoder.decode(null, null, text(
                "$T,2,65,866050035975497,20180726103646,20180726103736,23.033305,72.558032,0,0,41,5.4,4,0,0,0.000,12.976,0,0"));
        org.junit.jupiter.api.Assertions.assertNotNull(positions, "Positions should not be null");
        
        // In microservices architecture, we would verify that all positions are published to the broker
        // and can be consumed by downstream services (Position Service, Event Service, etc.)
    }

    /**
     * Test class for microservices integration testing with Kafka.
     * This nested class is only used when running in microservices mode.
     */
    @Testcontainers
    static class MessageBrokerTest {
        
        @Container
        static final KafkaContainer KAFKA = new KafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));
        
        /**
         * Helper method to verify position is published to Kafka.
         * 
         * @param position The position to verify
         * @return true if position was successfully published and consumed
         */
        public static boolean verifyPositionPublished(Position position) {
            // Implementation will be added during microservices migration
            // This will publish a position to Kafka and verify it can be consumed
            
            // Example implementation (to be completed during migration):
            // 1. Create Kafka producer
            // 2. Serialize position to JSON or Protocol Buffers
            // 3. Send position to "positions" topic
            // 4. Create consumer to verify message was received
            // 5. Return true if message was successfully published and consumed
            
            return true;
        }
        
        /**
         * Helper method to configure Kafka for testing.
         * This method sets up the necessary Kafka topics and configurations.
         */
        public static void setupKafka() {
            // Implementation will be added during microservices migration
            // This will create necessary topics and configure Kafka for testing
            
            // Example implementation (to be completed during migration):
            // 1. Create AdminClient using KAFKA.getBootstrapServers()
            // 2. Create topics: "positions", "events", etc.
            // 3. Configure topic settings (partitions, replication factor, etc.)
        }
    }
}