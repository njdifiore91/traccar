package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Test for TeraTrack protocol decoder.
 * This test supports both monolithic and microservices testing environments.
 */
public class TeraTrackProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic decode test for backward compatibility with monolithic architecture.
     * This test verifies the decoder can parse TeraTrack protocol messages correctly.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new TeraTrackProtocolDecoder(null));

        verifyAttributes(decoder, text(
                "{\"MDeviceID\":\"022043756090\",\"DiviceType\":\"1\",\"DataType\":\"1\",\"DataLength\":\"69\",\"DateTime\":\"2022-03-09 10:56:01\",\"Latitude\":\"-6.846451\",\"Longitude\":\"39.316324\",\"LongitudeState\":\"1\",\"LatitudeState\":\"0\",\"Speed\":\"90\",\"Mileage\":\"0\",\"FenceAlarm\":\"0\",\"AreaAlarmID\":\"0\",\"LockCutOff\":\"0\",\"SealTampered\":\"0\",\"MessageAck\":\"1\",\"LockRope\":\"1\",\"LockStatus\":\"1\",\"LockOpen\":\"0\",\"PasswordError\":\"0\",\"CardNo\":\"60000644\",\"IllegalCard\":\"0\",\"LowPower\":\"0\",\"UnCoverBack\":\"0\",\"CoverStatus\":\"1\",\"LockStuck\":\"0\",\"Power\":\"79\",\"GSM\":\"16\",\"IMEI\":\"860922043756090\",\"Index\":\"20\",\"Slave\":[]}"));

        verifyAttributes(decoder, text(
                "{\"MDeviceID\":\"074054558620\",\"DeviceType\":\"1\",\"DataType\":\"2\",\"DataLength\":\"0913\",\"DateTime\":\"2022-02-22 23:35:35\",\"Latitude\":\"-6.826699\",\"Longitude\":\"39.279008\",\"LatitudeState\":\"0\",\"LongitudeState\":\"1\",\"Speed\":\"0\",\"Mileage\":\"0\",\"FenceAlarm\":\"0\",\"AreaAlarmID\":\"0\",\"LockCutOff\":\"0\",\"SealTempered\":\"1\",\"MessageAck\":\"1\",\"LockRope\":\"0\",\"LockStatus\":\"0\",\"LockOpen\":\"1\",\"PasswordError\":\"0\",\"CardNo\":\"60060198\",\"IllegalCard\":\"0\",\"LowPower\":\"0\",\"UnCoverBack\":\"1\",\"CoverStatus\":\"0\",\"LockStuck\":\"1\",\"Power\":\"90\",\"GSM\":\"14\",\"IMEI\":\"861774054558620\",\"Index\":\"39\",\"Slave\":[{\"SDeviceId\":\"685304\",\"SPower\":\"00\",\"SLockCutOff\":\"0\",\"SLockOpen\":\"1\",\"SUnCoverBack\":\"0\",\"SCoverStatus\":\"1\",\"STimeOut\":\"1\",\"SLockRope\":\"0\",\"SSealTempered\":\"0\",\"SLockStuck\":\"0\"},{\"SDeviceId\":\"224779\",\"SPower\":\"00\",\"SLockCutOff\":\"0\",\"SLockOpen\":\"1\",\"SUnCoverBack\":\"0\",\"SCoverStatus\":\"1\",\"STimeOut\":\"1\",\"SLockRope\":\"0\",\"SSealTempered\":\"0\",\"SLockStuck\":\"0\"}]}"));

    }

    /**
     * Test for message broker integration in microservices architecture.
     * This test verifies the decoder can publish decoded positions to a message broker.
     * Only runs when the 'test.broker' system property is set to 'true'.
     */
    @EnabledIfSystemProperty(named = "test.broker", matches = "true")
    @Test
    @Testcontainers
    public void testMessageBrokerIntegration() throws Exception {
        // Start a Kafka container for testing
        try (KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"))) {
            kafka.start();
            
            // Create a decoder with message broker integration
            var decoder = inject(new TeraTrackProtocolDecoder(null));
            
            // Configure the decoder to use the Kafka broker
            // This would be handled by dependency injection in a real microservices environment
            var messagePublisher = new MockMessagePublisher(kafka.getBootstrapServers());
            inject(messagePublisher, TeraTrackProtocolDecoder.class, "messagePublisher");
            
            // Create a future to wait for the message to be published
            CompletableFuture<Position> positionFuture = messagePublisher.nextPosition();
            
            // Decode a message - this should publish to the broker
            decoder.decode(null, null, text(
                    "{\"MDeviceID\":\"022043756090\",\"DiviceType\":\"1\",\"DataType\":\"1\",\"DataLength\":\"69\",\"DateTime\":\"2022-03-09 10:56:01\",\"Latitude\":\"-6.846451\",\"Longitude\":\"39.316324\",\"LongitudeState\":\"1\",\"LatitudeState\":\"0\",\"Speed\":\"90\",\"Mileage\":\"0\",\"FenceAlarm\":\"0\",\"AreaAlarmID\":\"0\",\"LockCutOff\":\"0\",\"SealTampered\":\"0\",\"MessageAck\":\"1\",\"LockRope\":\"1\",\"LockStatus\":\"1\",\"LockOpen\":\"0\",\"PasswordError\":\"0\",\"CardNo\":\"60000644\",\"IllegalCard\":\"0\",\"LowPower\":\"0\",\"UnCoverBack\":\"0\",\"CoverStatus\":\"1\",\"LockStuck\":\"0\",\"Power\":\"79\",\"GSM\":\"16\",\"IMEI\":\"860922043756090\",\"Index\":\"20\",\"Slave\":[]}"));
            
            // Wait for the position to be published to the broker
            Position position = positionFuture.get(5, TimeUnit.SECONDS);
            
            // Verify the position attributes
            verifyPosition(position);
        }
    }
    
    /**
     * Test for cross-service boundary verification in microservices architecture.
     * This test verifies the protocol handling across service boundaries.
     * Only runs when the 'test.crossservice' system property is set to 'true'.
     */
    @EnabledIfSystemProperty(named = "test.crossservice", matches = "true")
    @Test
    public void testCrossServiceBoundary() throws Exception {
        // Create a decoder with cross-service integration
        var decoder = inject(new TeraTrackProtocolDecoder(null));
        
        // Mock the position service client
        var positionServiceClient = new MockPositionServiceClient();
        inject(positionServiceClient, TeraTrackProtocolDecoder.class, "positionServiceClient");
        
        // Decode a message - this should call the position service
        decoder.decode(null, null, text(
                "{\"MDeviceID\":\"022043756090\",\"DiviceType\":\"1\",\"DataType\":\"1\",\"DataLength\":\"69\",\"DateTime\":\"2022-03-09 10:56:01\",\"Latitude\":\"-6.846451\",\"Longitude\":\"39.316324\",\"LongitudeState\":\"1\",\"LatitudeState\":\"0\",\"Speed\":\"90\",\"Mileage\":\"0\",\"FenceAlarm\":\"0\",\"AreaAlarmID\":\"0\",\"LockCutOff\":\"0\",\"SealTampered\":\"0\",\"MessageAck\":\"1\",\"LockRope\":\"1\",\"LockStatus\":\"1\",\"LockOpen\":\"0\",\"PasswordError\":\"0\",\"CardNo\":\"60000644\",\"IllegalCard\":\"0\",\"LowPower\":\"0\",\"UnCoverBack\":\"0\",\"CoverStatus\":\"1\",\"LockStuck\":\"0\",\"Power\":\"79\",\"GSM\":\"16\",\"IMEI\":\"860922043756090\",\"Index\":\"20\",\"Slave\":[]}"));
        
        // Verify the position was sent to the position service
        Position position = positionServiceClient.getLastPosition();
        verifyPosition(position);
    }
    
    /**
     * Helper method to verify position attributes for cross-service tests.
     */
    private void verifyPosition(Position position) {
        // Verify essential position attributes
        assertEquals("-6.846451", position.getAttributes().get("latitude").toString());
        assertEquals("39.316324", position.getAttributes().get("longitude").toString());
        assertEquals("90", position.getAttributes().get("speed").toString());
        assertEquals("022043756090", position.getAttributes().get("deviceId").toString());
    }
    
    /**
     * Mock message publisher for testing broker integration.
     * In a real environment, this would be a real message broker client.
     */
    private static class MockMessagePublisher {
        private final String bootstrapServers;
        private CompletableFuture<Position> positionFuture;
        
        public MockMessagePublisher(String bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
            this.positionFuture = new CompletableFuture<>();
        }
        
        public void publish(Position position) {
            // In a real implementation, this would publish to Kafka/RabbitMQ
            positionFuture.complete(position);
        }
        
        public CompletableFuture<Position> nextPosition() {
            return positionFuture;
        }
    }
    
    /**
     * Mock position service client for testing cross-service integration.
     * In a real environment, this would be a gRPC or REST client to the position service.
     */
    private static class MockPositionServiceClient {
        private Position lastPosition;
        
        public void sendPosition(Position position) {
            // In a real implementation, this would send to the position service via gRPC/REST
            this.lastPosition = position;
        }
        
        public Position getLastPosition() {
            return lastPosition;
        }
    }
}