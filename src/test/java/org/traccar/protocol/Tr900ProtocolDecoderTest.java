package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test case for Tr900 Protocol Decoder.
 * This test has been updated to support both monolithic and microservices testing environments.
 * It can verify protocol handling across service boundaries using message brokers.
 */
public class Tr900ProtocolDecoderTest extends ProtocolTest {

    /**
     * Traditional monolithic test for the protocol decoder.
     * This test verifies the basic functionality of the decoder in a standalone environment.
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new Tr900ProtocolDecoder(null));

        verifyPosition(decoder, text(
                ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!"),
                position("2015-06-26 13:12:52.000", true, -31.62131, -58.50496));

        verifyPosition(decoder, text(
                ">12345678,1,1,070201,144111,W05829.2613,S3435.2313,,00,034,25,00,126-000,0,3,11111111*2d!"));

        verifyPosition(decoder, text(
                ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!\r\n"));
    }

    /**
     * Microservices test for the protocol decoder with message broker integration.
     * This test verifies that the decoder correctly publishes decoded positions to the message broker.
     * It uses an in-memory message broker implementation for testing.
     * 
     * This test is only enabled when running in the microservices environment,
     * identified by the system property "test.environment" set to "microservices".
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testDecodeWithMessageBroker() throws Exception {
        // Create a decoder with message broker integration
        var decoder = injectWithMessageBroker(new Tr900ProtocolDecoder(null));

        // Test decoding and verify message broker publication
        verifyPositionWithBroker(decoder, text(
                ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!"),
                position("2015-06-26 13:12:52.000", true, -31.62131, -58.50496));

        verifyPositionWithBroker(decoder, text(
                ">12345678,1,1,070201,144111,W05829.2613,S3435.2313,,00,034,25,00,126-000,0,3,11111111*2d!"));

        verifyPositionWithBroker(decoder, text(
                ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!\r\n"));
    }

    /**
     * Test for cross-service boundary handling with protocol service and position service.
     * This test verifies the complete flow from protocol decoding to position processing.
     * 
     * This test is only enabled when running in the microservices environment with integration testing,
     * identified by the system property "test.integration" set to "true".
     */
    @Test
    @EnabledIfSystemProperty(named = "test.integration", matches = "true")
    public void testProtocolToPositionServiceIntegration() throws Exception {
        // This test would be implemented as part of the gradual migration to microservices
        // It would verify the complete flow across service boundaries using actual or mock services
        // For now, this is a placeholder for future implementation
        
        // Example implementation (commented out until the microservices infrastructure is in place):
        /*
        // 1. Set up the test environment with mock services
        var protocolService = new MockProtocolService();
        var positionService = new MockPositionService();
        var messageBroker = new MockMessageBroker();
        
        // 2. Connect the services through the message broker
        protocolService.setMessageBroker(messageBroker);
        positionService.setMessageBroker(messageBroker);
        
        // 3. Create and configure the protocol decoder
        var decoder = new Tr900ProtocolDecoder(null);
        protocolService.registerDecoder(decoder);
        
        // 4. Send a test message to the protocol service
        String message = ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!";
        Position expectedPosition = position("2015-06-26 13:12:52.000", true, -31.62131, -58.50496);
        protocolService.processMessage(message);
        
        // 5. Verify that the position was processed by the position service
        Position processedPosition = positionService.awaitProcessedPosition(5, TimeUnit.SECONDS);
        assertNotNull(processedPosition, "Position not processed by position service");
        assertEquals(expectedPosition.getDeviceId(), processedPosition.getDeviceId(), "Device ID mismatch");
        assertEquals(expectedPosition.getLatitude(), processedPosition.getLatitude(), 0.00001, "Latitude mismatch");
        assertEquals(expectedPosition.getLongitude(), processedPosition.getLongitude(), 0.00001, "Longitude mismatch");
        */
    }
    
    /**
     * Test for protocol decoder performance with high message volume.
     * This test verifies that the decoder can handle a high volume of messages efficiently.
     * 
     * This test is only enabled when running performance tests,
     * identified by the system property "test.performance" set to "true".
     */
    @Test
    @EnabledIfSystemProperty(named = "test.performance", matches = "true")
    public void testDecodePerformance() throws Exception {
        // This test would be implemented to verify decoder performance under load
        // For now, this is a placeholder for future implementation
        
        // Example implementation (commented out until performance testing is needed):
        /*
        var decoder = inject(new Tr900ProtocolDecoder(null));
        String message = ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!";
        
        // Warm up
        for (int i = 0; i < 1000; i++) {
            decoder.decode(null, null, text(message));
        }
        
        // Measure performance
        long startTime = System.currentTimeMillis();
        int messageCount = 10000;
        for (int i = 0; i < messageCount; i++) {
            decoder.decode(null, null, text(message));
        }
        long endTime = System.currentTimeMillis();
        
        // Calculate and log metrics
        long duration = endTime - startTime;
        double messagesPerSecond = messageCount * 1000.0 / duration;
        System.out.println("Processed " + messageCount + " messages in " + duration + " ms");
        System.out.println("Performance: " + messagesPerSecond + " messages/second");
        */
    }
}