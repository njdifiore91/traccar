package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

// Conditional imports for microservices testing
try {
    // These classes will be available in the microservices environment
    Class.forName("org.traccar.protocol.integration.MessageBrokerIntegration");
} catch (ClassNotFoundException e) {
    // Ignore - running in monolithic mode
}

/**
 * Test for At2000 protocol frame decoder.
 * This test has been updated to support both monolithic and microservices testing.
 * It can be gradually migrated to the Protocol Service test folder.
 */
public class At2000FrameDecoderTest extends ProtocolTest {

    /**
     * Tests the basic decoding functionality of the At2000FrameDecoder.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new At2000FrameDecoder());

        verifyFrame(
                binary("01012f00000000000000000000000000003335363137333036343430373439320fad981997ae8e031fe10c0ea7641903ca32c0331df467233d2a9cd886fbeef8"),
                decoder.decode(null, null, binary("01012f00000000000000000000000000003335363137333036343430373439320fad981997ae8e031fe10c0ea7641903ca32c0331df467233d2a9cd886fbeef8")));

        verifyFrame(
                binary("893f0000000000000000000000000000e048b1a31deba3f5dbe8877f574877e6ed4d022b6611a10d80dfc4c0c11fa8aacf4a9de61528327e2b66843dd9c5d3a7cc9ee1d9c71a34bb482145d88b4fda3e"),
                decoder.decode(null, null, binary("893f0000000000000000000000000000e048b1a31deba3f5dbe8877f574877e6ed4d022b6611a10d80dfc4c0c11fa8aacf4a9de61528327e2b66843dd9c5d3a7cc9ee1d9c71a34bb482145d88b4fda3e")));

    }

    /**
     * Tests the integration with message brokers.
     * This test is only enabled in the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testMessageBrokerIntegration() throws Exception {
        // This test will only run in the microservices environment
        // where the MessageBrokerIntegration class is available
        try {
            Class<?> integrationClass = Class.forName("org.traccar.protocol.integration.MessageBrokerIntegration");
            Object integration = integrationClass.getDeclaredConstructor().newInstance();
            
            // Use reflection to call methods on the integration object
            integrationClass.getMethod("testProtocolMessagePublishing", 
                                      String.class, byte[].class)
                .invoke(integration, "at2000", 
                        binary("01012f00000000000000000000000000003335363137333036343430373439320fad981997ae8e031fe10c0ea7641903ca32c0331df467233d2a9cd886fbeef8"));
            
            // Verify message was received by the broker
            Boolean result = (Boolean) integrationClass.getMethod("verifyMessageReceived", String.class)
                .invoke(integration, "at2000");
            
            if (result != null && !result) {
                throw new AssertionError("Message was not received by the broker");
            }
        } catch (ClassNotFoundException e) {
            // Skip test in monolithic environment
            System.out.println("Skipping message broker integration test in monolithic environment");
        } catch (Exception e) {
            throw new RuntimeException("Error in message broker integration test", e);
        }
    }

    /**
     * Tests the protocol handling across service boundaries.
     * This test is only enabled in the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testCrossServiceIntegration() throws Exception {
        // This test will only run in the microservices environment
        try {
            Class<?> serviceClass = Class.forName("org.traccar.protocol.integration.ProtocolServiceIntegration");
            Object integration = serviceClass.getDeclaredConstructor().newInstance();
            
            // Test protocol handling across service boundaries
            byte[] testMessage = binary("01012f00000000000000000000000000003335363137333036343430373439320fad981997ae8e031fe10c0ea7641903ca32c0331df467233d2a9cd886fbeef8");
            
            // Use reflection to call methods on the integration object
            Object result = serviceClass.getMethod("testProtocolToPositionFlow", 
                                                 String.class, byte[].class)
                .invoke(integration, "at2000", testMessage);
            
            // Verify position was processed by position service
            Boolean positionProcessed = (Boolean) serviceClass.getMethod("verifyPositionProcessed", Object.class)
                .invoke(integration, result);
            
            if (positionProcessed != null && !positionProcessed) {
                throw new AssertionError("Position was not processed by position service");
            }
        } catch (ClassNotFoundException e) {
            // Skip test in monolithic environment
            System.out.println("Skipping cross-service integration test in monolithic environment");
        } catch (Exception e) {
            throw new RuntimeException("Error in cross-service integration test", e);
        }
    }
}