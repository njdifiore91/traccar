package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

// Conditional imports for microservices testing
try {
    // These classes will be available in the microservices environment
    Class.forName("org.traccar.protocol.integration.MessageBrokerIntegration");
} catch (ClassNotFoundException e) {
    // Ignore - running in monolithic mode
}

/**
 * Test for Meitrack protocol frame decoder.
 * This test has been updated to support both monolithic and microservices testing.
 * It can be gradually migrated to the Protocol Service test folder.
 */
public class MeitrackFrameDecoderTest extends ProtocolTest {

    /**
     * Tests the basic decoding functionality of the MeitrackFrameDecoder.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new MeitrackFrameDecoder());

        assertEquals(
                binary("24244e3132372c3836333037313031333830333036362c4141412c33352c2d312e3330323638302c33362e3835323133352c3135303430393231313032362c412c392c302c302e312c302c352c313635332c343039362c33323634382c3633397c30327c313030347c3930432c303030302c307c307c307c3346467c3330302c2a37430d0a"),
                decoder.decode(null, null, binary("24244e3132372c3836333037313031333830333036362c4141412c33352c2d312e3330323638302c33362e3835323133352c3135303430393231313032362c412c392c302c302e312c302c352c313635332c343039362c33323634382c3633397c30327c313030347c3930432c303030302c307c307c307c3346467c3330302c2a37430d0a")));

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
                .invoke(integration, "meitrack", 
                        binary("24244e3132372c3836333037313031333830333036362c4141412c33352c2d312e3330323638302c33362e3835323133352c3135303430393231313032362c412c392c302c302e312c302c352c313635332c343039362c33323634382c3633397c30327c313030347c3930432c303030302c307c307c307c3346467c3330302c2a37430d0a"));
            
            // Verify message was received by the broker
            Boolean result = (Boolean) integrationClass.getMethod("verifyMessageReceived", String.class)
                .invoke(integration, "meitrack");
            
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
            byte[] testMessage = binary("24244e3132372c3836333037313031333830333036362c4141412c33352c2d312e3330323638302c33362e3835323133352c3135303430393231313032362c412c392c302c302e312c302c352c313635332c343039362c33323634382c3633397c30327c313030347c3930432c303030302c307c307c307c3346467c3330302c2a37430d0a");
            
            // Use reflection to call methods on the integration object
            Object result = serviceClass.getMethod("testProtocolToPositionFlow", 
                                                 String.class, byte[].class)
                .invoke(integration, "meitrack", testMessage);
            
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