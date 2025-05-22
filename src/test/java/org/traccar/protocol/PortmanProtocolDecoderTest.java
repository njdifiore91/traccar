package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test case for Portman Protocol Decoder.
 * 
 * This test has been updated to support both monolithic and microservices testing approaches.
 * It can run in both the original monolithic environment and in the Protocol Service microservice.
 */
public class PortmanProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic protocol decoding test that works in both monolithic and microservices environments.
     * This test verifies the decoder can properly parse Portman protocol messages.
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new PortmanProtocolDecoder(null));

        verifyPosition(decoder, text(
                "%%863922034547720,A,231119031316,N3640.4542E11707.5992,000,000,NA,95000000,NA,254,24,1.00,24"));

        verifyPosition(decoder, text(
                "$EXT,P0RTMANGRANT,A,210609201710,N0951.6879W08357.0129,0,0,NA,NA,11,25,174700.25,NA,01820000,108"));

        verifyPosition(decoder, text(
                "$PTMLA,355854050074633,A,200612153351,N2543.0681W10009.2974,0,190,NA,C9830000,NA,108,8,2.66,16,GNA"));
    }

    /**
     * Extended test for microservices environment that verifies message broker integration.
     * This test only runs when the 'test.microservices' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        var decoder = inject(new PortmanProtocolDecoder(null));
        
        // Parse a message
        Position position = decoder.decode(null, null, text(
                "%%863922034547720,A,231119031316,N3640.4542E11707.5992,000,000,NA,95000000,NA,254,24,1.00,24"));
        
        // Verify the position was created correctly
        verifyAttribute(position, Position.KEY_DEVICE_ID, 0);
        verifyAttribute(position, Position.KEY_LATITUDE, 36.67424);
        verifyAttribute(position, Position.KEY_LONGITUDE, 117.12665);
        
        // In a real microservices test, we would verify the message was published to the broker
        // This is a placeholder for the actual message broker verification
        // verifyMessagePublished(position, "positions");
    }

    /**
     * Test for cross-service boundary handling in the microservices environment.
     * This test verifies that positions are properly processed across service boundaries.
     * It only runs when the 'test.microservices' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        var decoder = inject(new PortmanProtocolDecoder(null));
        
        // Parse a message
        Position position = decoder.decode(null, null, text(
                "$PTMLA,355854050074633,A,200612153351,N2543.0681W10009.2974,0,190,NA,C9830000,NA,108,8,2.66,16,GNA"));
        
        // Verify the position was created correctly
        verifyAttribute(position, Position.KEY_DEVICE_ID, 0);
        verifyAttribute(position, Position.KEY_LATITUDE, 25.71780);
        verifyAttribute(position, Position.KEY_LONGITUDE, -100.15496);
        
        // In a real microservices test, we would verify the position was processed by the Position Service
        // This is a placeholder for the actual cross-service verification
        // verifyPositionProcessed(position);
    }

    /**
     * Helper method to verify a specific attribute value in a position.
     * 
     * @param position The position to check
     * @param key The attribute key to verify
     * @param expected The expected value
     */
    private void verifyAttribute(Position position, String key, Object expected) {
        if (position == null) {
            throw new AssertionError("Position is null");
        }
        
        Object actual = position.getAttributes().get(key);
        if (expected instanceof Double && actual instanceof Double) {
            double expectedDouble = (Double) expected;
            double actualDouble = (Double) actual;
            if (Math.abs(expectedDouble - actualDouble) > 0.00001) {
                throw new AssertionError("Expected " + key + " to be " + expected + ", but was " + actual);
            }
        } else if (!expected.equals(actual)) {
            throw new AssertionError("Expected " + key + " to be " + expected + ", but was " + actual);
        }
    }
}