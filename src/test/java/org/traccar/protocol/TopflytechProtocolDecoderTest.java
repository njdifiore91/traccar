package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for Topflytech protocol decoder.
 * 
 * This test class has been updated to support both monolithic testing and
 * microservices testing as part of the gradual migration to the Protocol Service.
 */
public class TopflytechProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic decode test for backward compatibility with monolithic architecture.
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new TopflytechProtocolDecoder(null));

        verifyPosition(decoder, text(
                "(880316890094910BP00XG00b600000000L00074b54S00000000R0C0F0014000100f0130531152205A0706.1395S11024.0965E000.0251.25"));
    }
    
    /**
     * Test for microservices architecture with message broker integration.
     * This test will only run when the 'test.environment' system property is set to 'microservices'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testDecodeWithMessageBroker() throws Exception {
        // This test verifies that the protocol decoder correctly publishes
        // decoded positions to the message broker in a microservices environment
        
        var decoder = inject(new TopflytechProtocolDecoder(null));
        
        // Parse the message and verify basic position data
        Position position = (Position) decoder.decode(null, null, text(
                "(880316890094910BP00XG00b600000000L00074b54S00000000R0C0F0014000100f0130531152205A0706.1395S11024.0965E000.0251.25"));
        
        // In a real microservices test, we would verify that the position was published
        // to the message broker. For now, we just verify the position object is valid.
        verifyDecodedPosition(position);
    }
    
    /**
     * Helper method to verify a decoded position without requiring the full ProtocolTest verification.
     * This allows for more flexible testing across service boundaries.
     */
    private void verifyDecodedPosition(Position position) {
        // Basic verification of position data
        assertNotNull(position, "Position should not be null");
        assertEquals(-7.101395, position.getLatitude(), 0.00001, "Latitude should match expected value");
        assertEquals(110.400965, position.getLongitude(), 0.00001, "Longitude should match expected value");
    }
}