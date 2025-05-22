package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Tests for PathAwayProtocolDecoder.
 * This test class has been updated to support both monolithic and microservices testing environments.
 */
public class PathAwayProtocolDecoderTest extends ProtocolTest {

    /**
     * Tests basic decoding functionality in a monolithic environment.
     * This test is maintained for backward compatibility.
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new PathAwayProtocolDecoder(null));
        
        verifyPosition(decoder, request(
                "?UserName=name&Password=pass&LOC=$PWS,1,\"Roger\",,,100107,122846,45.317270,-79.642219,45.00,42,1,\"Comment\",0*58"));
    }
    
    /**
     * Tests decoding functionality with message broker integration.
     * This test verifies that the decoder works correctly in a microservices environment
     * where decoded positions are published to a message broker.
     */
    @Test
    public void testDecodeWithMessageBroker() throws Exception {
        var decoder = injectWithMessageBroker(new PathAwayProtocolDecoder(null));
        
        // Create an expected position for verification
        Position expectedPosition = position("2010-01-07 12:28:46.000", true, 45.317270, -79.642219);
        
        // Verify the position is correctly decoded and published to the message broker
        verifyPositionWithBroker(decoder, request(
                "?UserName=name&Password=pass&LOC=$PWS,1,\"Roger\",,,100107,122846,45.317270,-79.642219,45.00,42,1,\"Comment\",0*58"), 
                expectedPosition);
    }
    
    /**
     * Tests decoding with additional parameters to verify cross-service handling.
     * This test ensures that the decoder correctly processes messages with various parameters
     * that might be needed by other services in the microservices architecture.
     */
    @Test
    public void testDecodeWithExtendedParameters() throws Exception {
        var decoder = injectWithMessageBroker(new PathAwayProtocolDecoder(null));
        
        // Test with additional parameters that might be processed by other services
        verifyPositionWithBroker(decoder, request(
                "?UserName=name&Password=pass&LOC=$PWS,1,\"Roger\",,,100107,122846,45.317270,-79.642219,45.00,42,1,\"Comment with service data\",0*58"));
    }
}