package org.traccar.protocol;

/**
 * Test case for Tr900 Protocol Decoder.
 * 
 * This test class has been updated to support both monolithic and microservices architectures:
 * - The original test method is maintained for backward compatibility
 * - A new test method is added to verify integration with the message broker
 * - Position verification is enhanced to support cross-service communication
 */

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;
import org.traccar.config.Config;
import org.traccar.messaging.MessagePublisher;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.timeout;
import static org.mockito.ArgumentMatchers.any;

public class Tr900ProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Standard monolithic test approach
        // Note: In monolithic architecture, the decoder constructor only takes the protocol parameter
        var decoder = inject(new Tr900ProtocolDecoder(null));

        verifyPosition(decoder, text(
                ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!"),
                position("2015-06-26 13:12:52.000", true, -31.62131, -58.50496));

        verifyPosition(decoder, text(
                ">12345678,1,1,070201,144111,W05829.2613,S3435.2313,,00,034,25,00,126-000,0,3,11111111*2d!"));

        verifyPosition(decoder, text(
                ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!\r\n"));
    }
    
    @Test
    public void testDecodeWithMessageBroker() throws Exception {
        // Microservices test approach with message broker integration
        MessagePublisher messagePublisher = mock(MessagePublisher.class);
        Config config = mock(Config.class);
        
        // Create decoder with message publisher for microservices architecture
        // Note: In microservices architecture, the decoder constructor takes additional parameters
        // for message publishing and configuration
        var decoder = inject(new Tr900ProtocolDecoder(null, messagePublisher, config));
        
        // Test decoding and message publishing
        Position position = (Position) decoder.decode(null, null, text(
                ">00001001,4,1,150626,131252,W05830.2978,S3137.2783,,00,348,18,00,003-000,0,3,11111011*3b!"));
        
        // Verify position was correctly decoded
        verifyPosition(position, "2015-06-26 13:12:52.000", true, -31.62131, -58.50496);
        
        // Verify message was published to broker for cross-service communication
        verify(messagePublisher, timeout(1000)).publish(any(), any());
    }
    
    /**
     * Helper method to verify position details directly from a Position object
     * Used for microservices testing where we need to verify the position before it's published
     */
    private void verifyPosition(Position position, String time, boolean valid, double lat, double lon) throws Exception {
        Position expected = position(time, valid, lat, lon);
        
        assertEquals(expected.getFixTime(), position.getFixTime(), "time");
        assertEquals(expected.getValid(), position.getValid(), "valid");
        assertEquals(expected.getLatitude(), position.getLatitude(), 0.00001, "latitude");
        assertEquals(expected.getLongitude(), position.getLongitude(), 0.00001, "longitude");
    }
}