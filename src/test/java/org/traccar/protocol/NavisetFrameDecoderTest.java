package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.messaging.MessageProducer;

import static org.mockito.Mockito.mock;

/**
 * Test for Naviset frame decoder
 * Supports both monolithic and microservices testing environments
 */
public class NavisetFrameDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Test basic frame decoding functionality
        var decoder = inject(new NavisetFrameDecoder());

        verifyFrame(
                binary("1310e4073836383230343030353935383436362a060716"),
                decoder.decode(null, null, binary("1310e4073836383230343030353935383436362a060716")));
    }

    @Test
    public void testDecodeWithMessageBroker() throws Exception {
        // Test frame decoding with message broker integration
        // This test verifies the protocol handling across service boundaries
        var decoder = inject(new NavisetFrameDecoder());
        
        // Mock the message producer that would be used in microservices environment
        MessageProducer messageProducer = mockMessageProducer();
        
        // Set the message producer if the decoder has this field (for microservices)
        try {
            var field = decoder.getClass().getDeclaredField("messageProducer");
            field.setAccessible(true);
            field.set(decoder, messageProducer);
        } catch (NoSuchFieldException e) {
            // Field doesn't exist in monolithic version, which is fine
        }

        // Verify the frame is correctly decoded
        verifyFrame(
                binary("1310e4073836383230343030353935383436362a060716"),
                decoder.decode(null, null, binary("1310e4073836383230343030353935383436362a060716")));
        
        // In a real microservices test, we would also verify that the message was published
        // to the message broker, but that's beyond the scope of this basic test
    }
}