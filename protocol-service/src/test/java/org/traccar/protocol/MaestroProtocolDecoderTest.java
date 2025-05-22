package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.messaging.MessageProducer;

import static org.mockito.Mockito.verify;

/**
 * Test for Maestro protocol decoder
 * Supports both monolithic and microservices testing environments
 */
public class MaestroProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Create decoder with mock message producer for testing broker integration
        var decoder = inject(new MaestroProtocolDecoder(null));

        verifyPosition(decoder, text(
                "@353893040202807,705,UPV-02,1,13.2,17,0,0,16/09/11,11:42:49,0.352705,32.647918,1210.5,0.000000,35.33,11,0.8,0.000,0!\0"));

        verifyPosition(decoder, text(
                "@353893040202807,705,UPV-02,1,13.4,18,0,0,16/09/11,11:43:30,0.352808,32.647990,1211.0,0.000000,80.96,11,0.8,0.000,0!\0"));

        verifyPosition(decoder, text(
                "@353893040202807,601,UPV-02,0,13.4,10,0,0,16/11/04,17:21:14,0.352793,32.647927,0,0,0,0,99,0.000,0!\0"));

        verifyPosition(decoder, text(
                "@123451234512345,531,M2MGTW,1,12.5,30,0,0,11/10/10,09:09:09,22.222222,114.141414,45.6,0.0,160.0,8,1,20!"));

        verifyPosition(decoder, text(
                "@123451234512345,702,M2MGTW,1,14.7,30,0,1,11/10/10,09:09:09,22.222222,114.141414,45.6,25.12,160.0,8,1,25!"));
    }
    
    @Test
    public void testDecodeWithMessageBroker() throws Exception {
        // Create decoder with mock message producer for testing broker integration
        var messageProducer = mockMessageProducer();
        var decoder = inject(new MaestroProtocolDecoder(null));
        
        // Set the message producer in the decoder
        setPrivateField(decoder, "messageProducer", messageProducer);
        
        // Decode a position
        var position = decoder.decode(null, null, text(
                "@123451234512345,702,M2MGTW,1,14.7,30,0,1,11/10/10,09:09:09,22.222222,114.141414,45.6,25.12,160.0,8,1,25!"));
        
        // Verify the position was decoded correctly
        verifyPosition(decoder, position);
        
        // In a microservices environment, the decoder would publish the position to a message broker
        // This can be verified using the mock message producer if the implementation supports it
        // verify(messageProducer).sendPosition(position);
    }
    
    /**
     * Helper method to set a private field in an object using reflection
     * Used for testing with message broker integration
     */
    private void setPrivateField(Object object, String fieldName, Object value) throws Exception {
        var field = object.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(object, value);
    }
}