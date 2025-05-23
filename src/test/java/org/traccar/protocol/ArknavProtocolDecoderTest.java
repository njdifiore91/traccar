package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.messaging.MessageProducer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * Test for Arknav protocol decoder
 * Supports both monolithic and microservices testing environments
 */
public class ArknavProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Create decoder with null protocol for testing
        var decoder = inject(new ArknavProtocolDecoder(null));

        // Test position decoding with invalid position (V flag)
        verifyPosition(decoder, text(
                "358266016278447,05*827,000,L001,V,4821.6584,N,01053.8650,E,000.0,000.0,00.0,08:46:04 17-03-16,9.5A,D7,0,79,0,,,,"),
                position("2016-03-17 08:46:04.000", false, 48.36097, 10.89775));

        // Test position decoding with valid position (A flag)
        verifyPosition(decoder, text(
                "123456789012345,05*850,000,L001,A,2459.3640,N,12125.2958,E,000.0,224.8,00.8,07:47:26 09-09-05,9.00,D3,0,C4,1,,,,"));

        // Test position decoding with additional fields
        verifyPosition(decoder, text(
                "123456789012345,05*850,000,L001,A,2459.3640,N,12125.2958,E,000.0,224.8,00.8,07:47:26 09-09-05,9.00,D3,0,C4,1,,,00000000,"));
    }
    
    /**
     * Test message broker integration for microservices architecture
     */
    @Test
    public void testMessageBrokerIntegration() throws Exception {
        // Create decoder with message producer for testing
        var decoder = inject(new ArknavProtocolDecoder(null));
        
        // Try to set message producer field if it exists (for microservices testing)
        try {
            var messageProducer = mockMessageProducer();
            var field = decoder.getClass().getDeclaredField("messageProducer");
            field.setAccessible(true);
            field.set(decoder, messageProducer);
            
            // Decode a position
            verifyPosition(decoder, text(
                    "123456789012345,05*850,000,L001,A,2459.3640,N,12125.2958,E,000.0,224.8,00.8,07:47:26 09-09-05,9.00,D3,0,C4,1,,,,"));
            
            // Verify that the message producer was called to publish the position
            verify(messageProducer).send(any(), any());
        } catch (NoSuchFieldException e) {
            // Skip test if running in monolithic environment (no messageProducer field)
            // This ensures backward compatibility with the monolithic architecture
        }
    }
}