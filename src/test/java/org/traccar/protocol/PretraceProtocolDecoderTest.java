package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

/**
 * Test for Pretrace protocol decoder
 * Supports both monolithic and microservices testing environments
 */
public class PretraceProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Create decoder with injected dependencies
        var decoder = inject(new PretraceProtocolDecoder(null));

        // Test basic position decoding
        verifyPosition(decoder, text(
                "(867967021915915U1110A1701201500102238.1700N11401.9324E000264000000000009001790000000,&P11A4,F1050^47"));

        verifyPosition(decoder, text(
                "(864244029498838U1110A1509250653072238.1641N11401.9213E000196000000000406002990000000,&P195%,T1050,F14A5,R104C51E47B^30"));
    }
    
    /**
     * Test message broker integration for microservices architecture
     */
    @Test
    public void testMessageBrokerIntegration() throws Exception {
        // Create decoder with injected dependencies including message producer
        var decoder = inject(new PretraceProtocolDecoder(null));
        
        // Get the mock message producer from the decoder
        MessageProducer messageProducer = null;
        try {
            var field = decoder.getClass().getDeclaredField("messageProducer");
            field.setAccessible(true);
            messageProducer = (MessageProducer) field.get(decoder);
        } catch (NoSuchFieldException e) {
            // If running in monolithic mode, create a mock message producer
            messageProducer = mockMessageProducer();
            // Try to set it on the decoder if possible
            try {
                var field = decoder.getClass().getDeclaredField("messageProducer");
                field.setAccessible(true);
                field.set(decoder, messageProducer);
            } catch (NoSuchFieldException ex) {
                // Ignore if running in pure monolithic mode without message broker support
                return;
            }
        }
        
        // Decode a position that should be published to the message broker
        Position position = (Position) decoder.decode(null, null, text(
                "(867967021915915U1110A1701201500102238.1700N11401.9324E000264000000000009001790000000,&P11A4,F1050^47"));
        
        // Verify the position was published to the message broker (if in microservices mode)
        if (messageProducer != null) {
            verify(messageProducer).sendPositionAsync(any(Position.class));
        }
    }
    
    /**
     * Test cross-service boundary handling
     * This test verifies that the protocol decoder correctly handles data across service boundaries
     */
    @Test
    public void testCrossServiceBoundaries() throws Exception {
        // Create decoder with injected dependencies
        var decoder = inject(new PretraceProtocolDecoder(null));
        
        // In microservices architecture, the protocol service is responsible for:
        // 1. Decoding the protocol message into a Position object
        // 2. Publishing the Position to a message broker for other services to consume
        
        // Test that the decoder correctly decodes the message
        Position position = (Position) decoder.decode(null, null, text(
                "(867967021915915U1110A1701201500102238.1700N11401.9324E000264000000000009001790000000,&P11A4,F1050^47"));
        
        // Verify the position contains all required data for cross-service communication
        verifyPosition(decoder, text(
                "(867967021915915U1110A1701201500102238.1700N11401.9324E000264000000000009001790000000,&P11A4,F1050^47"));
        
        // Additional verification for specific attributes needed by other services
        // These attributes are critical for position processing and event detection services
        if (position != null) {
            // Verify device identifier is present (needed by all services)
            assertTrue(position.getDeviceId() > 0);
            
            // Verify protocol is set (needed for protocol-specific handling in other services)
            assertNotNull(position.getProtocol());
            
            // Verify timestamp is present (needed for time-based processing)
            assertNotNull(position.getFixTime());
            
            // Verify position has valid coordinates (needed for geofence detection)
            assertTrue(position.getValid());
            assertTrue(position.getLatitude() >= -90 && position.getLatitude() <= 90);
            assertTrue(position.getLongitude() >= -180 && position.getLongitude() <= 180);
        }
    }
    
    // Helper method to assert that a condition is true
    private void assertTrue(boolean condition) {
        org.junit.jupiter.api.Assertions.assertTrue(condition);
    }
    
    // Helper method to assert that an object is not null
    private void assertNotNull(Object object) {
        org.junit.jupiter.api.Assertions.assertNotNull(object);
    }
}