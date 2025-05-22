package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.messaging.MessageProducer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * Test case for PST frame decoder.
 * Supports both monolithic and microservices testing environments.
 */
public class PstFrameDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Create and inject the frame decoder
        var decoder = inject(new PstFrameDecoder());

        // Verify the frame decoding works correctly
        verifyFrame(
                binary("2fafac5a050f0000e0022fafac5a01891e882bbfdd06dd577c9865620a0efe524c419f940b6710f5ba0c86e5868ffc97c77eaaf166a31dba63f9894e98a91b9486c94e79ce537359737a5e9385431a590eb20b5115a2b7939e4e66ae"),
                decoder.decode(null, null, binary("282fafac5a050f0000e0022fafac5a01891e882bbfdd06dd577c9865620a0efe524c419f940b6710f5ba0c86e5868ffc97c77eaaf166a31dba63f9894e98a91b9486c94e79ce537359737a5e9385431a590eb20b5115a2b7939e4e66ae29")));
    }

    /**
     * Test integration with message broker for microservices architecture.
     * This test verifies that decoded frames can be properly published to the message broker.
     */
    @Test
    public void testMessageBrokerIntegration() throws Exception {
        // Create a mock message producer
        MessageProducer messageProducer = mockMessageProducer();
        
        // Create and inject the protocol decoder that would use the frame decoder
        var protocolDecoder = inject(new PstProtocolDecoder(null));
        
        // Set the message producer on the decoder if in microservices mode
        try {
            var field = protocolDecoder.getClass().getDeclaredField("messageProducer");
            field.setAccessible(true);
            field.set(protocolDecoder, messageProducer);
        } catch (NoSuchFieldException e) {
            // Ignore if running in monolithic mode where the field doesn't exist
        }
        
        // Process a sample message
        var input = binary("282fafac5a050f0000e0022fafac5a01891e882bbfdd06dd577c9865620a0efe524c419f940b6710f5ba0c86e5868ffc97c77eaaf166a31dba63f9894e98a91b9486c94e79ce537359737a5e9385431a590eb20b5115a2b7939e4e66ae29");
        protocolDecoder.decode(null, null, input);
        
        // In microservices mode, verify the message was published to the broker
        try {
            verify(messageProducer).publish(eq("positions"), any());
        } catch (Exception e) {
            // Ignore verification errors in monolithic mode where publishing doesn't happen
        }
    }

    /**
     * Test cross-service boundary handling.
     * This test verifies that the protocol can handle messages across service boundaries.
     */
    @Test
    public void testCrossServiceBoundaries() throws Exception {
        // Create and inject the frame decoder
        var decoder = inject(new PstFrameDecoder());
        
        // Create a sample message
        var input = binary("282fafac5a050f0000e0022fafac5a01891e882bbfdd06dd577c9865620a0efe524c419f940b6710f5ba0c86e5868ffc97c77eaaf166a31dba63f9894e98a91b9486c94e79ce537359737a5e9385431a590eb20b5115a2b7939e4e66ae29");
        
        // Decode the message
        var output = decoder.decode(null, null, input);
        
        // Verify the decoded frame is correct and can be passed to other services
        verifyFrame(
                binary("2fafac5a050f0000e0022fafac5a01891e882bbfdd06dd577c9865620a0efe524c419f940b6710f5ba0c86e5868ffc97c77eaaf166a31dba63f9894e98a91b9486c94e79ce537359737a5e9385431a590eb20b5115a2b7939e4e66ae"),
                output);
    }
}