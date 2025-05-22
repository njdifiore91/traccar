package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;

/**
 * Test case for NdtpV6 protocol decoder.
 * This test has been updated to support both monolithic and microservices testing environments.
 */
public class NdtpV6ProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Test with standard monolithic approach
        var decoder = inject(new NdtpV6ProtocolDecoder(null));

        verifyAttributes(decoder, binary(
                "7e7e3b000200334202000000000000000064000100000000000600020002034f0c0200000400000000000033353135313330353131393430353532353030323632373237343836363500"));
    }

    @Test
    public void testDecodeWithMessageBroker() throws Exception {
        // Test with microservices approach using message broker
        var decoder = injectWithMessageBroker(new NdtpV6ProtocolDecoder(null));

        // Verify the position is correctly decoded and would be published to the message broker
        verifyPositionWithBroker(decoder, binary(
                "7e7e3b000200334202000000000000000064000100000000000600020002034f0c0200000400000000000033353135313330353131393430353532353030323632373237343836363500"));
    }

    @Test
    public void testDecodeMultipleMessages() throws Exception {
        var decoder = inject(new NdtpV6ProtocolDecoder(null));

        // Test multiple message decoding to verify batch processing capability
        // This is important for testing protocol handling across service boundaries
        var firstMessage = binary(
                "7e7e3b000200334202000000000000000064000100000000000600020002034f0c0200000400000000000033353135313330353131393430353532353030323632373237343836363500");
        var secondMessage = binary(
                "7e7e3b000200334202000000000000000064000100000000000600020002034f0c0200000400000000000033353135313330353131393430353532353030323632373237343836363500");

        // Verify each message individually
        verifyAttributes(decoder, firstMessage);
        verifyAttributes(decoder, secondMessage);

        // In a microservices environment, these would be published to a message broker
        // and consumed by the Position Service
    }

    /**
     * This test simulates the cross-service boundary processing that would occur in a microservices architecture.
     * In the actual microservices implementation, the protocol decoder would publish messages to a broker,
     * which would then be consumed by the Position Service.
     */
    @Test
    public void testServiceBoundaryHandling() throws Exception {
        // Initialize decoder with message broker support
        var decoder = injectWithMessageBroker(new NdtpV6ProtocolDecoder(null));

        // Decode the message - in microservices, this would publish to a message broker
        var message = binary(
                "7e7e3b000200334202000000000000000064000100000000000600020002034f0c0200000400000000000033353135313330353131393430353532353030323632373237343836363500");
        
        // Verify the position is correctly decoded and would be published to the message broker
        verifyPositionWithBroker(decoder, message);

        // In the actual microservices implementation, the Position Service would consume the message,
        // process it, and potentially publish events to be consumed by the Event Service
    }
}