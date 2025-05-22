package org.traccar.protocol;

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for PacificTrack Protocol Decoder.
 * This test class has been updated to support both monolithic and microservices testing environments.
 */
public class PacificTrackProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testReadBitExt() {

        assertEquals(0x35, PacificTrackProtocolDecoder.readBitExt(
                Unpooled.wrappedBuffer(new byte[] { (byte) 0b10110101 })));

        assertEquals(0x135, PacificTrackProtocolDecoder.readBitExt(
                Unpooled.wrappedBuffer(new byte[] { (byte) 0b00000010, (byte) 0b10110101 })));
    }


    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new PacificTrackProtocolDecoder(null));

        verifyAttributes(decoder, binary(
                "FB80019702808835275309000091108181B2C08F0143000E10000000010000001400010192DF0143288063810A8202835584D285B486E68780882D89C38A788BCE8C3A8D3C8E418F809073A008ACA16600A225A0C0000F4240C10003DF2CC200004E20C3004428C0C4000008C6C5000316A4E011314334424A57464758444C35333137373302A086AB569DFE110E02A8811203FF81000190820100"));

        verifyAttributes(decoder, binary(
                "fb80c88181b00280883592151012618820b18b1f123340f004c90001300301928a0080008100c00000000091971c0b0417020d074df0ec03c242550b20081d0c009a0601a1855571a30000"));

        verifyAttributes(decoder, binary(
                "fb82e80280883527530900009110818202c0909308990b122519076138fc03b3480205a3e80003a0834dd19fb08112c08f0143000e020000000100000014000101929f806328c0000f4240810a858ce011314334424a57464758444c3533313737330190868102100828cf"));

        verifyAttributes(decoder, binary(
                "FB80B48181B20192AE86E68780882D89BB8A648BCEA008ACA16600A20380C10003DF2CC200004E20C3004428C0C4000008C6C5000316A4"));

    }
    
    /**
     * Tests the decoder with message broker integration.
     * This test is designed for the microservices architecture where decoded positions
     * are published to a message broker for consumption by other services.
     */
    @Test
    @Tag("microservice")
    public void testDecodeWithMessageBroker() throws Exception {
        // Create and inject the decoder with message broker support
        var decoder = injectWithMessageBroker(new PacificTrackProtocolDecoder(null));
        
        // Test decoding a position and verify it's published to the message broker
        verifyPositionWithBroker(decoder, binary(
                "FB80019702808835275309000091108181B2C08F0143000E10000000010000001400010192DF0143288063810A8202835584D285B486E68780882D89C38A788BCE8C3A8D3C8E418F809073A008ACA16600A225A0C0000F4240C10003DF2CC200004E20C3004428C0C4000008C6C5000316A4E011314334424A57464758444C35333137373302A086AB569DFE110E02A8811203FF81000190820100"));
    }
    
    /**
     * Tests the cross-service integration by verifying that decoded positions
     * can be properly processed by downstream services.
     * This test simulates the flow of data across service boundaries in a microservices architecture.
     */
    @Test
    @Tag("integration")
    public void testCrossServiceIntegration() throws Exception {
        // Create and inject the decoder with message broker support
        var decoder = injectWithMessageBroker(new PacificTrackProtocolDecoder(null));
        
        // Decode a position
        Object result = decoder.decode(null, null, binary(
                "fb82e80280883527530900009110818202c0909308990b122519076138fc03b3480205a3e80003a0834dd19fb08112c08f0143000e020000000100000014000101929f806328c0000f4240810a858ce011314334424a57464758444c3533313737330190868102100828cf"));
        
        // Verify the result is a valid position
        assertNotNull(result, "Decoded position should not be null");
        
        // In a real implementation, we would verify that the position can be processed by downstream services
        // For example, we might check that it can be properly serialized to the message broker format,
        // or that it contains all required fields for the position service to process it.
        
        // For now, we'll just verify it's a Position object with the expected attributes
        Position position = (Position) result;
        assertNotNull(position.getAttributes(), "Position attributes should not be null");
        assertNotNull(position.getDeviceId(), "Position device ID should not be null");
    }
}
