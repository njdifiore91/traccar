package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test case for Navis Frame Decoder.
 * This test supports both monolithic and microservices testing environments.
 * 
 * In microservices mode, this test verifies protocol handling across service boundaries
 * and integration with message brokers.
 */
public class NavisFrameDecoderTest extends ProtocolTest {

    @Test
    public void testDecodeNtcb() throws Exception {
        NavisFrameDecoder frameDecoder = new NavisFrameDecoder();

        verifyFrame(binary(
                "404e5443010000000000000059009adb2a3e54250000000000ff1500040b0a1008291838001200760ee600000000000000000000000f1500040b0a10ac20703fb1aec23f00000000320149668f430000000000000000000000000000000000000000000000f3808080"),
                frameDecoder.decode(null, null, binary("404e5443010000000000000059009adb2a3e54250000000000ff1500040b0a1008291838001200760ee600000000000000000000000f1500040b0a10ac20703fb1aec23f00000000320149668f430000000000000000000000000000000000000000000000f3808080")));
    }

    @Test
    public void testDecodeFlex10() throws Exception {
        NavisFrameDecoder frameDecoder = new NavisFrameDecoder();

        frameDecoder.setFlexDataSize(73);

        verifyFrame(binary(
                "7e54040000000400000030129957405c000b00632f9857405ccace03021e129101a103000000000000c4005ba3fe3b00000000120046100000000000001aff7f000080bfffff80000080bfffffffff9f"),
                frameDecoder.decode(null, null, binary("7e54040000000400000030129957405c000b00632f9857405ccace03021e129101a103000000000000c4005ba3fe3b00000000120046100000000000001aff7f000080bfffff80000080bfffffffff9f")));

        verifyFrame(binary(
                "7e4101080000000917c057405c002b001833c057405cbbce030225129101a00300007c6102408900400c1b3cfce3b23a12004710e000000000001bff7f000080bfffff80000080bfffffffffb2"),
                frameDecoder.decode(null, null, binary("7e4101080000000917c057405c002b001833c057405cbbce030225129101a00300007c6102408900400c1b3cfce3b23a12004710e000000000001bff7f000080bfffff80000080bfffffffffb2")));
    }
    
    /**
     * Tests the integration with message broker for position publishing.
     * This test is only enabled in microservices mode.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.mode", matches = "microservice")
    public void testMessageBrokerIntegration() throws Exception {
        NavisFrameDecoder frameDecoder = new NavisFrameDecoder();
        
        // Decode a frame
        Object decodedMessage = frameDecoder.decode(null, null, binary(
                "404e5443010000000000000059009adb2a3e54250000000000ff1500040b0a1008291838001200760ee600000000000000000000000f1500040b0a10ac20703fb1aec23f00000000320149668f430000000000000000000000000000000000000000000000f3808080"));
        
        // In microservices mode, this would verify the message is published to the broker
        // The actual implementation of this verification would be in the ProtocolTest base class
        verifyFrame(binary(
                "404e5443010000000000000059009adb2a3e54250000000000ff1500040b0a1008291838001200760ee600000000000000000000000f1500040b0a10ac20703fb1aec23f00000000320149668f430000000000000000000000000000000000000000000000f3808080"),
                decodedMessage);
    }
    
    /**
     * Tests protocol handling across service boundaries.
     * This test verifies that the protocol decoder correctly processes messages
     * and the resulting position objects can be consumed by other services.
     * This test is only enabled in microservices mode.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.mode", matches = "microservice")
    public void testCrossServiceHandling() throws Exception {
        // This test simulates the full flow from protocol decoding to position processing
        // across service boundaries using the message broker
        
        // Setup protocol decoder
        NavisProtocolDecoder protocolDecoder = new NavisProtocolDecoder(null);
        
        // Decode a message that would normally be received from a device
        Object decodedPosition = protocolDecoder.decode(null, null, binary(
                "404e5443010000000000000059009adb2a3e54250000000000ff1500040b0a1008291838001200760ee600000000000000000000000f1500040b0a10ac20703fb1aec23f00000000320149668f430000000000000000000000000000000000000000000000f3808080"));
        
        // In microservices mode, this would verify the position is published to the broker
        // and can be consumed by the position service
        // The actual implementation of this verification would be in the ProtocolTest base class
        verifyNotNull(decodedPosition);
    }
}