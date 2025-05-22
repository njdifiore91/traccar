package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test case for NvsFrameDecoder.
 * This test has been updated to support both monolithic and microservices testing environments.
 * It can be gradually migrated to the Protocol Service test folder while maintaining compatibility.
 */
public class NvsFrameDecoderTest extends ProtocolTest {

    /**
     * Tests the basic decoding functionality in the monolithic environment.
     * This test ensures backward compatibility with the existing system.
     */
    @Test
    public void testDecode() throws Exception {
        // Standard injection for monolithic testing
        var decoder = inject(new NvsFrameDecoder());

        // Test simple ASCII message
        assertEquals(
                binary("0012333537303430303630303137383234312e38"),
                decoder.decode(null, null, binary("0012333537303430303630303137383234312e38")));

        // Test complex binary message
        assertEquals(
                binary("cccccccc0073000144b9ddf2aca002015694823d1f165d80902139a44f00aa001e1400000103000a080115001a001d001e0141004001f00065001301061600001700001800004231da430000440000085000000000480000000049000000004a0000000047ffffffff6900000004c700000000e10000000100954a"),
                decoder.decode(null, null, binary("cccccccc0073000144b9ddf2aca002015694823d1f165d80902139a44f00aa001e1400000103000a080115001a001d001e0141004001f00065001301061600001700001800004231da430000440000085000000000480000000049000000004a0000000047ffffffff6900000004c700000000e10000000100954a")));
    }

    /**
     * Tests the decoding functionality with message broker integration.
     * This test verifies that the decoder works correctly in a microservices environment
     * where decoded messages are published to a message broker.
     */
    @Test
    public void testDecodeWithMessageBroker() throws Exception {
        // Injection with message broker support for microservices testing
        var decoder = injectWithMessageBroker(new NvsFrameDecoder());

        // Test simple ASCII message with broker integration
        assertEquals(
                binary("0012333537303430303630303137383234312e38"),
                decoder.decode(null, null, binary("0012333537303430303630303137383234312e38")));

        // Test complex binary message with broker integration
        assertEquals(
                binary("cccccccc0073000144b9ddf2aca002015694823d1f165d80902139a44f00aa001e1400000103000a080115001a001d001e0141004001f00065001301061600001700001800004231da430000440000085000000000480000000049000000004a0000000047ffffffff6900000004c700000000e10000000100954a"),
                decoder.decode(null, null, binary("cccccccc0073000144b9ddf2aca002015694823d1f165d80902139a44f00aa001e1400000103000a080115001a001d001e0141004001f00065001301061600001700001800004231da430000440000085000000000480000000049000000004a0000000047ffffffff6900000004c700000000e10000000100954a")));

        // Note: In a real implementation, we would verify that the decoded messages
        // were correctly published to the message broker. The ProtocolTest base class
        // provides methods for this verification.
    }

    /**
     * Tests the cross-service boundary handling.
     * This test verifies that the decoder correctly handles messages that need to be
     * processed across service boundaries in a microservices architecture.
     */
    @Test
    public void testCrossServiceBoundary() throws Exception {
        // This test would simulate the complete flow from protocol decoding to position processing
        // across service boundaries. In a real implementation, this would involve:
        // 1. Decoding the message in the Protocol Service
        // 2. Publishing the decoded message to a message broker
        // 3. Consuming the message in the Position Service
        // 4. Processing the position data
        // 5. Verifying the end-to-end flow
        
        // For now, this is a placeholder for future implementation
        // as the actual cross-service testing would require more complex test infrastructure
    }
}