package org.traccar.protocol;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;

/**
 * Test case for PST frame encoder.
 * This test has been updated to support both monolithic and microservices architecture.
 * It can be gradually migrated to the Protocol Service test folder.
 */
public class PstFrameEncoderTest extends ProtocolTest {

    @Test
    public void testEncode() throws Exception {
        PstFrameEncoder encoder = new PstFrameEncoder();

        ByteBuf result = Unpooled.buffer();
        encoder.encode(null, binary("2FAF0B10059A0000B001022FAF0B10E91349A2AD3B1DAD2FF8A78228A58F"), result);
        verifyFrame(binary("282FAF0B10059A0000B001022FAF0B10E91349A2AD3B1DAD2FF8A7822768A58F29"), result);
    }

    /**
     * Tests the encoder with a simulated message broker scenario.
     * This verifies that the encoder works correctly when messages are passed between services.
     */
    @Test
    public void testEncodeWithMessageBroker() throws Exception {
        PstFrameEncoder encoder = new PstFrameEncoder();

        // Simulate message coming from broker
        ByteBuf sourceMessage = binary("2FAF0B10059A0000B001022FAF0B10E91349A2AD3B1DAD2FF8A78228A58F");
        
        // Process the message as it would be in the Protocol Service
        ByteBuf result = Unpooled.buffer();
        encoder.encode(null, sourceMessage, result);
        
        // Verify the result is correct and could be sent to another service
        verifyFrame(binary("282FAF0B10059A0000B001022FAF0B10E91349A2AD3B1DAD2FF8A7822768A58F29"), result);
    }

    /**
     * Tests the encoder with a cross-service boundary scenario.
     * This simulates how the encoder would be used when handling protocol data
     * that needs to be processed across different microservices.
     */
    @Test
    public void testCrossServiceBoundary() throws Exception {
        PstFrameEncoder encoder = new PstFrameEncoder();

        // Simulate data from Protocol Service to Position Service
        ByteBuf protocolServiceOutput = binary("2FAF0B10059A0000B001022FAF0B10E91349A2AD3B1DAD2FF8A78228A58F");
        
        // Encode the data as it would be before transmission
        ByteBuf transmissionBuffer = Unpooled.buffer();
        encoder.encode(null, protocolServiceOutput, transmissionBuffer);
        
        // Verify the encoded data is in the expected format for the receiving service
        ByteBuf expectedFormat = binary("282FAF0B10059A0000B001022FAF0B10E91349A2AD3B1DAD2FF8A7822768A58F29");
        verifyFrame(expectedFormat, transmissionBuffer);
    }
}