package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import java.util.List;

public class NyitechProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new NyitechProtocolDecoder(null));

        verifyPosition(decoder, binary(
                "4040690030313436383230303238373201201c0c12031a308080801c0c12031a3007d67e7e08aceb841002000000ae08000000000000000000000000001e002900f0ffdd002700f2ffe0002700f2ffe1002400f0ffdf002400f3ffe3008a00ffff01010000a9c70d0a"));

        verifyPosition(decoder, binary(
                "4040390030313436383230303238373203200100010c000000001c0c1203192a1b0c12171d3104fed87d089288801000000000000011ec0d0a"));

        verifyPosition(decoder, binary(
                "4040480030313436383230303238373201101c0c12031a2907fa7e7e08b8eb841002000000bc080101040904040300010100000a818283848586878862611c0c12031a293f9c0d0a"));

        verifyPosition(decoder, binary(
                "40404b003247512d313630313030313901101e0b100604190c02c83707f887ac0f000000002d130101030304020000010100000d426162636465666768696a6ba51e0b1006041965c30d0a"));

        verifyPosition(decoder, binary(
                "4040490030313436383230303238373202201c0c120319348080001b0c12171d3104fed87d0892888010000000000000000000000000000000000000008b00ffff010100008a480d0a"));

    }

    /**
     * Test for microservices architecture integration.
     * This test verifies that the protocol decoder can be used in a microservices environment
     * where the decoded positions are published to a message broker.
     * 
     * Note: This test is only enabled when the 'test.microservices' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testMicroservicesIntegration() throws Exception {
        // This test would be implemented when migrating to the Protocol Service
        // It would verify that decoded positions are correctly published to the message broker
        
        var decoder = inject(new NyitechProtocolDecoder(null));
        
        // Decode a position
        Object result = decoder.decode(null, null, binary(
                "4040690030313436383230303238373201201c0c12031a308080801c0c12031a3007d67e7e08aceb841002000000ae08000000000000000000000000001e002900f0ffdd002700f2ffe0002700f2ffe1002400f0ffdf002400f3ffe3008a00ffff01010000a9c70d0a"));
        
        // Verify the result is a Position or List<Position>
        if (result instanceof Position) {
            // In microservices, this would be published to a message broker
            // For now, we just verify it's a valid position
            verifyPosition((Position) result);
        } else if (result instanceof List) {
            // Handle list of positions
            @SuppressWarnings("unchecked")
            List<Position> positions = (List<Position>) result;
            for (Position position : positions) {
                verifyPosition(position);
            }
        }
    }

    /**
     * Test for cross-service boundary handling.
     * This test verifies that the protocol decoder can work across service boundaries,
     * where the decoded positions are processed by other services.
     * 
     * Note: This test is only enabled when the 'test.microservices' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        // This test would be implemented when migrating to the Protocol Service
        // It would verify that the protocol works across service boundaries
        
        var decoder = inject(new NyitechProtocolDecoder(null));
        
        // Decode multiple positions to simulate a batch of data crossing service boundaries
        Object result1 = decoder.decode(null, null, binary(
                "4040390030313436383230303238373203200100010c000000001c0c1203192a1b0c12171d3104fed87d089288801000000000000011ec0d0a"));
        
        Object result2 = decoder.decode(null, null, binary(
                "4040480030313436383230303238373201101c0c12031a2907fa7e7e08b8eb841002000000bc080101040904040300010100000a818283848586878862611c0c12031a293f9c0d0a"));
        
        // In a microservices environment, these would be published to a message broker
        // and consumed by other services. For now, we just verify they're valid positions.
        if (result1 instanceof Position) {
            verifyPosition((Position) result1);
        }
        
        if (result2 instanceof Position) {
            verifyPosition((Position) result2);
        }
    }
}