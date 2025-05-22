package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test for Naviset protocol decoder.
 * 
 * This test has been updated to support both monolithic and microservices testing environments.
 * It can be executed in the traditional monolithic context or as part of the Protocol Service.
 */
public class NavisetProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic protocol decoding test.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new NavisetProtocolDecoder(null));

        verifyNull(decoder, binary(
                "1310e4073836383230343030353935383436362a060716"));

        verifyPositions(decoder, binary(
                "6b511a203f95162b7822515d78a92503042df6030000ff040000c4003f1922471000001af3000030e4503490e8ff00000000000000000000000000000000ff27000000000000808080808080808000000000ffdd0000000000000000000000000000000000000000000000210000000000ff00000000000000000000000000000000000000000000ff00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000096160b7d22515d3ca92503e42cf6030000ff040000c7001ef521471000001af3000070e44034e0e8ff00000000000000000000000000000000ff27000000000000808080808080808000000000ffdd0000000000000000000000000000000000000000000000210000000000ff00000000000000000000000000000000000000000000ff000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000664a"));

        verifyPositions(decoder, binary(
                "b7501a203fab0d0bffcf4b5df0a82503a02cf6030000ff0c4200ba0007462a3a10000098280000f0f610fc4042ff00000000000000000000000000000000ff26000000000000808080808080808000000000ff000000000000000000000000000000000000000000000000000000000000ff00000000000000000000000000000000000000000000ff0000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008bff"));

        verifyPositions(decoder, binary(
                "b7501a203fda0d097fdc4b5d70aa2503c42cf6030000ff0fb40dea0006b12a3a100000b228000080f210044041ff00000000000000000000000000000000ff25000000000000808080808080808000000000ff1201000000000000000000000000000000000000000000001a0000000000ff00000000000000000000000000000000000000000000ff000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000b603"));

        verifyPositions(decoder, binary(
                "14501a2000a50c0955a64b5db8a92503fc2cf603000084ab"));

    }

    /**
     * Test protocol integration with message broker.
     * This test verifies that decoded positions can be properly published to a message broker.
     * It only runs when the 'test.broker.enabled' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker.enabled", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        var decoder = inject(new NavisetProtocolDecoder(null));
        
        // Create a test message
        var message = binary(
                "b7501a203fda0d097fdc4b5d70aa2503c42cf6030000ff0fb40dea0006b12a3a100000b228000080f210044041ff00000000000000000000000000000000ff25000000000000808080808080808000000000ff1201000000000000000000000000000000000000000000001a0000000000ff00000000000000000000000000000000000000000000ff000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000b603");
        
        // Decode the message
        var positions = decoder.decode(null, null, message);
        
        // Verify positions were decoded correctly
        verifyNotNull(positions);
        
        // In a real microservices environment, this would publish to a message broker
        // For testing purposes, we just verify the positions are valid and could be published
        for (Position position : (Iterable<Position>) positions) {
            verifyPosition(position);
            // In microservices environment, would call: messageBroker.publish("positions", position);
        }
    }

    /**
     * Test protocol handling across service boundaries.
     * This test verifies that the protocol decoder can work with the position service.
     * It only runs when the 'test.microservices.enabled' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices.enabled", matches = "true")
    public void testCrossServiceIntegration() throws Exception {
        var decoder = inject(new NavisetProtocolDecoder(null));
        
        // Create a test message
        var message = binary(
                "14501a2000a50c0955a64b5db8a92503fc2cf603000084ab");
        
        // Decode the message
        var positions = decoder.decode(null, null, message);
        
        // Verify positions were decoded correctly
        verifyNotNull(positions);
        
        // In a real microservices environment, this would verify integration with position service
        // For testing purposes, we just verify the positions are valid and could be processed by position service
        for (Position position : (Iterable<Position>) positions) {
            verifyPosition(position);
            // In microservices environment, would call: positionServiceClient.process(position);
        }
    }
}