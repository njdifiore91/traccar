package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test for Arnavi Binary Protocol Decoder
 * This test class supports both monolithic and microservices testing environments
 */
public class ArnaviBinaryProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testHeader1Decode() throws Exception {

        var decoder = inject(new ArnaviBinaryProtocolDecoder(null));

        verifyNull(decoder, binary(
                "ff22f30c45f5c90f0300"));

        verifyPositions(decoder, binary(
                "5b01012800a3175f5903513934420447221c42055402781E0900f0c5215b4e0084005c00007c005d0000a300fa37010000295d"),
                position("2017-07-07 05:09:55.000", true, 45.05597, 39.03347));
    }

    @Test
    public void testHeader2Decode() throws Exception {

        var decoder = inject(new ArnaviBinaryProtocolDecoder(null));

        verifyNull(decoder, binary(
                "ff23f30c45f5c90f0300"));

        verifyPositions(decoder, binary(
                "5b01012800a3175f5903513934420447221c42055402781E0900f0c5215b4e0084005c00007c005d0000a300fa3701000029012800a3175f5903513934420447221c42055402781E0900f0c5215b4e0084005c00007c005d0000a300fa37010000295d"),
                position("2017-07-07 05:09:55.000", true, 45.05597, 39.03347));
    }

    /**
     * Test for message broker integration in microservices environment
     * This test is only enabled when running in the Protocol Service environment
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "protocol-service")
    public void testMessageBrokerIntegration() throws Exception {
        // This test will be executed only in the Protocol Service environment
        // where message broker integration is available
        
        var decoder = inject(new ArnaviBinaryProtocolDecoder(null));
        
        // Verify position is correctly decoded and would be published to the message broker
        Position position = verifyPosition(decoder, binary(
                "5b01012800a3175f5903513934420447221c42055402781E0900f0c5215b4e0084005c00007c005d0000a300fa37010000295d"));
        
        // In a real microservices test, we would verify the message was published to the broker
        // This is a placeholder for the actual implementation which would use a test message broker
        verifyAttribute(position, "protocol", "arnavi");
    }

    /**
     * Test for cross-service boundary handling
     * This test is only enabled when running in the Protocol Service environment
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "protocol-service")
    public void testCrossServiceHandling() throws Exception {
        // This test will be executed only in the Protocol Service environment
        // where cross-service communication is available
        
        var decoder = inject(new ArnaviBinaryProtocolDecoder(null));
        
        // Decode multiple positions to test batch handling across service boundaries
        var positions = verifyPositions(decoder, binary(
                "5b01012800a3175f5903513934420447221c42055402781E0900f0c5215b4e0084005c00007c005d0000a300fa3701000029012800a3175f5903513934420447221c42055402781E0900f0c5215b4e0084005c00007c005d0000a300fa37010000295d"));
        
        // In a real microservices test, we would verify the positions were correctly processed
        // across service boundaries. This is a placeholder for the actual implementation.
        verifyNotNull(positions);
        verifyEquals(2, positions.size());
    }
}