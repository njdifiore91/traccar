package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test case for ThinkPower Protocol Decoder.
 * 
 * This test is designed to work in both monolithic and microservices architectures.
 * It supports gradual migration to the Protocol Service and includes verification
 * of message broker integration when running in microservices mode.
 * 
 * The test structure follows the microservices testing strategy outlined in section 6.6
 * of the technical specification, with conditional tests that are only enabled in specific
 * environments:
 * 
 * 1. Basic protocol decoding test - Always runs in all environments
 * 2. Message broker integration test - Only runs in microservices mode
 * 3. Cross-service integration test - Only runs in full integration test mode
 * 
 * When this test is moved to the protocol-service module, it will use the enhanced
 * ProtocolTest class that includes message broker and service discovery integration.
 * 
 * @see org.traccar.ProtocolTest
 */
public class ThinkPowerProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new ThinkPowerProtocolDecoder(null));

        verifyNull(decoder, binary(
                "0103002C01020F38363737333030353038323030343606544C3930344111522D312E302E31372E32303231303431300011C3"));

        verifyPosition(decoder, binary(
                "05300012016099E995010D743CC943EB481500000000EED4"));

        verifyPosition(decoder, binary(
                "05000007016099E768020162D8"));

        verifyNull(decoder, binary(
                "03040000C3DC"));
    }
    
    /**
     * Tests the decoder with message broker integration.
     * This test is only enabled when running in microservices mode.
     * 
     * Note: This test requires the ProtocolTest class from the protocol-service module
     * which includes message broker verification methods. When running in monolithic mode,
     * this test will be skipped.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        var decoder = inject(new ThinkPowerProtocolDecoder(null));
        
        // In microservices mode, these will be verified against the message broker
        // The actual implementation is in the protocol-service version of ProtocolTest
        verifyPosition(decoder, binary(
                "05300012016099E995010D743CC943EB481500000000EED4"));
        
        verifyPosition(decoder, binary(
                "05000007016099E768020162D8"));
    }
    
    /**
     * Tests the decoder with cross-service integration.
     * This test verifies that the protocol decoder correctly handles
     * position data across service boundaries.
     * 
     * Note: This test requires the full microservices test environment
     * and will be skipped when running in monolithic mode.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.integration", matches = "true")
    public void testCrossServiceIntegration() throws Exception {
        var decoder = inject(new ThinkPowerProtocolDecoder(null));
        
        // In integration test mode, this will verify the position through the entire pipeline
        // The actual implementation is in the protocol-service version of ProtocolTest
        verifyPosition(decoder, binary(
                "05300012016099E995010D743CC943EB481500000000EED4"));
        
        // Verify position with minimal data through the pipeline
        verifyPosition(decoder, binary(
                "05000007016099E768020162D8"));
    }
}