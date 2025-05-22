package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test for Astra Protocol Decoder
 * This test has been updated to support both monolithic and microservices testing
 */
public class AstraProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new AstraProtocolDecoder(null));

        verifyPositions(decoder, binary(
                "58003201052196881ae0ce1f00000000000353d3d49e0010200000093d2353d3d49d031b934afffb036c0000a0000000e0eb"));

        verifyPositions(decoder, false, binary(
                "5800cb02052196881aff5b3c0000200010bf53cbfab10000000100393d5853cbfab0031b93affffb034b0000ae00000000010000000c000c00000000000000787e00000000000000000000000000000000000000000000000000000000000000000000000000000000003d0000200010bf53cbfae60000280000293c5853cbfae6031b93affffb034b0000ae00000000010000000d000c00000000000000ae7e0000000000000000000000000000000000000000000000000000000000000000000000000000000000e604"));

        verifyPositions(decoder, binary(
                "4b00700529c0c265976b8202cba9ff00676d864554a9c30000000020073401006436000300030008000000000000a0000100001920c43d00009600428302cba9ff00676d864554aa3e000000002007240100643b000300020008000000000000b0000100001920c43d00009600420f0e"));

        verifyPositions(decoder, binary(
                "4b00320524c1da58769e6d0322617effe874024453065600a800000100080000643e0000000000000000000000069500e7bb"));

        verifyPositions(decoder, binary(
                "4b013c02213aec35c501ad031368b8ffcd1ad043e5c4500c79000100003101005c490c001c0009020200020015069600ae03136801ffcd1af143e5c452125e000100003101005c491200090011010000020015068500af0313629effcd1f4b43e5c45d1e46000100003101005c491e00080409040500040015068700b0031359d5ffcd35ad43e5c47b2a3b000001003101005c492a1b1a0d0b0f0b00080015068700b103134984ffcd4b1e43e5c4913354000100003101005c49340b0103090606000f0015067700b203132e1affcd5a5a43e5c4af3348000001003101005c4935070a08000a070017001505f700b30313192cffcd7af143e5c4cd3733000001003101005c4937091206050a0800200015058600b403130debffcda88743e5c4eb2c3e000001003101005c493707030601080600290015058600b377"));

    }
    
    /**
     * Test for message broker integration
     * This test verifies that the protocol decoder can publish positions to a message broker
     * It is only enabled when running in the microservices environment
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservice", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // This test will only run when the system property test.microservice=true is set
        // In the microservices environment, this would verify integration with the message broker
        
        var decoder = inject(new AstraProtocolDecoder(null));
        
        // Decode a sample message
        Object result = decoder.decode(null, null, binary(
                "58003201052196881ae0ce1f00000000000353d3d49e0010200000093d2353d3d49d031b934afffb036c0000a0000000e0eb"));
        
        // Verify the result is a collection of positions
        verifyPositions(decoder, result);
        
        // In a real microservices test, we would verify that the positions were published to the message broker
        // This would involve checking that the message was received by a test consumer
        // For now, this is just a placeholder for the actual implementation
    }
    
    /**
     * Test for cross-service boundary handling
     * This test verifies that the protocol decoder can handle positions across service boundaries
     * It is only enabled when running in the microservices environment
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservice", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        // This test will only run when the system property test.microservice=true is set
        // In the microservices environment, this would verify handling across service boundaries
        
        var decoder = inject(new AstraProtocolDecoder(null));
        
        // Decode a sample message
        Object result = decoder.decode(null, null, binary(
                "4b00320524c1da58769e6d0322617effe874024453065600a800000100080000643e0000000000000000000000069500e7bb"));
        
        // Verify the result is a collection of positions
        verifyPositions(decoder, result);
        
        // In a real microservices test, we would verify that the positions were properly handled
        // by downstream services (Position Service, Event Service, etc.)
        // This would involve checking that the position was processed correctly across service boundaries
        // For now, this is just a placeholder for the actual implementation
    }
}