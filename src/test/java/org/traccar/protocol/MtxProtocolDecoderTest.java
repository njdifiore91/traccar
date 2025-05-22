package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test for MTX protocol decoder.
 * 
 * This test class is designed to support both monolithic and microservices testing.
 * It can be run in the traditional monolithic environment or in the Protocol Service.
 */
public class MtxProtocolDecoderTest extends ProtocolTest {

    /**
     * Test basic protocol decoding functionality.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new MtxProtocolDecoder(null));

        verifyPosition(decoder, text(
                "#MTX,353815011138124,20101226,195550,41.6296399,002.3611174,000,035,000000.00,X,X,1111,000,0,0"));

    }

    /**
     * Test protocol integration with message brokers.
     * This test is only enabled in the microservices environment when the message broker is available.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker.enabled", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // This test will be implemented when the Protocol Service is fully migrated
        // It will verify that decoded positions are correctly published to the message broker
    }

    /**
     * Test protocol handling across service boundaries.
     * This test is only enabled in the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices.enabled", matches = "true")
    public void testCrossServiceIntegration() throws Exception {
        // This test will be implemented when the Protocol Service is fully migrated
        // It will verify that the protocol decoder correctly interacts with other services
    }
}