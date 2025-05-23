package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test for TLV protocol decoder.
 * 
 * This test class is designed to support both monolithic and microservices testing.
 * It can be run in the traditional monolithic environment or in the Protocol Service.
 */
public class TlvProtocolDecoderTest extends ProtocolTest {

    /**
     * Test basic protocol decoding functionality.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new TlvProtocolDecoder(null));

        verifyNull(decoder, binary(
                "30430f383630323437303330303934333931ff10393233323132323030303834353433340f533636385f415f56312e30315f454eff1130303a30433a45373a30303a30303a30300132"));

        verifyNull(decoder, binary(
                "30410f383630323437303330303934333931"));

        verifyNull(decoder, binary(
                "30420f3836303234373033303039343339310131"));

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