package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test for Navigil protocol decoder.
 * 
 * This test class is designed to support both monolithic and microservices testing.
 * It can be run in the traditional monolithic environment or in the Protocol Service.
 */
public class NavigilProtocolDecoderTest extends ProtocolTest {

    /**
     * Test basic protocol decoding functionality.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new NavigilProtocolDecoder(null));

        verifyNull(decoder, binary(
                "01004300040020000000f60203080200e7cd0f510c0000003b00000000000000"));

        verifyPosition(decoder, binary(
                "0100b3000f0024000000f4a803080200ca0c1151ef8885f0b82e6d130400c00403000000"));

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