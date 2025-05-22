package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test for PT3000 protocol decoder.
 * 
 * This test class is designed to support both monolithic and microservices testing.
 * It can be run in the traditional monolithic environment or in the Protocol Service.
 */
public class Pt3000ProtocolDecoderTest extends ProtocolTest {

    /**
     * Test basic protocol decoding functionality.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new Pt3000ProtocolDecoder(null));

        verifyPosition(decoder, text(
                "%356939010012099,$GPRMC,124945.752,A,4436.6245,N,01054.4634,E,0.11,358.52,060408,,,A,+393334347445,N028d"),
                position("2008-04-06 12:49:45.000", true, 44.61041, 10.90772));

        verifyPosition(decoder, text(
                "%356939010014433,$GPRMC,172821.000,A,4019.5147,N,00919.1160,E,0.00,,010613,,,A,+393998525043,N098d"));

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