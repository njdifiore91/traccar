package org.traccar.protocol;

import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test for Polte protocol decoder.
 * 
 * This test class is designed to support both monolithic and microservices testing.
 * It can be run in the traditional monolithic environment or in the Protocol Service.
 */
public class PolteProtocolDecoderTest extends ProtocolTest {

    /**
     * Test basic protocol decoding functionality.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new PolteProtocolDecoder(null));

        verifyPosition(decoder, request(HttpMethod.POST, "/",
                buffer("{\"_id\":\"5f75cf7b02c5023bfc0beaf7\",\"location\":{\"LocationMetrics\":{\"EnvironmentDensity\":1,\"LocationType\":2,\"carrierInfo\":{\"aux\":{\"PLMN\":\"310410\",\"country\":\"United States\",\"name\":\"ATT Wireless Inc\"},\"crs\":{\"PLMN\":\"310410\",\"country\":\"United States\",\"name\":\"ATT Wireless Inc\"}},\"hdop\":1850000,\"leversion\":\"2.2.18-20200729T140651\",\"towercount\":1},\"altitude\":0.0011297669261693954,\"confidence\":783.7972188868215,\"detected_at\":1601556342,\"latitude\":29.77368956725161,\"longitude\":-98.26530342694024,\"towerDB\":\"default\",\"ueToken\":\"ALT12503DE04336CB2E3A4A113FCDE05DF05A6F\",\"uid\":\"WZuDMv5Je\"},\"report\":{\"battery\":{\"count\":555,\"level\":100,\"voltage\":3.52},\"event\":3,\"time\":\"2020-10-01T12:45:48.207Z\"},\"time\":\"2020-10-01T12:45:42Z\",\"ueToken\":\"ALT12503DE04336CB2E3A4A113FCDE05DF05A6F\",\"uid\":\"WZuDMv5Je\"}")));

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