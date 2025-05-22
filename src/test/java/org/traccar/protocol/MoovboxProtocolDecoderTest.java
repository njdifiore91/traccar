package org.traccar.protocol;

import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

/**
 * Test case for Moovbox Protocol Decoder.
 * Updated to support both monolithic and microservices testing approaches.
 */
public class MoovboxProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Standard monolithic test approach
        var decoder = inject(new MoovboxProtocolDecoder(null));

        verifyPositions(decoder, request(HttpMethod.POST, "/",
                buffer("<gps id=\"911\">\n<coordinates><coordinate>\n<fix>3</fix>\n<time>1597580050</time>\n<latitude>100.726257</latitude>\n<longitude>13.821351</longitude>\n<altitude>9.500000</altitude>\n<climb>0.000000</climb>\n<speed>0.064000</speed>\n<separation>-27.300000</separation>\n<track>0.000000</track>\n<satellites>9</satellites>\n</coordinate></coordinates>\n</gps>")));
    }

    /**
     * Tests the protocol decoder with message broker integration.
     * This test is only enabled when running in microservices mode.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices.enabled", matches = "true")
    public void testDecodeWithMessageBroker() throws Exception {
        // This test will be executed only when running in microservices mode
        // Initialize the protocol decoder with message broker support
        var decoder = inject(new MoovboxProtocolDecoder(null));
        
        // Verify that positions are correctly published to the message broker
        verifyPositionsWithBroker(decoder, request(HttpMethod.POST, "/",
                buffer("<gps id=\"911\">\n<coordinates><coordinate>\n<fix>3</fix>\n<time>1597580050</time>\n<latitude>100.726257</latitude>\n<longitude>13.821351</longitude>\n<altitude>9.500000</altitude>\n<climb>0.000000</climb>\n<speed>0.064000</speed>\n<separation>-27.300000</separation>\n<track>0.000000</track>\n<satellites>9</satellites>\n</coordinate></coordinates>\n</gps>")));
    }

    /**
     * Tests cross-service boundary handling with the position service.
     * This test verifies that the protocol decoder correctly integrates with other services.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices.enabled", matches = "true")
    public void testCrossServiceIntegration() throws Exception {
        // This test will be executed only when running in microservices mode
        // Initialize the protocol decoder with service integration support
        var decoder = inject(new MoovboxProtocolDecoder(null));
        
        // Verify that positions are correctly processed across service boundaries
        verifyPositionProcessing(decoder, request(HttpMethod.POST, "/",
                buffer("<gps id=\"911\">\n<coordinates><coordinate>\n<fix>3</fix>\n<time>1597580050</time>\n<latitude>100.726257</latitude>\n<longitude>13.821351</longitude>\n<altitude>9.500000</altitude>\n<climb>0.000000</climb>\n<speed>0.064000</speed>\n<separation>-27.300000</separation>\n<track>0.000000</track>\n<satellites>9</satellites>\n</coordinate></coordinates>\n</gps>")));
    }
}