package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Tests for the ManPower Protocol Decoder.
 * This test has been updated to support both monolithic and microservices testing environments.
 */
public class ManPowerProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new ManPowerProtocolDecoder(null));

        verifyPosition(decoder, text(
                "simei:352581250259539,,,tracker,51,24,1.73,130426023608,A,3201.5462,N,03452.2975,E,0.01,28B9,1DED,425,01,1x0x0*0x1*60x+2,en-us,"),
                position("2013-04-26 02:36:08.000", true, 32.02577, 34.87163));

        verifyPosition(decoder, text(
                "simei:352581250259539,,,weather,99,20,0.00,130426032310,V,3201.5517,N,03452.3064,E,1.24,28B9,25A1,425,01,1x0x0*0x1*60x+2,en-us,"));
        
        verifyPosition(decoder, text(
                "simei:352581250259539,,,SMS,54,19,90.41,130426172308,V,3201.5523,N,03452.2705,E,0.14,28B9,01A5,425,01,1x0x0*0x1*60x+2,en-us,"));
    }

    /**
     * Tests the decoder with message broker integration.
     * This test is only enabled in the microservices testing environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testDecodeWithMessageBroker() throws Exception {
        var decoder = injectWithMessageBroker(new ManPowerProtocolDecoder(null));

        // Test with a valid position
        Position expectedPosition = position("2013-04-26 02:36:08.000", true, 32.02577, 34.87163);
        verifyPositionWithBroker(decoder, text(
                "simei:352581250259539,,,tracker,51,24,1.73,130426023608,A,3201.5462,N,03452.2975,E,0.01,28B9,1DED,425,01,1x0x0*0x1*60x+2,en-us,"),
                expectedPosition);

        // Test with an invalid position
        verifyPositionWithBroker(decoder, text(
                "simei:352581250259539,,,weather,99,20,0.00,130426032310,V,3201.5517,N,03452.3064,E,1.24,28B9,25A1,425,01,1x0x0*0x1*60x+2,en-us,"));
    }

    /**
     * Tests the decoder with cross-service integration.
     * This test is only enabled in the integration testing environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.integration", matches = "true")
    public void testDecodeWithServiceIntegration() throws Exception {
        // This test would be implemented when the full microservices environment is available
        // It would test the protocol decoder's integration with other services like position-service
        // For now, this is a placeholder for future implementation
        
        // Example of how this might be implemented:
        // var decoder = injectWithFullServiceStack(new ManPowerProtocolDecoder(null));
        // verifyPositionWithServiceStack(decoder, text(
        //         "simei:352581250259539,,,tracker,51,24,1.73,130426023608,A,3201.5462,N,03452.2975,E,0.01,28B9,1DED,425,01,1x0x0*0x1*60x+2,en-us,"));
    }
}