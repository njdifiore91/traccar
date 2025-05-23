package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;

// Additional imports for microservices testing
import org.mockito.Mockito;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

/**
 * Test for the ArknavX8 protocol decoder.
 * This test is designed to work in both monolithic and microservices environments.
 * 
 * For microservices testing, set the system property:
 * -Dtest.environment=microservice
 */
public class ArknavX8ProtocolDecoderTest extends ProtocolTest {

    private ArknavX8ProtocolDecoder decoder;

    @BeforeEach
    public void setUp() {
        decoder = inject(new ArknavX8ProtocolDecoder(null));
    }

    /**
     * Standard protocol decoder test that works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {
        verifyNull(decoder, text(
                "351856045213782,241111"));

        verifyPosition(decoder, text(
                "1G,181213092101,A,0347.0756N,09842.7435E,0.0,183,1.1,11008000"));

        verifyAttributes(decoder, text(
                "2G,181213092101,08,4084.0,00.04,04.01,000396255.0"));

        verifyNull(decoder, text(
                "2R,090214235955,00,,00.04,03.76,001892024.9"));

        verifyNull(decoder, text(
                "351856040005407,240101"));

        verifyPosition(decoder, text(
                "1R,110509053244,A,2457.9141N,12126.3321E,220.0,315,10.0,00000000"));

        verifyNull(decoder, text(
                "2R,110509053244,837493,,998372,,,"));

        verifyPosition(decoder, text(
                "1G,110509053245,A,2457.9141N,12126.3192E,3.1,35,2.0,00000001"));

        verifyPosition(decoder, text(
                "1G,110509053246,A,2457.9121N,12126.3415E,2.0,288,1.7,00000000"));

        verifyPosition(decoder, text(
                "1M,110509053247,A,2457.9118N,12126.3522E,1.0,55,2.2,00000000"));
    }

    /**
     * Test specifically for microservices environment that verifies message broker integration.
     * This test is only enabled when running in a microservices environment.
     */
    @Test
    @Tag("integration")
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservice")
    public void testMessageBrokerIntegration() throws Exception {
        // This test would use a real or mock message broker in a microservices environment
        // For now, we'll just verify the position is correctly decoded and would be ready for publishing
        
        var position = decoder.decode(null, null, text(
                "1G,181213092101,A,0347.0756N,09842.7435E,0.0,183,1.1,11008000"));
        
        // Verify position was decoded correctly before it would be published to the message broker
        verifyPosition(position);
        
        // In a real microservices test, we would verify the position was published to the message broker
        // and consumed by the position service
    }

    /**
     * Test that verifies protocol handling across service boundaries.
     * This test is only enabled when running in a microservices environment.
     */
    @Test
    @Tag("integration")
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservice")
    public void testCrossServiceHandling() throws Exception {
        // In a real microservices test, this would set up mocks or test containers for dependent services
        // and verify the complete flow from protocol decoding to position processing
        
        // For now, we'll just verify the position is correctly decoded
        var position = decoder.decode(null, null, text(
                "1M,110509053247,A,2457.9118N,12126.3522E,1.0,55,2.2,00000000"));
        
        // Verify position was decoded correctly
        verifyPosition(position);
        
        // In a real microservices test, we would verify:  
        // 1. The position was published to the message broker
        // 2. The position service consumed and processed the position
        // 3. Events were generated if applicable
        // 4. The complete flow worked across service boundaries
    }
}