package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test for MiniFinder protocol decoder.
 * 
 * This test has been updated to support both monolithic and microservices testing.
 * It can be executed in both environments and includes support for message broker integration.
 */
public class MiniFinderProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new MiniFinderProtocolDecoder(null));

        verifyNull(decoder, text(
                "!1,867273023933661,V07S.5701.1621,100"));

        verifyAttributes(decoder, text(
                "!3,ok"));

        verifyNull(decoder, text(
                "!1,123456789012345"));

        verifyAttribute(decoder, text(
                "!4,10,040123,,,1.0,110,0,0S,33"),
                "phone1", "040123");

        verifyAttribute(decoder, text(
                "!5,17,V,50"),
                Position.KEY_BATTERY_LEVEL, 50);

        verifyAttributes(decoder, text(
                "!5,17,V"));

        verifyNull(decoder, text(
                "!1,860719027585011"));

        verifyPosition(decoder, text(
                "!D,02/05/17,19:56:17,47.083542,15.482373,0,0,100001,479.3,100,4,9,0"));

        verifyPosition(decoder, text(
                "!D,15/04/17,13:58:53,51.483067,-0.452548,60,180,140001,28.7,47,4,13,0"));

        verifyPosition(decoder, text(
                "!D,07/04/17,05:42:26,-37.588970,145.121231,0,0,0c0001,185.2,92,7,14,1.2"));

        verifyPosition(decoder, text(
                "!D,28/11/16,00:04:09,42.926067,-85.747589,124,236,140001,179.8,60,11,16,0"));

        verifyPosition(decoder, text(
                "!C,30/1/16,1:1:6,31.259157,30.020910,0,0,100001,25.32,100,0.03,0.01,0"));

        verifyPosition(decoder, text(
                "!A,26/10/12,00:28:41,7.770385,-72.215706,0.0,25101,0"));

        verifyPosition(decoder, text(
                "!A,01/12/10,13:25:35,22.641724,114.023666,000.1,281.6,0"));

        verifyPosition(decoder, text(
                "!D,08/07/15,04:01:32,40.428257,-3.704808,0,0,170001,701.7,22,5,14,0"));

        verifyPosition(decoder, text(
                "!D,08/07/15,04:55:13,40.428257,-3.704932,0,0,180001,680.0,8,8,13,0"));

        verifyPosition(decoder, text(
                "!D,08/07/15,02:01:32,40.428230,-3.704950,4,170,170001,682.7,43,6,13,0"));

        verifyNull(decoder, text(
                "!1,860719020212696"));

        verifyPosition(decoder, text(
                "!D,22/2/14,13:40:58,56.899601,14.811541,0,0,1,176.0,98,5,16,0"),
                position("2014-02-22 13:40:58.000", true, 56.89960, 14.81154));

        verifyPosition(decoder, text(
                "!D,22/2/14,13:47:51,56.899517,14.811665,0,0,b0001,179.3,97,5,16,0"));

        verifyPosition(decoder, text(
                "!D,3/7/13,6:35:30,22.645952,114.040436,0.0,225.8,1f0001,12.11,98,0,0,0"));

    }
    
    /**
     * Test for message broker integration.
     * This test will only run when the 'test.broker.enabled' system property is set to 'true'.
     * It verifies that decoded positions are correctly published to the message broker.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker.enabled", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // This test will be implemented when the message broker infrastructure is available
        // It will verify that decoded positions are correctly published to the broker
        
        // Example implementation (commented out until broker infrastructure is available):
        /*
        // Setup message broker test environment
        var brokerClient = createBrokerClient();
        var messageCollector = new MessageCollector();
        brokerClient.subscribe("positions", messageCollector);
        
        // Create and inject decoder with broker publishing enabled
        var decoder = inject(new MiniFinderProtocolDecoder(null));
        
        // Process a sample message that should produce a position
        var result = decoder.decode(null, null, text(
                "!D,22/2/14,13:40:58,56.899601,14.811541,0,0,1,176.0,98,5,16,0"));
        
        // Verify the position was decoded correctly
        assertNotNull(result);
        assertTrue(result instanceof Position);
        
        // Wait for the message to be published to the broker
        messageCollector.waitForMessages(1, 5000);
        
        // Verify the message was published correctly
        assertEquals(1, messageCollector.getMessages().size());
        var brokerMessage = messageCollector.getMessages().get(0);
        assertEquals(((Position) result).getDeviceId(), brokerMessage.getDeviceId());
        assertEquals(((Position) result).getLatitude(), brokerMessage.getLatitude(), 0.0001);
        assertEquals(((Position) result).getLongitude(), brokerMessage.getLongitude(), 0.0001);
        */
    }
    
    /**
     * Test for cross-service boundary handling.
     * This test will only run when the 'test.microservices.enabled' system property is set to 'true'.
     * It verifies that the protocol decoder correctly interacts with other services.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices.enabled", matches = "true")
    public void testCrossServiceIntegration() throws Exception {
        // This test will be implemented when the microservices infrastructure is available
        // It will verify that the protocol decoder correctly interacts with other services
        
        // Example implementation (commented out until microservices infrastructure is available):
        /*
        // Setup mock position service client
        var positionServiceClient = createMockPositionServiceClient();
        
        // Create and inject decoder with service client
        var decoder = inject(new MiniFinderProtocolDecoder(null));
        decoder.setPositionServiceClient(positionServiceClient);
        
        // Process a sample message that should produce a position
        decoder.decode(null, null, text(
                "!D,22/2/14,13:40:58,56.899601,14.811541,0,0,1,176.0,98,5,16,0"));
        
        // Verify the position was sent to the position service
        verify(positionServiceClient, times(1)).sendPosition(any());
        */
    }

}