package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

// Additional imports for microservices testing
import org.traccar.helper.model.PositionUtil;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.test.MessageBrokerTest;
import org.traccar.test.TestDataFactory;

// Note: MessageBrokerTest and TestDataFactory are custom test utility classes
// that would be implemented as part of the microservices testing infrastructure.

/**
 * Test for Mictrack protocol decoder.
 * 
 * This test has been updated to support both monolithic and microservices testing environments.
 * It can be executed in the original monolithic environment and also in the Protocol Service.
 */
public class MictrackProtocolDecoderTest extends ProtocolTest {

    /**
     * Tests standard message decoding in both monolithic and microservices environments.
     * 
     * This test verifies the basic functionality of the Mictrack protocol decoder
     * by testing various message formats and ensuring they are correctly parsed.
     */
    @Test
    public void testDecodeStandard() throws Exception {

        var decoder = inject(new MictrackProtocolDecoder(null));

        verifyAttributes(decoder, text(
                "MT;5;867035041396795;Y1;220111085741+test,8c:53:c3:db:e7:26,-58,jiuide-842,80:26:89:f0:5e:4f,-74,jiu2ide 403,94:e4:4b:0a:31:08,-75,jiu3ide,7a:91:e9:50:26:0b,-85,CNet-9rNe,78:91:e9:40:26:0b,-87+0+4092+1"));

        verifyAttribute(decoder, text(
                "867035041390699 netlock=Success!"),
                Position.KEY_RESULT, "netlock=Success");

        verifyAttribute(decoder, text(
                "mode=Success!"),
                Position.KEY_RESULT, "mode=Success");

        verifyPosition(decoder, text(
                "MT;6;866425031361423;R0;10+190109091803+22.63827+114.02922+2.14+69+2+3744+113"),
                position("2019-01-09 09:18:03.000", true, 22.63827, 114.02922));

        verifyAttributes(decoder, text(
                "MT;6;866425031377981;R1;190108024848+6a:db:54:5a:79:6d,-91,00:9a:cd:a2:e6:21,-94+3+3831+0"));

        verifyAttributes(decoder, text(
                "MT;1;866425031379169;R2;181129081017+0,21681,20616,460+4+3976+0"));

        verifyAttributes(decoder, text(
                "MT;1;866425031379169;R3;181129081017+0,167910723,14924,460,176+4+3976+0"));

        verifyAttributes(decoder, text(
                "MT;6;866425031377981;R12;190108024848+6a:db:54:5a:79:6d,-91,00:9a:cd:a2:e6:21,-94+0,21681,20616,460+3+3831+0"));

        verifyAttributes(decoder, text(
                "MT;6;866425031377981;R13;190108024848+6a:db:54:5a:79:6d,-91,00:9a:cd:a2:e6:21,-94+0,167910723,14924,460,176+3+3831+0"));

        verifyAttributes(decoder, text(
                "MT;5;866425031379169;RH;5+190116112648+0+0+0+0+11+3954+1"));
    }

    /**
     * Tests decoding of messages with low altitude values.
     * 
     * This test verifies that the decoder correctly handles position messages
     * with low altitude values, which is a specific case for this protocol.
     */
    @Test
    public void testDecodeLowAltitude() throws Exception {

        var decoder = inject(new MictrackProtocolDecoder(null));

        verifyPositions(decoder, text(
                "861836051888035$162835.00,A,4139.6460,N,07009.7239,W,,41.53,-25.8,220621"));

        verifyPositions(decoder, text(
                "861108032038761$062232.00,A,2238.2832,N,11401.7381,E,0.01,309.62,95.0,131117"));

        verifyPositions(decoder, text(
                "861108032038761$062232.00,A,2238.2832,N,11401.7381,E,0.01,309.62,95.0,131117$062332.00,A,2238.2836,N,11401.7386,E,0.06,209.62,95.0,131117"));

        verifyPositions(decoder, text(
                "861108032038761$062232.00,A,2238.2832,N,11401.7381,E,0.01,309.62,95.0,131117"),
                position("2017-11-13 06:22:32.000", true, 22.63806, 114.028976));
    }

    /**
     * Tests protocol integration with message brokers in a microservices environment.
     * 
     * This test is only enabled when running in the microservices environment and verifies
     * that decoded positions are correctly published to the message broker.
     * 
     * Note: This test requires the MessageBrokerTest utility class which would be
     * implemented as part of the microservices testing infrastructure.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testMessageBrokerIntegration() throws Exception {
        // Create a message broker test helper
        MessageBrokerTest brokerTest = new MessageBrokerTest();
        
        // Create and configure the decoder with message broker integration
        Config config = new Config();
        config.setString(Keys.PROTOCOL_NAME.withPrefix("mictrack"), "mictrack");
        config.setBoolean(Keys.PROTOCOL_MESSAGE_BROKER_ENABLED.withPrefix("mictrack"), true);
        
        var decoder = inject(new MictrackProtocolDecoder(config));
        
        // Register a message listener with the broker test helper
        brokerTest.registerPositionListener();
        
        // Decode a position message
        decoder.decode(null, null, text(
                "MT;6;866425031361423;R0;10+190109091803+22.63827+114.02922+2.14+69+2+3744+113"));
        
        // Verify that the position was published to the message broker
        Position position = brokerTest.waitForPositionMessage(5000);
        assertNotNull(position);
        assertEquals(22.63827, position.getLatitude(), 0.0001);
        assertEquals(114.02922, position.getLongitude(), 0.0001);
        
        // Clean up resources
        brokerTest.cleanup();
    }

    /**
     * Tests cross-service boundary handling by verifying position enrichment.
     * 
     * This test is only enabled when running in the microservices environment and verifies
     * that positions are correctly processed across service boundaries with proper enrichment.
     * 
     * Note: This test requires the TestDataFactory utility class and PositionUtil.enrichPosition method
     * which would be implemented as part of the microservices testing infrastructure.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testCrossServiceBoundaries() throws Exception {
        // Create test device and position
        long deviceId = 123456;
        String uniqueId = "866425031361423";
        
        // Register the test device
        TestDataFactory.registerDevice(deviceId, uniqueId);
        
        // Create and configure the decoder
        Config config = new Config();
        config.setString(Keys.PROTOCOL_NAME.withPrefix("mictrack"), "mictrack");
        var decoder = inject(new MictrackProtocolDecoder(config));
        
        // Decode a position message
        Position position = decoder.decode(null, null, text(
                "MT;6;866425031361423;R0;10+190109091803+22.63827+114.02922+2.14+69+2+3744+113"));
        
        assertNotNull(position);
        assertEquals(deviceId, position.getDeviceId());
        
        // Verify position was enriched with additional data
        // This simulates what would happen when the position crosses service boundaries
        Position enrichedPosition = PositionUtil.enrichPosition(position);
        
        // Verify enrichment data
        assertNotNull(enrichedPosition.getAddress());
        assertTrue(enrichedPosition.hasAttribute(Position.KEY_GEOFENCE));
        
        // Clean up test data
        TestDataFactory.removeDevice(deviceId);
    }
}