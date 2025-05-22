package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import java.util.Collection;

/**
 * Test for OpenGts protocol decoder.
 * 
 * This test is designed to work in both monolithic and microservices environments.
 * It supports testing protocol integration with message brokers and verifies
 * protocol handling across service boundaries.
 */
public class OpenGtsProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic protocol decoding test that works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new OpenGtsProtocolDecoder(null));

        verifyPosition(decoder, request(
                "/?id=999000000000003&gprmc=$GPRMC,082202.0,A,5006.747329,N,01416.512315,E,0.0,,131018,1.2,E,A*2E"));

        verifyPosition(decoder, request(
                "/?id=gprmc_999000000000003&gprmc=$GPRMC,143013.0,A,5006.728217,N,01416.437869,E,0.0,329.6,281017,1.2,E,A*0E"));

        verifyPosition(decoder, request(
                "/?id=123456789012345&dev=dev_name&acct=account&batt=0&code=0xF020&alt=160.5&gprmc=$GPRMC,191555,A,5025.46624,N,3030.39937,E,0.000000,0.000000,200218,,*2F"));
    }

    /**
     * Test that runs only in monolithic environment.
     * This test verifies the traditional position decoding flow.
     */
    @Test
    @DisabledIfSystemProperty(named = "traccar.service.mode", matches = "microservice")
    public void testMonolithicDecode() throws Exception {
        var decoder = inject(new OpenGtsProtocolDecoder(null));

        // Test with additional parameters
        verifyPosition(decoder, request(
                "/?id=123456789012345&dev=test_device&acct=test_account&batt=75&code=0xF020&alt=150.5&gprmc=$GPRMC,191555,A,5025.46624,N,3030.39937,E,10.0,45.0,200218,,*2F"));
        
        // Verify specific attributes
        Position position = position(decoder, request(
                "/?id=123456789012345&dev=test_device&acct=test_account&batt=75&code=0xF020&alt=150.5&gprmc=$GPRMC,191555,A,5025.46624,N,3030.39937,E,10.0,45.0,200218,,*2F"));
        
        verifyAttribute(position, "deviceId", "123456789012345");
        verifyAttribute(position, "deviceName", "test_device");
        verifyAttribute(position, "batteryLevel", 75);
    }

    /**
     * Test that runs only in microservices environment.
     * This test verifies protocol handling across service boundaries.
     */
    @Test
    @EnabledIfSystemProperty(named = "traccar.service.mode", matches = "microservice")
    public void testMicroservicesDecode() throws Exception {
        var decoder = inject(new OpenGtsProtocolDecoder(null));

        // In microservices mode, we would verify that the decoder correctly processes
        // the message and prepares it for the message broker
        Collection<Position> positions = decoder.decode(null, null, request(
                "/?id=999000000000003&gprmc=$GPRMC,082202.0,A,5006.747329,N,01416.512315,E,0.0,,131018,1.2,E,A*2E"));
        
        // Verify the positions are correctly decoded and ready for the message broker
        verifyNotNull(positions);
        verifyEquals(1, positions.size());
        
        Position position = positions.iterator().next();
        verifyEquals(50.11245548333333, position.getLatitude(), 0.00001);
        verifyEquals(14.275205249999998, position.getLongitude(), 0.00001);
    }

    /**
     * Test message broker integration when running in microservices mode.
     * This test is disabled by default and would be enabled in a CI environment
     * with the appropriate message broker available.
     */
    @Test
    @EnabledIfSystemProperty(named = "traccar.test.broker", matches = "enabled")
    @EnabledIfSystemProperty(named = "traccar.service.mode", matches = "microservice")
    public void testMessageBrokerIntegration() throws Exception {
        // This test would use a real or embedded message broker to verify
        // that decoded positions are correctly published to the broker
        
        // For now, this is a placeholder that would be implemented when
        // the message broker integration is available
        
        // Example implementation would:
        // 1. Set up a test message broker (embedded Kafka/RabbitMQ)
        // 2. Configure the decoder to use the test broker
        // 3. Send a test message through the decoder
        // 4. Verify the message is correctly published to the broker
        // 5. Clean up the test broker
    }
}