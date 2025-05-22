package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

// For microservices testing
import org.traccar.helper.DataConverter;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.Network;

import java.util.Date;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PositrexProtocolDecoderTest extends ProtocolTest {

    /**
     * Test basic decoding functionality in monolithic architecture
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new PositrexProtocolDecoder(null));

        verifyPosition(decoder, binary(
                "8280902e002b81c99fd607033905008b1c000003ae00003c9c000054ee00000079000000000d34d43f0fffffffda0000000000104fb80000204086464717807f8931082622128190980fffff862261047296590fffff"));
    }

    /**
     * Test protocol decoder with message broker integration
     * This test is only enabled when running in microservices mode
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testDecodeWithMessageBroker() throws Exception {
        // Create a mock message broker client
        var messageBrokerClient = mock(MessageBrokerClient.class);
        var latch = new CountDownLatch(1);
        
        // Configure the mock to capture the published message
        final Position[] capturedPosition = new Position[1];
        when(messageBrokerClient.publishPosition(any(Position.class)))
                .thenAnswer(invocation -> {
                    capturedPosition[0] = invocation.getArgument(0);
                    latch.countDown();
                    return null;
                });

        // Create and configure the decoder with message broker integration
        var config = new Config();
        config.setString(Keys.PROTOCOL_MESSAGE_BROKER_ENABLED.getKey(), "true");
        
        var decoder = new PositrexProtocolDecoder(null);
        decoder.setConfig(config);
        decoder = inject(decoder);
        
        // Set the message broker client
        decoder.setMessageBrokerClient(messageBrokerClient);

        // Decode the message
        var result = decoder.decode(null, null, binary(
                "8280902e002b81c99fd607033905008b1c000003ae00003c9c000054ee00000079000000000d34d43f0fffffffda0000000000104fb80000204086464717807f8931082622128190980fffff862261047296590fffff"));

        // Verify the result
        assertNotNull(result);
        verifyPosition(result);
        
        // Verify the message was published to the broker
        latch.await(1, TimeUnit.SECONDS);
        assertNotNull(capturedPosition[0]);
        assertEquals(((Position) result).getDeviceId(), capturedPosition[0].getDeviceId());
        assertEquals(((Position) result).getLatitude(), capturedPosition[0].getLatitude(), 0.0001);
        assertEquals(((Position) result).getLongitude(), capturedPosition[0].getLongitude(), 0.0001);
    }

    /**
     * Test cross-service protocol handling
     * This test verifies that the protocol decoder correctly handles messages across service boundaries
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testCrossServiceProtocolHandling() throws Exception {
        // Create a mock position service client
        var positionServiceClient = mock(PositionServiceClient.class);
        var latch = new CountDownLatch(1);
        
        // Configure the mock to capture the enriched position
        final Position[] capturedPosition = new Position[1];
        when(positionServiceClient.enrichPosition(any(Position.class)))
                .thenAnswer(invocation -> {
                    Position position = invocation.getArgument(0);
                    // Simulate position enrichment by the position service
                    position.set(Position.KEY_MOTION, true);
                    position.set(Position.KEY_GEOFENCE, "Test Geofence");
                    capturedPosition[0] = position;
                    latch.countDown();
                    return position;
                });

        // Create and configure the decoder with cross-service integration
        var config = new Config();
        config.setString(Keys.PROTOCOL_POSITION_SERVICE_ENABLED.getKey(), "true");
        
        var decoder = new PositrexProtocolDecoder(null);
        decoder.setConfig(config);
        decoder = inject(decoder);
        
        // Set the position service client
        decoder.setPositionServiceClient(positionServiceClient);

        // Decode the message
        var result = decoder.decode(null, null, binary(
                "8280902e002b81c99fd607033905008b1c000003ae00003c9c000054ee00000079000000000d34d43f0fffffffda0000000000104fb80000204086464717807f8931082622128190980fffff862261047296590fffff"));

        // Verify the result
        assertNotNull(result);
        verifyPosition(result);
        
        // Verify cross-service enrichment
        latch.await(1, TimeUnit.SECONDS);
        assertNotNull(capturedPosition[0]);
        assertEquals(true, capturedPosition[0].getAttributes().get(Position.KEY_MOTION));
        assertEquals("Test Geofence", capturedPosition[0].getAttributes().get(Position.KEY_GEOFENCE));
    }

    /**
     * Mock class for message broker client integration testing
     * This would be replaced by the actual implementation in the microservices architecture
     */
    private interface MessageBrokerClient {
        Object publishPosition(Position position);
    }

    /**
     * Mock class for position service client integration testing
     * This would be replaced by the actual implementation in the microservices architecture
     */
    private interface PositionServiceClient {
        Position enrichPosition(Position position);
    }
}