package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for Net protocol decoder.
 * 
 * This test is designed to work in both monolithic and microservices architectures.
 * For microservices testing, set system property "test.microservices=true".
 */
public class NetProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new NetProtocolDecoder(null));

        verifyPosition(decoder, text(
                "@L03686090604017761712271020161807037078881037233751000000010F850036980A4000"));

        verifyPosition(decoder, text(
                "@L0368609060401776171223102005072803703296103721462100008009000000300B12B000"));

    }

    /**
     * Test for message broker integration.
     * This test is only enabled when running in microservices mode.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // This test would be implemented when the Protocol Service is ready
        // It would verify that decoded positions are properly published to the message broker
        
        // Example implementation (commented out until Protocol Service is ready):
        /*
        // Setup message broker test fixture
        var messageBrokerMock = new MessageBrokerMock();
        
        // Create and configure decoder with message broker
        var decoder = new NetProtocolDecoder(null);
        decoder.setMessageBroker(messageBrokerMock);
        inject(decoder);
        
        // Process a message
        var rawMessage = text("@L03686090604017761712271020161807037078881037233751000000010F850036980A4000");
        var position = decoder.decode(null, null, rawMessage);
        
        // Verify position was published to message broker
        CompletableFuture<Position> publishedPosition = messageBrokerMock.getLastPublishedPosition();
        Position result = publishedPosition.get(5, TimeUnit.SECONDS);
        
        assertNotNull(result);
        assertEquals(position.getDeviceId(), result.getDeviceId());
        assertEquals(position.getLatitude(), result.getLatitude(), 0.0001);
        assertEquals(position.getLongitude(), result.getLongitude(), 0.0001);
        */
    }

    /**
     * Test for cross-service boundary handling.
     * This test is only enabled when running in microservices mode.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        // This test would be implemented when the Protocol Service is ready
        // It would verify that the protocol decoder correctly interacts with other services
        
        // Example implementation (commented out until Protocol Service is ready):
        /*
        // Setup service mocks
        var positionServiceMock = new PositionServiceMock();
        var deviceServiceMock = new DeviceServiceMock();
        
        // Create and configure decoder with service clients
        var decoder = new NetProtocolDecoder(null);
        decoder.setPositionService(positionServiceMock);
        decoder.setDeviceService(deviceServiceMock);
        inject(decoder);
        
        // Configure device service mock to return a device
        deviceServiceMock.addDevice("036860906040177617", 1L);
        
        // Process a message
        var rawMessage = text("@L03686090604017761712271020161807037078881037233751000000010F850036980A4000");
        var position = decoder.decode(null, null, rawMessage);
        
        // Verify position was sent to position service
        CompletableFuture<Position> processedPosition = positionServiceMock.getLastProcessedPosition();
        Position result = processedPosition.get(5, TimeUnit.SECONDS);
        
        assertNotNull(result);
        assertEquals(position.getDeviceId(), result.getDeviceId());
        assertEquals(position.getLatitude(), result.getLatitude(), 0.0001);
        assertEquals(position.getLongitude(), result.getLongitude(), 0.0001);
        */
    }
}