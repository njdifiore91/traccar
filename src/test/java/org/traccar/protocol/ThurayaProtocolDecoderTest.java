package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.ArgumentCaptor;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;
import org.traccar.session.DeviceSession;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ThurayaProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new ThurayaProtocolDecoder(null));

        verifyPositions(decoder, binary(
                "235400437101072bca3c0201348b9a00014c9f085493fc02200c5411470000000042323a300001348b9a00014d03085493ea02200c5010000000000042323a3000f2c1"));

        verifyPosition(decoder, binary(
                "2354002b5101072bca3c01348b9a00013fba000000000000000010000000000042323a3000174f4e00f9de"));

        verifyNull(decoder, binary(
                "235400d88115071e37d691030133342e3233362e3133302e3637000000001e56313030320030000700080102030405060708020101010101020201030103030302020000007800000078000004b000001c20050a64000015b3800015b374657374696e67003132333435360002010f28393031303539383938303134373738000043383a592c43373a592c43333a592c43323a592c43313a592c42353a592c42343a592c42323a592c42313a592c41323a592c41313a590045313a592c45373a590065746973616c61742e61650047455400322e3130d6de"));

    }

    /**
     * Test for microservices architecture with message broker integration.
     * This test verifies that the protocol decoder correctly publishes decoded positions
     * to the message broker and propagates correlation IDs across service boundaries.
     * 
     * Only runs when the 'test.microservices' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // Mock the message broker publisher
        var messagePublisher = mock(org.traccar.messaging.MessagePublisher.class);
        when(messagePublisher.publish(any(), any())).thenReturn(true);
        
        // Create a decoder with the mocked message publisher
        var decoder = new ThurayaProtocolDecoder(null);
        decoder.setMessagePublisher(messagePublisher);
        
        // Inject other dependencies
        inject(decoder);
        
        // Set up a device session with a correlation ID
        var deviceSession = mock(DeviceSession.class);
        when(deviceSession.getDeviceId()).thenReturn(1L);
        when(deviceSession.getCorrelationId()).thenReturn("test-correlation-id");
        decoder.setDeviceSession(deviceSession);
        
        // Decode a binary message
        var positions = decoder.decode(null, null, binary(
                "2354002b5101072bca3c01348b9a00013fba000000000000000010000000000042323a3000174f4e00f9de"));
        
        // Verify that the position was decoded correctly
        assertEquals(1, positions.size());
        
        // Verify that the position was published to the message broker
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher).publish(topicCaptor.capture(), positionCaptor.capture());
        
        // Verify the topic and position
        assertEquals("positions", topicCaptor.getValue());
        assertEquals(1L, positionCaptor.getValue().getDeviceId());
        
        // Verify that the correlation ID was propagated
        assertEquals("test-correlation-id", positionCaptor.getValue().getCorrelationId());
    }

    /**
     * Test for cross-service boundary handling.
     * This test verifies that the protocol decoder correctly handles messages
     * that need to be processed across service boundaries.
     * 
     * Only runs when the 'test.microservices' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        // Mock the service client for cross-service communication
        var serviceClient = mock(org.traccar.client.ServiceClient.class);
        when(serviceClient.sendRequest(any(), any())).thenReturn(true);
        
        // Create a decoder with the mocked service client
        var decoder = new ThurayaProtocolDecoder(null);
        decoder.setServiceClient(serviceClient);
        
        // Inject other dependencies
        inject(decoder);
        
        // Decode a binary message that requires cross-service processing
        var positions = decoder.decode(null, null, binary(
                "235400437101072bca3c0201348b9a00014c9f085493fc02200c5411470000000042323a300001348b9a00014d03085493ea02200c5010000000000042323a3000f2c1"));
        
        // Verify that multiple positions were decoded correctly
        assertEquals(2, positions.size());
        
        // Verify that the positions were sent to the position service
        ArgumentCaptor<String> serviceCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<List<Position>> positionsCaptor = ArgumentCaptor.forClass(List.class);
        verify(serviceClient).sendRequest(serviceCaptor.capture(), positionsCaptor.capture());
        
        // Verify the service name and positions list
        assertEquals("position-service", serviceCaptor.getValue());
        assertEquals(2, positionsCaptor.getValue().size());
    }
}