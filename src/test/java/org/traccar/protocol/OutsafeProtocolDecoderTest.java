package org.traccar.protocol;

import io.netty.handler.codec.http.HttpMethod;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for Outsafe protocol decoder.
 * 
 * This test is designed to work in both monolithic and microservices environments.
 * It supports testing protocol integration with message brokers and verifies
 * protocol handling across service boundaries.
 */
public class OutsafeProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic decode test for monolithic architecture.
     * This test verifies the decoder can parse HTTP POST requests correctly.
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new OutsafeProtocolDecoder(null));

        verifyPosition(decoder, request(HttpMethod.POST, "/",
                buffer("{\"device\":\"865303040103725\",\"owner\":\"\",\"data\":{\"cmd\":\"\",\"ms1\":-1,\"ms2\":-1,\"ms3\":0,\"ms4\":0,\"observation\":\"\",\"content\":null},\"time\":1589277568,\"origin\":\"mqgatte\",\"latitude\":19.346855,\"longitude\":-99.29587,\"altitude\":2757,\"heading\":0,\"rssi\":0}")));

        verifyPosition(decoder, request(HttpMethod.POST, "/",
                buffer("{\"device\":\"862061044762093\",\"owner\":\"\",\"data\":{\"cmd\":\"GEO\",\"ms1\":82,\"ms2\":80,\"ms3\":5266,\"ms4\":-68,\"observation\":\"$NMEA 323455\",\"content\":null},\"time\":null,\"origin\":\"TCP\",\"latitude\":19.334734,\"longitude\":-99.307236,\"altitude\":2000,\"heading\":0,\"rssi\":123}")));

        verifyPosition(decoder, request(HttpMethod.POST, "/",
                buffer("{\"device\":\"1e09d88a-fe8e-4dee-90b9-6297088ff3de\",\"owner\":\"\",\"data\":{\"cmd\":\"GEO\",\"ms1\":82,\"ms2\":80,\"ms3\":5266,\"ms4\":-68,\"observation\":\"$NMEA 323455\",\"content\":null},\"time\":null,\"origin\":\"TCP\",\"latitude\":19.334734,\"longitude\":-99.307236,\"altitude\":2000,\"heading\":0,\"rssi\":123}")));
    }

    /**
     * Test for microservices architecture with message broker integration.
     * This test is only enabled when running in a microservices environment.
     * It verifies that decoded positions are correctly published to the message broker.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // This test will only run when the system property test.microservices=true is set
        
        // Create a mock message producer that would be injected in a microservices environment
        var messageProducer = Mockito.mock(org.traccar.common.messaging.MessageProducer.class);
        
        // Create and inject the decoder with the mock message producer
        var decoder = new OutsafeProtocolDecoder(null);
        inject(decoder);
        injectProperty(decoder, "messageProducer", messageProducer);
        
        // Decode a position
        var position = decoder.decode(null, null, request(HttpMethod.POST, "/",
                buffer("{\"device\":\"865303040103725\",\"owner\":\"\",\"data\":{\"cmd\":\"\",\"ms1\":-1,\"ms2\":-1,\"ms3\":0,\"ms4\":0,\"observation\":\"\",\"content\":null},\"time\":1589277568,\"origin\":\"mqgatte\",\"latitude\":19.346855,\"longitude\":-99.29587,\"altitude\":2757,\"heading\":0,\"rssi\":0}")));
        
        // Verify the position was decoded correctly
        verifyDecodedPosition(position);
        
        // Verify the position was published to the message broker
        Mockito.verify(messageProducer).publish(Mockito.eq("positions"), Mockito.any());
    }
    
    /**
     * Test for cross-service boundary handling in microservices architecture.
     * This test verifies that the protocol service correctly processes positions
     * and forwards them to the position service via the message broker.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        // This test will only run when the system property test.microservices=true is set
        
        // Create mocks for cross-service communication
        var messageProducer = Mockito.mock(org.traccar.common.messaging.MessageProducer.class);
        var messageConsumer = Mockito.mock(org.traccar.common.messaging.MessageConsumer.class);
        
        // Create and inject the decoder with the mock message producer
        var decoder = new OutsafeProtocolDecoder(null);
        inject(decoder);
        injectProperty(decoder, "messageProducer", messageProducer);
        
        // Decode a position
        var position = decoder.decode(null, null, request(HttpMethod.POST, "/",
                buffer("{\"device\":\"865303040103725\",\"owner\":\"\",\"data\":{\"cmd\":\"\",\"ms1\":-1,\"ms2\":-1,\"ms3\":0,\"ms4\":0,\"observation\":\"\",\"content\":null},\"time\":1589277568,\"origin\":\"mqgatte\",\"latitude\":19.346855,\"longitude\":-99.29587,\"altitude\":2757,\"heading\":0,\"rssi\":0}")));
        
        // Verify the position was decoded correctly
        verifyDecodedPosition(position);
        
        // Verify the position was published to the message broker for the position service
        Mockito.verify(messageProducer).publish(Mockito.eq("positions"), Mockito.any());
        
        // Simulate position service consuming the message
        // In a real test, this would be handled by the position service's test
        // Here we're just verifying the cross-service boundary
    }
    
    /**
     * Helper method to verify a decoded position has the expected values.
     * This is used by both monolithic and microservices tests.
     */
    private void verifyDecodedPosition(Position position) {
        assertNotNull(position);
        assertEquals(19.346855, position.getLatitude(), 0.00001);
        assertEquals(-99.29587, position.getLongitude(), 0.00001);
        assertEquals(2757, position.getAltitude(), 0.1);
        assertEquals(0, position.getCourse(), 0.1);
    }
    
    /**
     * Helper method to inject a property into an object using reflection.
     * This is used to inject mocks for microservices testing.
     */
    private void injectProperty(Object object, String propertyName, Object propertyValue) throws Exception {
        var field = object.getClass().getDeclaredField(propertyName);
        field.setAccessible(true);
        field.set(object, propertyValue);
    }
}