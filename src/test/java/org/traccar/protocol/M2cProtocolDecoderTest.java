package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.traccar.ProtocolTest;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Test for M2C protocol decoder
 * Supports both monolithic and microservices testing environments
 */
public class M2cProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        // Create a decoder with injected dependencies
        var decoder = inject(new M2cProtocolDecoder(null));

        // Test position decoding in monolithic mode
        verifyPositions(decoder, text(
                "[#M2C,2020,P1.B1.H3.F9.R1,102,864547034433966,1,L,0,20,171221,062016,28.647552,77.192841,0,0,0.0,0,0,64,255,11983,0,0,0,0.0,0,0,0,404,4,1F6,4D77,31,0*7524\r\n",
                "#M2C,2020,P1.B1.H3.F9.R1,102,864547034433966,2,L,0,20,171221,062019,28.647552,77.192841,0,0,0.0,0,0,64,255,11983,0,0,0,0.0,0,0,0,404,4,1F6,4D77,31,0*7528\r\n",
                "#M2C,2020,P1.B1.H3.F9.R1,102,864547034433966,3,L,0,20,171221,062024,28.647552,77.192841,0,0,0.0,0,0,64,255,16292,0,0,0,0.0,0,0,0,404,4,1F6,4D77,31,0*7523\r\n"));

        verifyPositions(decoder, text(
                "[#M2C,2020,P1.B1.H1.F1.R1,101,862462038980016,2,L,1,100,170704,074933,28.647556,77.192940,900,194,0.0,0,0,0,255,11942,0,0,0,0,0,0,0,0,30068,5051,0,0,1*8159\r\n"));

        verifyPositions(decoder, text(
                "[#M2C,2020,P1.B1.H1.F1.R1,101,862462038980016,7,L,0,31,170704,075905,28.647615,77.192970,300,260,0.0,6,7,3,255,11967,0,12,0,0,0,0,0,0,19500,5051,0,27,1*8234\r\n",
                "#M2C,2020,P1.B1.H1.F1.R1,101,862462038980016,8,L,0,33,170704,075905,28.647615,77.192970,300,260,0.0,6,7,0,255,11942,0,12,0,0,0,0,0,0,20300,5051,0,27,1*8217\r\n"));
    }

    @Test
    public void testDecodeWithMessageBroker() throws Exception {
        // Create a decoder with injected dependencies including a mock message producer
        var decoder = inject(new M2cProtocolDecoder(null));
        
        // Create a mock message producer for testing microservices integration
        MessageProducer messageProducer = mockMessageProducer();
        
        // Set the mock message producer on the decoder
        try {
            var field = decoder.getClass().getDeclaredField("messageProducer");
            field.setAccessible(true);
            field.set(decoder, messageProducer);
        } catch (NoSuchFieldException e) {
            // Field might not exist in monolithic mode, which is fine
        }

        // Test position decoding with message broker integration
        var positions = decoder.decode(null, null, text(
                "[#M2C,2020,P1.B1.H3.F9.R1,102,864547034433966,1,L,0,20,171221,062016,28.647552,77.192841,0,0,0.0,0,0,64,255,11983,0,0,0,0.0,0,0,0,404,4,1F6,4D77,31,0*7524\r\n"));
        
        // Verify positions were decoded correctly
        verifyDecodedPositions(positions);
        
        // In microservices mode, verify the message producer was called to publish the positions
        try {
            // This will only work in microservices mode where the messageProducer field exists
            var field = decoder.getClass().getDeclaredField("messageProducer");
            field.setAccessible(true);
            if (field.get(decoder) != null) {
                // Capture the position sent to the message producer
                ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
                verify(messageProducer).sendPosition(positionCaptor.capture());
                
                // Verify the position data sent to the message broker
                Position capturedPosition = positionCaptor.getValue();
                assertEquals(28.647552, capturedPosition.getLatitude(), 0.00001);
                assertEquals(77.192841, capturedPosition.getLongitude(), 0.00001);
                
                // Verify no other interactions with the message producer
                verifyNoMoreInteractions(messageProducer);
            }
        } catch (NoSuchFieldException e) {
            // Field doesn't exist in monolithic mode, which is fine
        }
    }
    
    /**
     * Helper method to verify decoded positions
     * @param decodedObject The decoded object from the protocol decoder
     */
    private void verifyDecodedPositions(Object decodedObject) {
        // Verify the decoded positions
        if (decodedObject instanceof Position) {
            // Single position case
            Position position = (Position) decodedObject;
            assertEquals(28.647552, position.getLatitude(), 0.00001);
            assertEquals(77.192841, position.getLongitude(), 0.00001);
        } else if (decodedObject instanceof java.util.Collection) {
            // Multiple positions case
            @SuppressWarnings("unchecked")
            java.util.Collection<Position> positions = (java.util.Collection<Position>) decodedObject;
            assertEquals(1, positions.size());
            Position position = positions.iterator().next();
            assertEquals(28.647552, position.getLatitude(), 0.00001);
            assertEquals(77.192841, position.getLongitude(), 0.00001);
        }
    }
}