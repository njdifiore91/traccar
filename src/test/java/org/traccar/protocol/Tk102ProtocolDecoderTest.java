package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

// Import for message broker testing
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;

// Import for cross-service testing
import org.mockito.Mockito;
import java.util.concurrent.CompletableFuture;

/**
 * Test for Tk102 Protocol Decoder
 * 
 * This test class supports both monolithic and microservices testing contexts.
 * It can be run in the traditional monolithic environment or as part of the
 * Protocol Service in the microservices architecture.
 */
public class Tk102ProtocolDecoderTest extends ProtocolTest {

    /**
     * Simple interface for the Position Service Client.
     * This would be implemented in the microservices architecture to handle
     * communication between the Protocol Service and Position Service.
     */
    interface PositionServiceClient {
        /**
         * Sends a position to the Position Service.
         * @param position The position to send
         * @return A future that completes when the position has been processed
         */
        CompletableFuture<Boolean> sendPosition(Position position);
    }

    /**
     * Traditional decoder test that works in both monolithic and microservices contexts
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new Tk102ProtocolDecoder(null));

        verifyNull(decoder, buffer(
                "[\u00800000000000\u000821315452]"));

        verifyNull(decoder, buffer(
                "[\u00f00000000000\u000821315452]"));

        verifyPosition(decoder, buffer(
                "[\u00900100100001\u0036(ONE025857A2232.0729N11356.0030E000.02109110100000000)]"));

        verifyPosition(decoder, buffer(
                "[\u00900100100001\u0036(ITV025857A2232.0729N11356.0030E000.02109110100000000)]"));

        verifyNull(decoder, buffer(
                "[\u00210000000081\u0072(353327023367238,TK102-W998_01_V1.1.001_130219,255,001,255,001,0,100,100,0,internet,0000,0000,0,0,255,0,4,1,11,00)]"));
        
        verifyNull(decoder, buffer(
                "[\u004c0000001323\u004e(GSM,0,0,07410001,20120101162600,404,010,9261,130,0,2353,130,35,9263,130,33,1)]"));

        verifyNull(decoder, buffer(
                "[\u00250000000082\u001d(100100000000000600-30-65535)]"));

        verifyNull(decoder, buffer(
                "[\u00230000000004\u0018(062100000000000600-0-0)]"));

        verifyPosition(decoder, buffer(
                "[\u003d0000000083\u0036(ITV013939A4913.8317N02824.9241E000.90018031310010000)]"));
        
        verifyPosition(decoder, buffer(
                "[\u003d0000000036\u0036(ITV012209A4913.8281N02824.9258E000.32018031310010000)]"));
        
        verifyPosition(decoder, buffer(
                "[\u003b0000000010\u0036(ONE200834A5952.8114N01046.0832E003.93212071305010000)]"));

        verifyPosition(decoder, buffer(
                "[\u00930000000000\u0046(ITV153047A1534.0805N03233.0888E000.00029041500000400&Wsz-wl001&B0000)]"));
    }

    /**
     * Test for message broker integration
     * Only runs when the 'test.broker' system property is set to 'true'
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker", matches = "true")
    @SpringBootTest
    @EmbeddedKafka(partitions = 1, topics = {"positions"})
    @DirtiesContext
    @ActiveProfiles("test")
    public void testMessageBrokerIntegration() throws Exception {
        // This test verifies that decoded positions are properly published to the message broker
        
        // Mock the Kafka template that would be injected in a real microservices environment
        KafkaTemplate<String, Position> kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        Mockito.when(kafkaTemplate.send(Mockito.anyString(), Mockito.any(Position.class)))
               .thenReturn(CompletableFuture.completedFuture(null));
        
        // Create a decoder with the mocked Kafka template
        var decoder = new Tk102ProtocolDecoder(null);
        
        // Inject dependencies including our mocked Kafka template
        inject(decoder, "positionPublisher", kafkaTemplate);
        
        // Decode a valid position message
        decoder.decode(null, null, buffer(
                "[\u00900100100001\u0036(ONE025857A2232.0729N11356.0030E000.02109110100000000)]"));
        
        // Verify that the position was published to the Kafka topic
        Mockito.verify(kafkaTemplate).send(Mockito.eq("positions"), Mockito.any(Position.class));
    }

    /**
     * Test for cross-service boundary handling
     * Only runs when the 'test.crossservice' system property is set to 'true'
     */
    @Test
    @EnabledIfSystemProperty(named = "test.crossservice", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        // This test verifies that the protocol decoder properly interacts with other services
        
        // Mock the position service client that would be used in a microservices environment
        PositionServiceClient positionClient = Mockito.mock(PositionServiceClient.class);
        Mockito.when(positionClient.sendPosition(Mockito.any(Position.class)))
               .thenReturn(CompletableFuture.completedFuture(true));
        
        // Create a decoder with the mocked position service client
        var decoder = new Tk102ProtocolDecoder(null);
        
        // Inject dependencies including our mocked position service client
        inject(decoder, "positionServiceClient", positionClient);
        
        // Decode a valid position message
        decoder.decode(null, null, buffer(
                "[\u00900100100001\u0036(ONE025857A2232.0729N11356.0030E000.02109110100000000)]"));
        
        // Verify that the position was sent to the position service
        Mockito.verify(positionClient).sendPosition(Mockito.any(Position.class));
    }
}