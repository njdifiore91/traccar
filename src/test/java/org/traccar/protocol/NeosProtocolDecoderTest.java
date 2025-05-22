package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

// Import for microservices testing
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Test for Neos Protocol Decoder
 * Updated to support both monolithic and microservices testing
 */
public class NeosProtocolDecoderTest extends ProtocolTest {

    /**
     * Original test for backward compatibility with monolithic architecture
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new NeosProtocolDecoder(null));

        verifyPosition(decoder, text(
                ">12345678,1,1,070201,144111,W05829.2613,S3435.2313,,00,034,25,00,126-000,0,3,11111111*2d!\r\n"));
    }

    /**
     * Test for microservices architecture with message broker integration
     * This test is only enabled when running in microservices mode
     */
    @Test
    @EnabledIfSystemProperty(named = "test.mode", matches = "microservices")
    public void testDecodeWithMessageBroker() throws Exception {
        // Create the decoder with a mock message broker client
        var mockMessageProducer = Mockito.mock(MessageProducer.class);
        var decoder = inject(new NeosProtocolDecoder(mockMessageProducer));
        
        // Set up the mock to capture the published position
        CompletableFuture<Position> positionFuture = new CompletableFuture<>();
        Mockito.doAnswer(invocation -> {
            Position position = invocation.getArgument(0);
            positionFuture.complete(position);
            return null;
        }).when(mockMessageProducer).publishPosition(Mockito.any(Position.class));
        
        // Decode the message
        decoder.decode(null, null, text(
                ">12345678,1,1,070201,144111,W05829.2613,S3435.2313,,00,034,25,00,126-000,0,3,11111111*2d!\r\n"));
        
        // Verify the position was published to the message broker
        Position position = positionFuture.get(1, TimeUnit.SECONDS);
        verifyPosition(position);
        
        // Verify the message producer was called exactly once
        Mockito.verify(mockMessageProducer, Mockito.times(1)).publishPosition(Mockito.any(Position.class));
    }
    
    /**
     * Test to verify protocol handling across service boundaries
     * This test simulates the complete flow from protocol decoding to position processing
     */
    @Test
    @EnabledIfSystemProperty(named = "test.mode", matches = "microservices")
    public void testProtocolAcrossServiceBoundaries() throws Exception {
        // Create mocks for cross-service communication
        var mockMessageProducer = Mockito.mock(MessageProducer.class);
        var mockPositionHandler = Mockito.mock(PositionHandler.class);
        
        // Create the decoder with the mock message producer
        var decoder = inject(new NeosProtocolDecoder(mockMessageProducer));
        
        // Set up the mocks to simulate the cross-service message flow
        CompletableFuture<Position> positionFuture = new CompletableFuture<>();
        
        // When the protocol service publishes a position
        Mockito.doAnswer(invocation -> {
            Position position = invocation.getArgument(0);
            // Simulate the message broker delivering to the position service
            mockPositionHandler.handlePosition(position);
            positionFuture.complete(position);
            return null;
        }).when(mockMessageProducer).publishPosition(Mockito.any(Position.class));
        
        // Decode the message
        decoder.decode(null, null, text(
                ">12345678,1,1,070201,144111,W05829.2613,S3435.2313,,00,034,25,00,126-000,0,3,11111111*2d!\r\n"));
        
        // Verify the position was processed through the entire service chain
        Position position = positionFuture.get(1, TimeUnit.SECONDS);
        verifyPosition(position);
        
        // Verify the position handler in the position service was called
        Mockito.verify(mockPositionHandler, Mockito.times(1)).handlePosition(Mockito.any(Position.class));
    }
    
    /**
     * Interface representing the message broker client used in microservices architecture
     */
    interface MessageProducer {
        void publishPosition(Position position);
    }
    
    /**
     * Interface representing the position handler in the position service
     */
    interface PositionHandler {
        void handlePosition(Position position);
    }
}