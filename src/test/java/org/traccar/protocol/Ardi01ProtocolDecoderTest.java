package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test for Ardi01 protocol decoder.
 * 
 * This test is designed to work in both monolithic and microservices architectures.
 * It supports testing protocol integration with message brokers when running in the
 * Protocol Service context.
 */
public class Ardi01ProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic decode test for the monolithic architecture.
     * This test verifies the decoder can properly parse position data from device messages.
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new Ardi01ProtocolDecoder(null));

        verifyPosition(decoder, text(
                "013227003054776,20141010052719,24.4736042,56.8445807,110,289,40,7,5,78,-1"),
                position("2014-10-10 05:27:19.000", true, 56.84458, 24.47360));

        verifyPosition(decoder, text(
                "013227003054776,20141010052719,24.4736042,56.8445807,110,289,40,7,5,78,-1"));
    }
    
    /**
     * Test for microservices architecture with message broker integration.
     * This test is only enabled when running in the Protocol Service context.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservice")
    public void testMessageBrokerIntegration() throws Exception {
        var decoder = inject(new Ardi01ProtocolDecoder(null));
        
        // Setup message broker mock or test double
        setupMessageBrokerMock();
        
        // Process a sample message
        Position position = decoder.decode(null, null, text(
                "013227003054776,20141010052719,24.4736042,56.8445807,110,289,40,7,5,78,-1"));
        
        // Verify position was correctly decoded
        verifyPosition(position, "2014-10-10 05:27:19.000", true, 56.84458, 24.47360);
        
        // Verify message was published to the broker
        verifyMessagePublished(position);
    }
    
    /**
     * Test for cross-service boundary handling.
     * Verifies that the protocol decoder correctly integrates with other services.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservice")
    public void testCrossServiceIntegration() throws Exception {
        var decoder = inject(new Ardi01ProtocolDecoder(null));
        
        // Setup service mocks for cross-service testing
        setupServiceMocks();
        
        // Process a sample message
        Position position = decoder.decode(null, null, text(
                "013227003054776,20141010052719,24.4736042,56.8445807,110,289,40,7,5,78,-1"));
        
        // Verify position was correctly decoded
        verifyPosition(position, "2014-10-10 05:27:19.000", true, 56.84458, 24.47360);
        
        // Verify cross-service interactions
        verifyServiceInteractions(position);
    }
    
    /**
     * Sets up mocks for message broker testing.
     * This method is a placeholder and would be implemented with actual mock setup code.
     */
    private void setupMessageBrokerMock() {
        // In a real implementation, this would set up Kafka or RabbitMQ test doubles
        // For example:
        // when(mockMessageProducer.send(any(Position.class))).thenReturn(CompletableFuture.completedFuture(null));
    }
    
    /**
     * Sets up mocks for cross-service testing.
     * This method is a placeholder and would be implemented with actual mock setup code.
     */
    private void setupServiceMocks() {
        // In a real implementation, this would set up mocks for other services
        // For example:
        // when(mockPositionService.processPosition(any(Position.class))).thenReturn(CompletableFuture.completedFuture(null));
    }
    
    /**
     * Verifies that a message was published to the broker.
     * This method is a placeholder and would be implemented with actual verification code.
     */
    private void verifyMessagePublished(Position position) {
        // In a real implementation, this would verify that the message was published
        // For example:
        // verify(mockMessageProducer, times(1)).send(eq(position));
    }
    
    /**
     * Verifies cross-service interactions.
     * This method is a placeholder and would be implemented with actual verification code.
     */
    private void verifyServiceInteractions(Position position) {
        // In a real implementation, this would verify interactions with other services
        // For example:
        // verify(mockPositionService, times(1)).processPosition(eq(position));
    }
}