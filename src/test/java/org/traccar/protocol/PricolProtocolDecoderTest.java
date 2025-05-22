package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test for Pricol protocol decoder.
 * 
 * This test is designed to work in both monolithic and microservices environments.
 * It can be gradually migrated to the Protocol Service test folder.
 */
public class PricolProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new PricolProtocolDecoder(null));

        verifyPosition(decoder, binary(
                "3c5052493030303350020000011402110b222b0455152e4e001de819ca450000000000000003820249000000000000000000000000000000000000000040003e"));

        verifyNotNull(decoder, binary(
                "3c544553303030324b02000000000000000000000000000000000000000000000000000000037c01f4000000000000000000000000000000000000000000003e"));

        verifyPosition(decoder, binary(
                "3c4944303030303150FFFFFFFF1C050C121D38045D09FA4e001DE815F4452FFFFFFFFFFF03FF03FF03FF03FF03FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF113e"));
    }

    /**
     * Tests the decoder with message broker integration.
     * This test is only enabled in the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservice")
    public void testMessageBrokerIntegration() throws Exception {
        var decoder = inject(new PricolProtocolDecoder(null));
        
        // Configure message broker mock
        configureMessageBrokerMock();
        
        // Test decoding and verify message publication
        Position position = decoder.decode(null, null, binary(
                "3c5052493030303350020000011402110b222b0455152e4e001de819ca450000000000000003820249000000000000000000000000000000000000000040003e"));
        
        verifyPositionPublished(position);
    }

    /**
     * Tests protocol handling across service boundaries.
     * This test is only enabled in the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservice")
    public void testCrossServiceHandling() throws Exception {
        var decoder = inject(new PricolProtocolDecoder(null));
        
        // Configure service client mocks
        configureServiceClientMocks();
        
        // Test decoding and verify cross-service interactions
        Position position = decoder.decode(null, null, binary(
                "3c5052493030303350020000011402110b222b0455152e4e001de819ca450000000000000003820249000000000000000000000000000000000000000040003e"));
        
        verifyPositionProcessed(position);
    }
    
    /**
     * Configures the message broker mock for testing.
     * This method is a no-op in the monolithic environment.
     */
    private void configureMessageBrokerMock() {
        if (isMessageBrokerAvailable()) {
            // Configure message broker mock using the test utilities
            // This will be implemented when the message broker integration is added
            setupMessageProducerMock("positions");
        }
    }
    
    /**
     * Configures service client mocks for cross-service testing.
     * This method is a no-op in the monolithic environment.
     */
    private void configureServiceClientMocks() {
        if (isServiceClientAvailable()) {
            // Configure service client mocks using the test utilities
            // This will be implemented when the service clients are added
            setupPositionServiceClientMock();
        }
    }
    
    /**
     * Verifies that a position was published to the message broker.
     * This method is a no-op in the monolithic environment.
     */
    private void verifyPositionPublished(Position position) {
        if (isMessageBrokerAvailable()) {
            // Verify that the position was published to the message broker
            // This will be implemented when the message broker integration is added
            verifyMessagePublished("positions", position);
        }
    }
    
    /**
     * Verifies that a position was processed by the position service.
     * This method is a no-op in the monolithic environment.
     */
    private void verifyPositionProcessed(Position position) {
        if (isServiceClientAvailable()) {
            // Verify that the position was processed by the position service
            // This will be implemented when the service clients are added
            verifyPositionServiceProcessed(position);
        }
    }
    
    /**
     * Checks if the message broker is available for testing.
     * This method returns false in the monolithic environment.
     */
    private boolean isMessageBrokerAvailable() {
        return System.getProperty("test.environment", "").equals("microservice");
    }
    
    /**
     * Checks if the service clients are available for testing.
     * This method returns false in the monolithic environment.
     */
    private boolean isServiceClientAvailable() {
        return System.getProperty("test.environment", "").equals("microservice");
    }
}