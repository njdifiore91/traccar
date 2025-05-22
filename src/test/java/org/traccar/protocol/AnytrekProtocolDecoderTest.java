package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.Assumptions;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test case for Anytrek Protocol Decoder.
 * This test supports both monolithic and microservices architecture.
 * 
 * In microservices mode, it verifies:
 * - Basic protocol decoding functionality
 * - Message broker integration for position publishing
 * - Cross-service communication through service discovery
 * 
 * Note: The microservices-specific test methods require additional support methods
 * in the ProtocolTest class that are only available in the microservices version:
 * - assumeMessageBrokerAvailable()
 * - setupMessageBrokerMock()
 * - verifyMessagePublished(Position, String)
 * - assumeServiceDiscoveryAvailable()
 * - setupServiceDiscoveryMock()
 * - verifyPositionProcessed(Position)
 */
public class AnytrekProtocolDecoderTest extends ProtocolTest {

    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new AnytrekProtocolDecoder(null));

        verifyPosition(decoder, binary(
                "78783500300086428703204121160085015111050C0A0D20C6FD24A102FF8EAC0C01001404000000FFFFFFFF131702210000000000000000000D0A"));

        verifyPosition(decoder, binary(
                "787835003000867279033457792c009801001209080a3408c81b2a7d0305b88b0c00001ccb0000000f00000002b90174f30b000000000000000d0a"));
    }

    /**
     * Test message broker integration for position publishing.
     * This test verifies that decoded positions are properly published to the message broker.
     * 
     * The test is only enabled when running in microservices mode with the appropriate system property set.
     * It uses a mock message broker to verify that positions are correctly published to the "positions" topic.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices.enabled", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // Skip test if message broker is not available in the test environment
        assumeMessageBrokerAvailable();
        
        var decoder = inject(new AnytrekProtocolDecoder(null));
        
        // Setup mock message broker to capture published messages
        // In microservices mode, this configures a mock that intercepts messages
        // sent to the broker without actually sending them
        setupMessageBrokerMock();
        
        Position position = (Position) decoder.decode(null, null, binary(
                "78783500300086428703204121160085015111050C0A0D20C6FD24A102FF8EAC0C01001404000000FFFFFFFF131702210000000000000000000D0A"));
        
        // Verify the position was published to the correct topic with the expected payload
        // This checks that the protocol decoder properly published the decoded position
        // to the message broker with the correct topic and content
        verifyMessagePublished(position, "positions");
    }
    
    /**
     * Test cross-service boundary handling.
     * This test verifies that the protocol decoder can properly handle messages
     * that need to be processed across service boundaries.
     * 
     * The test is only enabled when running in microservices mode with the appropriate system property set.
     * It uses service discovery mocks to simulate the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices.enabled", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        // Skip test if service discovery is not available in the test environment
        assumeServiceDiscoveryAvailable();
        
        var decoder = inject(new AnytrekProtocolDecoder(null));
        
        // Setup mock service discovery to simulate the microservices environment
        // This configures service discovery to return mock services for cross-service communication
        // without requiring actual service instances to be running
        setupServiceDiscoveryMock();
        
        Position position = (Position) decoder.decode(null, null, binary(
                "787835003000867279033457792c009801001209080a3408c81b2a7d0305b88b0c00001ccb0000000f00000002b90174f30b000000000000000d0a"));
        
        // Verify the position was correctly processed across service boundaries
        // This checks that the protocol decoder properly communicated with other services
        // through the service discovery mechanism and processed the position correctly
        verifyPositionProcessed(position);
    }
}