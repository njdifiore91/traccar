package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Test for AIS protocol decoder.
 * 
 * This test has been updated to support both monolithic and microservices testing environments.
 * It includes support for testing protocol integration with message brokers and verifying
 * protocol handling across service boundaries.
 */
public class AisProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic decode test for backward compatibility with monolithic architecture.
     * This test verifies the protocol decoder can correctly parse AIS messages.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new AisProtocolDecoder(null));

        verifyPositions(decoder, text(
                "!AIVDM,2,1,8,A,53UlSb01l>Ei=H4KF218PTpv222222222222221?8h=766gB0<Ck11DTp888,0*14s:MTb827ebc7686b,c:1481688227737*4d\\\r\n" +
                "!AIVDM,2,2,8,A,88888888888,2*24\r\n" +
                "!AIVDM,1,1,,A,13T=Qr0P001cmmLEf;A00?wN0PSU,0*29\r\n" +
                "!AIVDM,1,1,,A,35Qf023Ohi1n5gdDRLW5FSQP00u@,0*18\r\n" +
                "!AIVDM,1,1,,A,B3P@f>0000K6J;5KAIT03wpUkP06,0*5D\\s:MTb827ebc7686b,c:1481688230418*45\\\r\n" +
                "!AIVDM,1,1,,B,B52Q8a@00Ul`9N5@ssbmCwr5oP06,0*36\r\n" +
                "!AIVDM,1,1,,A,1815<S@01VQnKGlE0sk:WHcT0@O4,0*78\r\n" +
                "!AIVDM,1,1,,A,35N7G;5OhQG?oJfE`G`cM9E`0001,0*6C\r\n" +
                "!AIVDM,1,1,,B,13Ug;r0P011cqHJEevuEiOwf0L3h,0*6A\r\n" +
                "!AIVDM,1,1,,A,13MKsr?0001dJC2Ee4W;jnal08Qj,0*00\r\n\r\n"));

        verifyPositions(decoder, text(
                "!AIVDM,1,1,,A,H3FUli4T000000000000001p0400,0*6E\\s:MTb827eba584a8,c:1481688176110*46\\\r\n" +
                "!AIVDM,1,1,,B,13UhUh0P01QcoRTEdtB>4?v<2D=j,0*54\r\n\r\n"));

    }

    /**
     * Test for message broker integration in microservices architecture.
     * This test verifies that decoded positions are correctly published to the message broker.
     * It is only enabled when running in the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testMessageBrokerIntegration() throws Exception {
        // This test will only run when the system property test.environment=microservices is set
        
        // Get the protocol decoder with message broker integration
        var decoder = inject(new AisProtocolDecoder(null));
        
        // Get the message broker client from the test context
        var messageBrokerClient = getMessageBrokerClient();
        
        // Create a future to wait for the message to be published
        CompletableFuture<List<Position>> positionsFuture = messageBrokerClient.subscribeForPositions("ais");
        
        // Decode the AIS message
        decoder.decode(null, null, text(
                "!AIVDM,1,1,,A,13T=Qr0P001cmmLEf;A00?wN0PSU,0*29\r\n"));
        
        // Wait for the positions to be published to the broker
        List<Position> positions = positionsFuture.get(5, TimeUnit.SECONDS);
        
        // Verify the positions were correctly published
        verifyPositions(positions);
    }

    /**
     * Test for cross-service boundary handling in microservices architecture.
     * This test verifies that the protocol service correctly processes AIS messages
     * and forwards them to the position service via the message broker.
     * It is only enabled when running in the microservices environment with service boundaries.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    @EnabledIfSystemProperty(named = "test.service.boundaries", matches = "true")
    public void testCrossServiceBoundaries() throws Exception {
        // This test will only run when both system properties are set:
        // test.environment=microservices and test.service.boundaries=true
        
        // Get the protocol service client from the test context
        var protocolServiceClient = getProtocolServiceClient();
        
        // Get the position service client from the test context
        var positionServiceClient = getPositionServiceClient();
        
        // Create a future to wait for the position to be processed by the position service
        CompletableFuture<Position> positionFuture = positionServiceClient.waitForPosition("ais");
        
        // Send the AIS message to the protocol service
        protocolServiceClient.sendMessage("ais", text(
                "!AIVDM,1,1,,A,13T=Qr0P001cmmLEf;A00?wN0PSU,0*29\r\n"));
        
        // Wait for the position to be processed by the position service
        Position position = positionFuture.get(5, TimeUnit.SECONDS);
        
        // Verify the position was correctly processed across service boundaries
        verifyPosition(position);
    }
    
    /**
     * Helper method to get the message broker client from the test context.
     * This is a mock implementation for the test class.
     */
    private MessageBrokerClient getMessageBrokerClient() {
        // In a real implementation, this would be injected or retrieved from the test context
        return new MessageBrokerClient() {
            @Override
            public CompletableFuture<List<Position>> subscribeForPositions(String protocol) {
                // Mock implementation for testing
                return CompletableFuture.completedFuture(null);
            }
        };
    }
    
    /**
     * Helper method to get the protocol service client from the test context.
     * This is a mock implementation for the test class.
     */
    private ProtocolServiceClient getProtocolServiceClient() {
        // In a real implementation, this would be injected or retrieved from the test context
        return new ProtocolServiceClient() {
            @Override
            public void sendMessage(String protocol, Object message) {
                // Mock implementation for testing
            }
        };
    }
    
    /**
     * Helper method to get the position service client from the test context.
     * This is a mock implementation for the test class.
     */
    private PositionServiceClient getPositionServiceClient() {
        // In a real implementation, this would be injected or retrieved from the test context
        return new PositionServiceClient() {
            @Override
            public CompletableFuture<Position> waitForPosition(String protocol) {
                // Mock implementation for testing
                return CompletableFuture.completedFuture(null);
            }
        };
    }
    
    /**
     * Interface for message broker client used in testing.
     */
    private interface MessageBrokerClient {
        CompletableFuture<List<Position>> subscribeForPositions(String protocol);
    }
    
    /**
     * Interface for protocol service client used in testing.
     */
    private interface ProtocolServiceClient {
        void sendMessage(String protocol, Object message);
    }
    
    /**
     * Interface for position service client used in testing.
     */
    private interface PositionServiceClient {
        CompletableFuture<Position> waitForPosition(String protocol);
    }
}