package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test for Niot Protocol Decoder
 * 
 * This test has been updated to support both monolithic and microservices testing approaches
 * as part of the gradual migration to the Protocol Service architecture.
 * 
 * The test supports:
 * 1. Basic protocol decoding in the monolithic architecture
 * 2. Message broker integration testing for the microservices architecture
 * 3. Cross-service protocol handling verification
 */
public class NiotProtocolDecoderTest extends ProtocolTest {
    
    /**
     * Basic protocol decoder test for monolithic architecture
     * This test verifies the basic functionality of the Niot protocol decoder
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new NiotProtocolDecoder(null));

        verifyPosition(decoder, binary(
                "585880004c08675430347318522007161451458024b28003f566ee00000328f8000748217ffc500729007a280000000000160001383932353430323130363431363738373136323100050002004e00570d"),
                position("2020-07-16 14:51:45.000", true, -1.33611, 36.89684));

        verifyPosition(decoder, binary(
                "585880004c08675430355777182005201100468024121b03f390ba00000105f8000b8d207ffc5f0f290084500000000000160001383932353430323130363431363839323430303700050002004e55940d"));

        verifyPosition(decoder, binary(
                "585880004C08640460465310081912101835080011679303C1E18F00400085F8014FBED87FFC4D15290085501A28000000160001383932353430323131313431323931333238343200050002004E55B40D"));
    }
    
    /**
     * Test for message broker integration in microservices architecture
     * This test verifies that the protocol decoder correctly publishes decoded positions to the message broker
     * Only runs when the 'test.broker.enabled' system property is set to 'true'
     * 
     * In the microservices architecture, the Protocol Service publishes decoded positions
     * to a message broker (Kafka/RabbitMQ) for consumption by other services.
     * This test ensures that the protocol decoder correctly integrates with the message broker.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker.enabled", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // Create a test message broker client
        var brokerClient = createMessageBrokerClient();
        
        // Create and configure the protocol decoder with broker integration
        var decoder = inject(new NiotProtocolDecoder(null));
        configureBrokerPublisher(decoder);
        
        // Process a test message
        Position position = (Position) decoder.decode(null, null, binary(
                "585880004c08675430347318522007161451458024b28003f566ee00000328f8000748217ffc500729007a280000000000160001383932353430323130363431363738373136323100050002004e00570d"));
        
        // Verify position was correctly decoded
        verifyPosition(position, position("2020-07-16 14:51:45.000", true, -1.33611, 36.89684));
        
        // Verify the message was published to the broker
        verifyMessagePublished(brokerClient, "positions", message -> {
            return message.containsKey("latitude") && 
                   message.containsKey("longitude") &&
                   message.containsKey("deviceId") &&
                   Math.abs((double) message.get("latitude") - (-1.33611)) < 0.00001 &&
                   Math.abs((double) message.get("longitude") - 36.89684) < 0.00001;
        });
    }
    
    /**
     * Test for cross-service protocol handling
     * This test verifies that the protocol decoder correctly handles messages across service boundaries
     * Only runs when the 'test.microservices.enabled' system property is set to 'true'
     * 
     * In the microservices architecture, the Protocol Service interacts with other services
     * such as the Position Service and Event Service. This test ensures that the protocol
     * decoder correctly handles these cross-service interactions and that position data
     * is properly passed between services.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices.enabled", matches = "true")
    public void testCrossServiceHandling() throws Exception {
        // Create test clients for dependent services
        var positionServiceClient = createPositionServiceClient();
        var eventServiceClient = createEventServiceClient();
        
        // Create and configure the protocol decoder with service clients
        var decoder = inject(new NiotProtocolDecoder(null));
        configureServiceClients(decoder, positionServiceClient, eventServiceClient);
        
        // Process a test message
        Position position = (Position) decoder.decode(null, null, binary(
                "585880004c08675430347318522007161451458024b28003f566ee00000328f8000748217ffc500729007a280000000000160001383932353430323130363431363738373136323100050002004e00570d"));
        
        // Verify position was correctly decoded
        verifyPosition(position, position("2020-07-16 14:51:45.000", true, -1.33611, 36.89684));
        
        // Verify position was sent to position service
        verifyPositionSent(positionServiceClient, position);
        
        // Verify event processing was triggered
        verifyEventProcessingTriggered(eventServiceClient, position);
    }
}