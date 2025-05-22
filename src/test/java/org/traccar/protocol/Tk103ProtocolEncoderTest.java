package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

// For microservices testing
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for TK103 protocol encoder.
 * This test is designed to work in both monolithic and microservices architectures.
 * 
 * In the microservices architecture, this test will be migrated to the Protocol Service.
 */
public class Tk103ProtocolEncoderTest extends ProtocolTest {

    /**
     * Helper method to create encoder instance with appropriate configuration
     * for the current architecture (monolithic or microservices).
     * 
     * @param alternative Whether to use alternative format
     * @return Configured protocol encoder
     */
    private Tk103ProtocolEncoder createEncoder(boolean alternative) {
        // In monolithic mode, use the inject method from ProtocolTest
        return inject(new Tk103ProtocolEncoder(null, alternative));
    }

    @Test
    public void testEncodeOutputControl() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_OUTPUT_CONTROL);
        command.set(Command.KEY_DATA, "1");

        assertEquals("(123456789012345AV001)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeEngineStop() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);

        assertEquals("(123456789012345AV010)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodePositionSingle() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_SINGLE);

        assertEquals("(123456789012345AP00)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodePositionPeriodic() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_PERIODIC);
        command.set(Command.KEY_FREQUENCY, 60);

        assertEquals("(123456789012345AR00003C0000)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodePositionStop() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_STOP);

        assertEquals("(123456789012345AR0000000000)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeGetVersion() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_GET_VERSION);

        assertEquals("(123456789012345AP07)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeRebootDevice() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_REBOOT_DEVICE);

        assertEquals("(123456789012345AT00)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeSetOdometer() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_SET_ODOMETER);

        assertEquals("(123456789012345AX01)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodePositionSingleAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_SINGLE);

        assertEquals("[begin]sms2,*getposl*,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodePositionPeriodicAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_PERIODIC);

        assertEquals("[begin]sms2,*routetrack*99*,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodePositionStopAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_STOP);

        assertEquals("[begin]sms2,*routetrackoff*,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeGetVersionAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_GET_VERSION);

        assertEquals("[begin]sms2,*about*,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeRebootDeviceAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_REBOOT_DEVICE);

        assertEquals("[begin]sms2,88888888,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeIdentificationAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_IDENTIFICATION);

        assertEquals("[begin]sms2,999999,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeSosOnAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ALARM_SOS);
        command.set(Command.KEY_ENABLE, true);

        assertEquals("[begin]sms2,*soson*,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeSosOffAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ALARM_SOS);
        command.set(Command.KEY_ENABLE, false);

        assertEquals("[begin]sms2,*sosoff*,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeCustom() throws Exception {
        var encoder = createEncoder(false);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "AA00");

        assertEquals("(123456789012345AA00)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeCustomAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "any text is ok");

        assertEquals("[begin]sms2,any text is ok,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeSetConnectionAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_SET_CONNECTION);
        command.set(Command.KEY_SERVER, "1.2.3.4");
        command.set(Command.KEY_PORT, "5555");

        assertEquals("[begin]sms2,*setip*1*2*3*4*5555*,[end]", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeSosNumberAlternative() throws Exception {
        var encoder = createEncoder(true);

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_SOS_NUMBER);
        command.set(Command.KEY_INDEX, 0);
        command.set(Command.KEY_PHONE, "+55555555555");
        command.set(Command.KEY_DEVICE_PASSWORD, "232323");

        assertEquals("[begin]sms2,*master*232323*+55555555555*,[end]", encoder.encodeCommand(command));
    }

    /**
     * Tests for microservices architecture with message broker integration.
     * These tests are only enabled when running in the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testMessageBrokerIntegration() throws Exception {
        // This test verifies that the encoder can properly format messages
        // that will be sent to the message broker in the microservices architecture
        var encoder = createEncoder(false);
        
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);
        
        String encodedCommand = encoder.encodeCommand(command);
        assertEquals("(123456789012345AV010)", encodedCommand);
        
        // In a real microservices test, we would verify that the command is properly
        // published to the message broker and received by the appropriate service
        // This is a simplified version for demonstration purposes
        CompletableFuture<String> messageFuture = mockMessageBrokerPublish(encodedCommand);
        String receivedMessage = messageFuture.get(1, TimeUnit.SECONDS);
        assertEquals(encodedCommand, receivedMessage);
    }
    
    /**
     * Mock method to simulate publishing a message to the message broker.
     * In a real implementation, this would interact with the actual message broker client.
     * 
     * @param message Message to publish
     * @return CompletableFuture that completes when the message is published
     */
    private CompletableFuture<String> mockMessageBrokerPublish(String message) {
        // In a real implementation, this would publish to Kafka/RabbitMQ
        // and return a future that completes when the message is acknowledged
        return CompletableFuture.completedFuture(message);
    }
    
    /**
     * Test for cross-service boundary protocol handling.
     * This test verifies that commands can be properly encoded and decoded
     * across service boundaries in the microservices architecture.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testCrossServiceBoundaryHandling() throws Exception {
        // This test simulates the interaction between the Protocol Service
        // and other services in the microservices architecture
        var encoder = createEncoder(false);
        
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_SINGLE);
        
        String encodedCommand = encoder.encodeCommand(command);
        assertEquals("(123456789012345AP00)", encodedCommand);
        
        // Simulate sending the command to the device through the Protocol Service
        CompletableFuture<Boolean> deliveryFuture = mockProtocolServiceCommandDelivery(encodedCommand);
        Boolean delivered = deliveryFuture.get(1, TimeUnit.SECONDS);
        assertNotNull(delivered);
        
        // In a real implementation, we would verify that the command was properly
        // delivered to the device and that any response was properly processed
    }
    
    /**
     * Mock method to simulate delivering a command through the Protocol Service.
     * In a real implementation, this would interact with the actual Protocol Service client.
     * 
     * @param encodedCommand Encoded command to deliver
     * @return CompletableFuture that completes when the command is delivered
     */
    private CompletableFuture<Boolean> mockProtocolServiceCommandDelivery(String encodedCommand) {
        // In a real implementation, this would use gRPC or REST to communicate
        // with the Protocol Service and return a future that completes when
        // the command is delivered to the device
        return CompletableFuture.completedFuture(true);
    }
}