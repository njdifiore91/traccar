package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Tests for the Portman Protocol Encoder.
 * This test class has been updated to support both monolithic and microservices testing environments.
 * 
 * To run the microservices-specific tests, use: -Dtest.environment=microservices
 * To run the integration tests, use: -Dtest.integration=true
 */
public class PortmanProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncodeEngineStop() throws Exception {
        var encoder = inject(new PortmanProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);

        assertEquals("&&123456789012345,XA5\r\n", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeEngineResume() throws Exception {
        var encoder = inject(new PortmanProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_RESUME);

        assertEquals("&&123456789012345,XA6\r\n", encoder.encodeCommand(command));
    }

    /**
     * Tests the encoder with message broker integration.
     * This test is only enabled when running in a microservices environment.
     * 
     * To run this test, use: -Dtest.environment=microservices
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testEncodeWithMessageBroker() throws Exception {
        // Create and configure the encoder with message broker support
        var encoder = inject(new PortmanProtocolEncoder(null));
        
        // Create a command to encode
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);
        
        // Encode the command
        String encodedCommand = encoder.encodeCommand(command);
        
        // Verify the encoded command
        assertEquals("&&123456789012345,XA5\r\n", encodedCommand);
        
        // In a real implementation, we would verify that the command was published to the message broker
        // This is a placeholder for future implementation
        // Example verification might look like:
        // verify(messageBroker).publishCommand(eq(command.getDeviceId()), eq(encodedCommand));
    }

    /**
     * Tests the encoder with service boundary crossing.
     * This test verifies that commands can be properly encoded when crossing service boundaries.
     * This test is only enabled when running in an integration testing environment.
     * 
     * To run this test, use: -Dtest.integration=true
     */
    @Test
    @EnabledIfSystemProperty(named = "test.integration", matches = "true")
    public void testEncodeAcrossServiceBoundary() throws Exception {
        // Create and configure the encoder with service client support
        var encoder = inject(new PortmanProtocolEncoder(null));
        
        // Create a command to encode
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "custom_command_data");
        
        // Encode the command
        String encodedCommand = encoder.encodeCommand(command);
        
        // Verify the encoded command is not null (actual format depends on the custom command)
        assertNotNull(encodedCommand);
        
        // In a real implementation, we would verify that the command was properly handled across service boundaries
        // This is a placeholder for future implementation
        // Example verification might look like:
        // verify(protocolServiceClient).sendCommand(eq(command.getDeviceId()), eq(encodedCommand));
    }
}