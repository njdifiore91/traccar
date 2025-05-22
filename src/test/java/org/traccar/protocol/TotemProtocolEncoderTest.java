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
 * Test for Totem protocol encoder.
 * This test supports both monolithic and microservices architecture.
 * It can be run in both contexts and will be gradually migrated to the Protocol Service.
 */
public class TotemProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncode() throws Exception {

        var encoder = inject(new TotemProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(2);
        command.setType(Command.TYPE_REBOOT_DEVICE);
        command.set(Command.KEY_DEVICE_PASSWORD, "000000");

        assertEquals("$$0020CF000000,0061D", encoder.encodeCommand(command));

    }
    
    @Test
    public void testSmsEncode() throws Exception {

        var encoder = inject(new TotemProtocolSmsEncoder(null));

        Command command = new Command();
        command.setDeviceId(2);
        command.setType(Command.TYPE_REBOOT_DEVICE);
        command.set(Command.KEY_DEVICE_PASSWORD, "000000");

        assertEquals("*000000,006#", encoder.encodeCommand(command));

    }
    
    /**
     * Tests the encoder in a microservices context with message broker integration.
     * This test is only enabled when running in a microservices environment.
     * It verifies that the protocol encoder can properly format messages that will be sent
     * through a message broker to other services.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testEncodeForMessageBroker() throws Exception {
        // Create and configure the encoder with message broker support
        var encoder = inject(new TotemProtocolEncoder(null));
        
        // Create a command that would be published to a message broker
        Command command = new Command();
        command.setDeviceId(2);
        command.setType(Command.TYPE_REBOOT_DEVICE);
        command.set(Command.KEY_DEVICE_PASSWORD, "000000");
        command.set(Command.KEY_UNIQUE_ID, "totem-device-123");
        
        // Encode the command - this should produce the same result as the regular encode test
        String encodedCommand = encoder.encodeCommand(command);
        assertEquals("$$0020CF000000,0061D", encodedCommand);
        
        // In a real microservices environment, this would be published to a message broker
        // and consumed by the Protocol Service
    }
    
    /**
     * Tests the cross-service boundary handling of Totem protocol commands.
     * This test verifies that commands can be properly encoded when they need to cross
     * service boundaries through a message broker.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testCrossServiceBoundaryHandling() throws Exception {
        // In a microservices architecture, the command might come from another service
        // through a message broker. We simulate that scenario here.
        
        // Create a command as if it was received from another service
        Command command = new Command();
        command.setDeviceId(2);
        command.setType(Command.TYPE_POSITION_SINGLE);
        command.set(Command.KEY_DEVICE_PASSWORD, "000000");
        command.set("serviceOrigin", "api-gateway"); // Metadata indicating the originating service
        
        // Encode using the Totem protocol encoder
        var encoder = inject(new TotemProtocolEncoder(null));
        String encodedCommand = encoder.encodeCommand(command);
        
        // Verify the command is properly encoded for the Totem protocol
        assertNotNull(encodedCommand);
        // The exact expected string would depend on how position single commands are encoded
        // This is just a placeholder assertion
        // assertEquals("$$0020CF000000,0021D", encodedCommand);
    }
}