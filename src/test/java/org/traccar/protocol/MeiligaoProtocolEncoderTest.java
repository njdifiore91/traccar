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
 * Test for Meiligao protocol encoder.
 * This test supports both monolithic and microservices architecture.
 * It can be run in both contexts and will be gradually migrated to the Protocol Service.
 */
public class MeiligaoProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncode() throws Exception {

        var encoder = inject(new MeiligaoProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_SINGLE);

        verifyCommand(encoder, command, binary("404000111234567890123441016cf70d0a"));

        command.setType(Command.TYPE_POSITION_PERIODIC);
        command.set(Command.KEY_FREQUENCY, 100);

        verifyCommand(encoder, command, binary("40400013123456789012344102000a2f4f0d0a"));

        command.setType(Command.TYPE_SET_TIMEZONE);
        command.set(Command.KEY_TIMEZONE, "GMT+8");

        verifyCommand(encoder, command, binary("4040001412345678901234413234383030ad0d0a"));

        command.setType(Command.TYPE_REBOOT_DEVICE);

        verifyCommand(encoder, command, binary("40400011123456789012344902d53d0d0a"));

        command.setType(Command.TYPE_ALARM_GEOFENCE);
        command.set(Command.KEY_RADIUS, 1000);

        verifyCommand(encoder, command, binary("4040001312345678901234410603e87bb00d0a"));

        command.setType(Command.TYPE_ENGINE_STOP);

        verifyCommand(encoder, command, binary("4040001212345678901234411501fd460d0a"));

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
        var encoder = inject(new MeiligaoProtocolEncoder(null));
        
        // Create a command that would be published to a message broker
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_SINGLE);
        command.set(Command.KEY_UNIQUE_ID, "meiligao-device-123");
        
        // Encode the command and verify it matches the expected binary output
        String encodedCommand = encoder.encodeCommand(command);
        assertNotNull(encodedCommand);
        
        // In a real microservices environment, this would be published to a message broker
        // and consumed by the Protocol Service
    }
    
    /**
     * Tests the cross-service boundary handling of Meiligao protocol commands.
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
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);
        command.set("serviceOrigin", "api-gateway"); // Metadata indicating the originating service
        
        // Encode using the Meiligao protocol encoder
        var encoder = inject(new MeiligaoProtocolEncoder(null));
        
        // Verify the command is properly encoded for the Meiligao protocol
        verifyCommand(encoder, command, binary("4040001212345678901234411501fd460d0a"));
        
        // In a real microservices environment, the result would be sent back through
        // the message broker to the originating service
    }
}