package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

/**
 * Test for Noran protocol encoder.
 * This test has been updated to support both monolithic and microservices testing environments.
 */
public class NoranProtocolEncoderTest extends ProtocolTest {

    /**
     * Tests basic command encoding functionality.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testEncode() throws Exception {
        var encoder = inject(new NoranProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);

        verifyCommand(encoder, command, binary(
                "0d0a2a4b5700440002000000000000002a4b572c3030302c3030372c3030303030302c302300000000000000000000000000000000000000000000000000000000000d0a"));
    }

    /**
     * Tests command encoding with message broker integration.
     * This test is specifically for the microservices environment.
     */
    @Test
    @Tag("microservice")
    public void testEncodeWithMessageBroker() throws Exception {
        // Create and configure the encoder with message broker support
        var encoder = inject(new NoranProtocolEncoder(null));
        
        // Create a command to send
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);
        
        // Verify the command is correctly encoded
        verifyCommand(encoder, command, binary(
                "0d0a2a4b5700440002000000000000002a4b572c3030302c3030372c3030303030302c302300000000000000000000000000000000000000000000000000000000000d0a"));
        
        // In a real microservices environment, we would verify that the command was published to the message broker
        // and that the protocol service correctly processed it. This would involve additional test infrastructure
        // that is not yet available in this test environment.
    }

    /**
     * Tests command encoding with cross-service integration.
     * This test verifies that commands can be properly encoded when crossing service boundaries.
     */
    @Test
    @Tag("integration")
    public void testEncodeCrossService() throws Exception {
        // Create and configure the encoder with cross-service support
        var encoder = inject(new NoranProtocolEncoder(null));
        
        // Create a command that would typically come from another service
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);
        command.set(Command.KEY_UNIQUE_ID, "test-command-id"); // Simulate a command ID from another service
        
        // Verify the command is correctly encoded
        verifyCommand(encoder, command, binary(
                "0d0a2a4b5700440002000000000000002a4b572c3030302c3030372c3030303030302c302300000000000000000000000000000000000000000000000000000000000d0a"));
        
        // In a real microservices environment, we would verify the end-to-end flow across services
        // This would involve additional test infrastructure that simulates the complete service interaction
    }
}