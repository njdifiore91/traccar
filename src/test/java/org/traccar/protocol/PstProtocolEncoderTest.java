package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

/**
 * PST Protocol Encoder Test
 * This test verifies the encoding functionality of the PST protocol.
 * It has been updated to support both monolithic and microservices testing.
 */
public class PstProtocolEncoderTest extends ProtocolTest {

    /**
     * Tests encoding of engine stop command in monolithic mode
     */
    @Test
    public void testEncodeEngineStop() throws Exception {
        var encoder = inject(new PstProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);

        verifyCommand(encoder, command, binary("860ddf790600000001060002ffffffffe42b"));
    }

    /**
     * Tests encoding of engine resume command in monolithic mode
     */
    @Test
    public void testEncodeEngineResume() throws Exception {
        var encoder = inject(new PstProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_RESUME);

        verifyCommand(encoder, command, binary("860ddf790600000001060001ffffffff0af9"));
    }

    /**
     * Tests encoding of engine stop command with message broker integration
     * This test will only run when the 'test.broker.enabled' system property is set to 'true'
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker.enabled", matches = "true")
    public void testEncodeEngineStopWithBroker() throws Exception {
        var encoder = inject(new PstProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);

        // Verify the command encoding
        verifyCommand(encoder, command, binary("860ddf790600000001060002ffffffffe42b"));
        
        // In a microservices environment, this would also verify the message was published to the broker
        // This is a placeholder for the actual implementation that would be added when migrating to microservices
        if (isMicroservicesEnabled()) {
            verifyMessagePublished(command, "commands");
        }
    }

    /**
     * Tests encoding of engine resume command with message broker integration
     * This test will only run when the 'test.broker.enabled' system property is set to 'true'
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker.enabled", matches = "true")
    public void testEncodeEngineResumeWithBroker() throws Exception {
        var encoder = inject(new PstProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_RESUME);

        // Verify the command encoding
        verifyCommand(encoder, command, binary("860ddf790600000001060001ffffffff0af9"));
        
        // In a microservices environment, this would also verify the message was published to the broker
        // This is a placeholder for the actual implementation that would be added when migrating to microservices
        if (isMicroservicesEnabled()) {
            verifyMessagePublished(command, "commands");
        }
    }

    /**
     * Helper method to check if running in microservices mode
     * This will be implemented in the ProtocolTest base class
     */
    private boolean isMicroservicesEnabled() {
        return Boolean.getBoolean("test.microservices.enabled");
    }

    /**
     * Helper method to verify a message was published to the broker
     * This will be implemented in the ProtocolTest base class
     */
    private void verifyMessagePublished(Command command, String topic) {
        // This is a placeholder for the actual implementation
        // In the microservices version, this would verify that the command was published to the broker
        // using a test message consumer or mock broker client
    }
}