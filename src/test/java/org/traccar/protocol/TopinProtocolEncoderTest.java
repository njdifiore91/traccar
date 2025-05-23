package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

/**
 * Test case for Topin protocol encoder.
 * 
 * This test has been updated to support both monolithic and microservices testing.
 * It will be gradually migrated to the Protocol Service test folder as part of the
 * microservices architecture transition.
 */
public class TopinProtocolEncoderTest extends ProtocolTest {

    /**
     * Tests the encoding of a command in the monolithic architecture.
     * This test will continue to work during the transition to microservices.
     */
    @Test
    public void testEncode() throws Exception {
        var encoder = inject(new TopinProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_SOS_NUMBER);
        command.set(Command.KEY_INDEX, 1);
        command.set(Command.KEY_PHONE, "13533333333");

        verifyCommand(encoder, command, binary("78780C4131333533333333333333330D0A"));
    }

    /**
     * Tests the encoding of a command in the microservices architecture.
     * This test will only run when the 'test.microservice' system property is set to 'true'.
     * 
     * In the microservices architecture, the protocol encoder needs to produce messages
     * that can be sent across service boundaries via a message broker.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservice", matches = "true")
    public void testEncodeWithMessageBroker() throws Exception {
        var encoder = inject(new TopinProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_SOS_NUMBER);
        command.set(Command.KEY_INDEX, 1);
        command.set(Command.KEY_PHONE, "13533333333");

        // Verify the command encoding produces the expected binary output
        verifyCommand(encoder, command, binary("78780C4131333533333333333333330D0A"));

        // In a microservices environment, we would also verify that the command
        // is properly published to the message broker and can be consumed by
        // other services. This part of the test will be implemented as the
        // message broker integration is developed.
    }
}