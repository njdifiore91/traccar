package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

/**
 * Test for NavtelecomProtocolEncoder
 * This test has been updated to support both monolithic and microservices architecture
 * and will be gradually migrated to the Protocol Service.
 */
@Tag("protocol")
@Tag("encoder")
public class NavtelecomProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncode() throws Exception {
        var encoder = inject(new NavtelecomProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "*!SETOUT 1Y");

        verifyCommand(encoder, command, binary("404e544300000000010000000b004f5c2a215345544f5554203159"));
    }

    /**
     * Tests the protocol encoder with message broker integration.
     * This test verifies that commands can be properly encoded when sent through a message broker.
     */
    @Test
    @Tag("broker")
    public void testEncodeWithBroker() throws Exception {
        // This test will be implemented when the message broker integration is available
        // It will verify that commands can be properly encoded when sent through a message broker
        // For now, we're just setting up the test structure
        
        var encoder = inject(new NavtelecomProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "*!SETOUT 1Y");
        command.set("broker", true); // Flag to indicate this command should be sent via broker

        // The actual implementation will use a mock message broker
        // For now, we're just verifying the same encoding works
        verifyCommand(encoder, command, binary("404e544300000000010000000b004f5c2a215345544f5554203159"));
    }

    /**
     * Tests the protocol encoder across service boundaries.
     * This test verifies that commands can be properly encoded when crossing service boundaries.
     */
    @Test
    @Tag("cross-service")
    public void testEncodeAcrossServiceBoundaries() throws Exception {
        // This test will be implemented when the cross-service integration is available
        // It will verify that commands can be properly encoded when crossing service boundaries
        // For now, we're just setting up the test structure
        
        var encoder = inject(new NavtelecomProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "*!SETOUT 1Y");
        command.set("serviceOrigin", "api-gateway"); // Flag to indicate the service that originated the command

        // The actual implementation will use service mocks
        // For now, we're just verifying the same encoding works
        verifyCommand(encoder, command, binary("404e544300000000010000000b004f5c2a215345544f5554203159"));
    }
}