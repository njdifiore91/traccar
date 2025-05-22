package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;
import org.traccar.model.Position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for PretraceProtocolEncoder.
 * <p>
 * This test class has been updated to support both monolithic and microservices architectures.
 * Tests with @EnabledIfSystemProperty(named = "test.mode", matches = "microservices") annotation
 * will only run when the system is in microservices mode.
 */
public class PretraceProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncodePositionPeriodic() throws Exception {
        var encoder = inject(new PretraceProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_PERIODIC);
        command.set(Command.KEY_FREQUENCY, 300);

        assertEquals("(123456789012345D221300,300,,^69)", encoder.encodeCommand(command));
    }

    @Test
    public void testEncodeCustom() throws Exception {
        var encoder = inject(new PretraceProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "D21012");

        assertEquals("(123456789012345D21012^44)", encoder.encodeCommand(command));
    }
    
    /**
     * Tests the encoder with message broker integration.
     * This test is only enabled when running in microservices mode.
     * <p>
     * In microservices architecture, the encoder should publish encoded commands
     * to a message broker for other services to consume.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.mode", matches = "microservices")
    public void testEncodeWithMessageBroker() throws Exception {
        // Set up mock message broker
        var messageBroker = mockMessageBroker();
        
        // Create encoder with message broker
        var encoder = inject(new PretraceProtocolEncoder(null));
        
        // Create command
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_PERIODIC);
        command.set(Command.KEY_FREQUENCY, 300);
        
        // Encode command
        String encodedCommand = encoder.encodeCommand(command);
        
        // Verify the encoded command
        assertEquals("(123456789012345D221300,300,,^69)", encodedCommand);
        
        // Verify message was published to the command topic
        verifyCommandPublished(messageBroker, command, encodedCommand);
    }
    
    /**
     * Tests the encoder with service client integration.
     * This test is only enabled when running in microservices mode.
     * <p>
     * In microservices architecture, the encoder may need to communicate with
     * other services to retrieve device information or send commands.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.mode", matches = "microservices")
    public void testEncodeWithServiceClient() throws Exception {
        // Set up mock protocol service client
        var serviceClient = mockProtocolServiceClient();
        when(serviceClient.getDeviceIdentifier(1)).thenReturn("123456789012345");
        
        // Create encoder with service client
        var encoder = inject(new PretraceProtocolEncoder(null));
        
        // Create command
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "D21012");
        
        // Encode command
        String encodedCommand = encoder.encodeCommand(command);
        
        // Verify the encoded command
        assertEquals("(123456789012345D21012^44)", encodedCommand);
        
        // Verify service client was called
        verify(serviceClient).getDeviceIdentifier(1);
        
        // Verify command was sent across service boundary
        verifyCommandSent(serviceClient, command, encodedCommand);
    }
    
    /**
     * Tests the encoder with distributed tracing.
     * This test is only enabled when running in microservices mode.
     * <p>
     * In microservices architecture, distributed tracing is essential for
     * tracking requests across service boundaries and troubleshooting issues.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.mode", matches = "microservices")
    public void testEncodeWithDistributedTracing() throws Exception {
        // Set up mock tracer
        var tracer = mockTracer();
        
        // Create encoder with tracer
        var encoder = inject(new PretraceProtocolEncoder(null));
        
        // Create command
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_POSITION_PERIODIC);
        command.set(Command.KEY_FREQUENCY, 300);
        
        // Encode command with tracing context
        withSpan("encode-pretrace-command", span -> {
            String encodedCommand = encoder.encodeCommand(command);
            assertEquals("(123456789012345D221300,300,,^69)", encodedCommand);
            return null;
        });
        
        // Verify span was created and completed
        verifySpan(tracer, "encode-pretrace-command");
    }
}