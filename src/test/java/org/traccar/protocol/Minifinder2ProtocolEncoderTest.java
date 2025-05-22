package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;
import org.traccar.model.Device;

// Import for message broker testing
import org.traccar.test.MessageBrokerTest;
import org.traccar.test.TestDataFactory;

import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;

/**
 * Test for Minifinder2 Protocol Encoder
 * Supports both monolithic and microservices testing environments
 */
public class Minifinder2ProtocolEncoderTest extends ProtocolTest {

    @Test
    public void testEncodeNano() throws Exception {
        // Standard monolithic test case
        var encoder = inject(new Minifinder2ProtocolEncoder(null));

        encoder.setModelOverride("Nano");

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_FIRMWARE_UPDATE);
        command.set(Command.KEY_DATA, "https://example.com");

        verifyCommand(encoder, command, binary("ab00160059d2010004143068747470733a2f2f6578616d706c652e636f6d"));
    }
    
    @Test
    @EnabledIfSystemProperty(named = "test.microservice", matches = "true")
    public void testEncodeNanoMicroservice() throws Exception {
        // Microservice-specific test case
        var encoder = inject(new Minifinder2ProtocolEncoder(null));

        encoder.setModelOverride("Nano");

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_FIRMWARE_UPDATE);
        command.set(Command.KEY_DATA, "https://example.com");

        verifyCommand(encoder, command, binary("ab00160059d2010004143068747470733a2f2f6578616d706c652e636f6d"));
    }
    
    @Test
    @EnabledIfSystemProperty(named = "test.microservice", matches = "true")
    public void testProtocolServiceMessageBrokerIntegration() throws Exception {
        // Test integration with message broker
        var messageBrokerTest = new MessageBrokerTest();
        var messageProducer = messageBrokerTest.getMessageProducer();
        
        var encoder = inject(new Minifinder2ProtocolEncoder(messageProducer));
        encoder.setModelOverride("Nano");
        
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_FIRMWARE_UPDATE);
        command.set(Command.KEY_DATA, "https://example.com");
        
        // Encode and verify the command
        var result = encoder.encodeCommand(command);
        
        // Verify the encoded command is correct
        verifyBinary(result, binary("ab00160059d2010004143068747470733a2f2f6578616d706c652e636f6d"));
        
        // Verify message was published to the broker
        messageBrokerTest.verifyCommandPublished(command);
    }
    
    @Test
    @EnabledIfSystemProperty(named = "test.microservice", matches = "true")
    public void testCrossServiceCommandHandling() throws Exception {
        // Test command handling across service boundaries
        var deviceServiceClient = mock(DeviceServiceClient.class);
        var device = TestDataFactory.createDevice(1, "Nano");
        when(deviceServiceClient.getDevice(any())).thenReturn(device);
        
        var messageBrokerTest = new MessageBrokerTest();
        var messageProducer = messageBrokerTest.getMessageProducer();
        
        var encoder = inject(new Minifinder2ProtocolEncoder(messageProducer));
        encoder.setDeviceServiceClient(deviceServiceClient);
        
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_FIRMWARE_UPDATE);
        command.set(Command.KEY_DATA, "https://example.com");
        
        // Encode and send the command
        var result = encoder.encodeCommand(command);
        
        // Verify the encoded command is correct
        verifyBinary(result, binary("ab00160059d2010004143068747470733a2f2f6578616d706c652e636f6d"));
        
        // Verify device service was called to get device information
        verify(deviceServiceClient).getDevice(any());
        
        // Verify command status was published to the broker
        messageBrokerTest.verifyCommandStatusPublished(command.getDeviceId(), command.getType(), true);
    }
}