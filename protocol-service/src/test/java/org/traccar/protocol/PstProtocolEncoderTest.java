package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;
import org.traccar.messaging.MessageProducer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for PST protocol encoder
 * Supports both monolithic and microservices testing environments
 */
@ExtendWith(MockitoExtension.class)
public class PstProtocolEncoderTest extends ProtocolTest {

    @Mock
    private MessageProducer messageProducer;

    @Test
    public void testEncodeEngineStop() throws Exception {
        // Test for direct encoding (monolithic mode)
        var encoder = inject(new PstProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);

        verifyCommand(encoder, command, binary("860ddf790600000001060002ffffffffe42b"));

        // Test for message broker integration (microservices mode)
        var encoderWithBroker = inject(new PstProtocolEncoder(null, messageProducer));
        when(messageProducer.isConnected()).thenReturn(true);
        
        encoderWithBroker.encodeCommand(command);
        
        // Verify the command was published to the message broker
        verify(messageProducer).publish(eq("protocol.commands"), any());
    }

    @Test
    public void testEncodeEngineResume() throws Exception {
        // Test for direct encoding (monolithic mode)
        var encoder = inject(new PstProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_RESUME);

        verifyCommand(encoder, command, binary("860ddf790600000001060001ffffffff0af9"));

        // Test for message broker integration (microservices mode)
        var encoderWithBroker = inject(new PstProtocolEncoder(null, messageProducer));
        when(messageProducer.isConnected()).thenReturn(true);
        
        encoderWithBroker.encodeCommand(command);
        
        // Verify the command was published to the message broker
        verify(messageProducer).publish(eq("protocol.commands"), any());
    }

    @Test
    public void testServiceBoundaryHandling() throws Exception {
        // Test for cross-service boundary handling
        var encoder = inject(new PstProtocolEncoder(null, messageProducer));
        when(messageProducer.isConnected()).thenReturn(false); // Simulate broker disconnection
        
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);
        
        // Should fall back to direct encoding when broker is unavailable
        verifyCommand(encoder, command, binary("860ddf790600000001060002ffffffffe42b"));
    }
}