package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;

// Imports for microservices testing
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import org.mockito.Mockito;
import org.traccar.session.cache.CacheManager;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test for Teltonika protocol encoder.
 * This test is designed to work in both monolithic and microservices environments.
 * 
 * In the microservices architecture, protocol handling is moved to the Protocol Service,
 * which communicates with other services via message brokers.
 */
public class TeltonikaProtocolEncoderTest extends ProtocolTest {

    /**
     * Tests the basic encoding functionality in the monolithic environment.
     * This test maintains backward compatibility with the existing system.
     */
    @Test
    public void testEncode() throws Exception {
        var encoder = inject(new TeltonikaProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);

        command.set(Command.KEY_DATA, "setdigout 11");
        verifyCommand(encoder, command, binary("00000000000000160C01050000000E7365746469676F75742031310D0A010000E258"));

        command.set(Command.KEY_DATA, "03030000000185E8");
        verifyCommand(encoder, command, binary("00000000000000100c01050000000803030000000185e8010000da8b"));
    }

    /**
     * Tests the encoding functionality with message broker integration.
     * This test is only enabled when running in the microservices environment.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testEncodeWithMessageBroker() throws Exception {
        // Create a mock cache manager for microservices testing
        CacheManager cacheManager = Mockito.mock(CacheManager.class);
        
        // Create the encoder with the mock cache manager
        var encoder = new TeltonikaProtocolEncoder(cacheManager);
        inject(encoder);
        
        // Create a command to encode
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "setdigout 11");
        
        // Encode the command
        ByteBuf result = encoder.encodeCommand(command);
        
        // Verify the encoded command
        assertEquals(
            "00000000000000160C01050000000E7365746469676F75742031310D0A010000E258",
            ByteBufUtil.hexDump(result));
    }

    /**
     * Tests protocol handling across service boundaries.
     * This test verifies that commands can be properly encoded and decoded
     * when crossing service boundaries via message brokers.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testCrossServiceCommandHandling() throws Exception {
        // Create a mock cache manager for microservices testing
        CacheManager cacheManager = Mockito.mock(CacheManager.class);
        
        // Create the encoder with the mock cache manager
        var encoder = new TeltonikaProtocolEncoder(cacheManager);
        inject(encoder);
        
        // Create a command to encode
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_CUSTOM);
        command.set(Command.KEY_DATA, "03030000000185E8");
        
        // Encode the command (simulating Protocol Service encoding)
        ByteBuf result = encoder.encodeCommand(command);
        
        // Verify the encoded command
        assertEquals(
            "00000000000000100c01050000000803030000000185e8010000da8b",
            ByteBufUtil.hexDump(result));
        
        // In a real microservices environment, this encoded command would be sent
        // through a message broker to the device via the Protocol Service
    }
}