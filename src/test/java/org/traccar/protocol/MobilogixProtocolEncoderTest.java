package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;
import org.traccar.ProtocolTest;
import org.traccar.model.Command;
import org.traccar.session.cache.CacheManager;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test for Mobilogix Protocol Encoder
 * Updated to support both monolithic and microservices testing
 */
public class MobilogixProtocolEncoderTest extends ProtocolTest {
    private final Date time = Date.from(
            LocalDateTime.of(LocalDate.of(2025, 2, 22), LocalTime.of(1, 2, 3)).atZone(ZoneOffset.systemDefault()).toInstant());

    /**
     * Test encoding in monolithic mode (backward compatibility)
     */
    @Test
    public void testEncode() throws Exception {
        var encoder = inject(new MobilogixProtocolEncoder(null));

        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);

        assertEquals("[2025-02-22 01:02:03,S6,RELAY=1]", encoder.encodeCommand(command, time));
    }

    /**
     * Test encoding with message broker integration
     * This test is only enabled when running in microservices mode
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testEncodeWithMessageBroker() throws Exception {
        // Create a mock message broker client
        MessageBrokerClient messageBrokerClient = mock(MessageBrokerClient.class);
        when(messageBrokerClient.publishCommand(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));
        
        // Create the encoder with message broker support
        var encoder = new MobilogixProtocolEncoder(null);
        encoder.setMessageBrokerClient(messageBrokerClient);
        
        // Mock the cache manager
        CacheManager cacheManager = mock(CacheManager.class);
        encoder.setCacheManager(cacheManager);
        
        // Create and encode the command
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);
        
        String encodedCommand = encoder.encodeCommand(command, time);
        assertEquals("[2025-02-22 01:02:03,S6,RELAY=1]", encodedCommand);
        
        // Verify the command was published to the message broker
        Mockito.verify(messageBrokerClient).publishCommand(eq(command), any());
    }

    /**
     * Test cross-service boundary handling
     * This test verifies that commands can be properly encoded and sent across service boundaries
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testCrossServiceBoundaryHandling() throws Exception {
        // Create mock services
        PositionServiceClient positionServiceClient = mock(PositionServiceClient.class);
        MessageBrokerClient messageBrokerClient = mock(MessageBrokerClient.class);
        
        // Configure the mock services
        when(messageBrokerClient.publishCommand(any(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(positionServiceClient.getLastPosition(any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        
        // Create the encoder with service clients
        var encoder = new MobilogixProtocolEncoder(null);
        encoder.setMessageBrokerClient(messageBrokerClient);
        encoder.setPositionServiceClient(positionServiceClient);
        
        // Mock the cache manager
        CacheManager cacheManager = mock(CacheManager.class);
        encoder.setCacheManager(cacheManager);
        
        // Create and encode the command
        Command command = new Command();
        command.setDeviceId(1);
        command.setType(Command.TYPE_ENGINE_STOP);
        
        String encodedCommand = encoder.encodeCommand(command, time);
        assertNotNull(encodedCommand);
        assertEquals("[2025-02-22 01:02:03,S6,RELAY=1]", encodedCommand);
        
        // Verify cross-service interactions
        Mockito.verify(messageBrokerClient).publishCommand(eq(command), any());
        Mockito.verify(positionServiceClient).getLastPosition(eq(1L));
    }

    /**
     * Interface for message broker client
     * This would be implemented in the actual microservices architecture
     */
    public interface MessageBrokerClient {
        CompletableFuture<Boolean> publishCommand(Command command, String encodedCommand);
    }

    /**
     * Interface for position service client
     * This would be implemented in the actual microservices architecture
     */
    public interface PositionServiceClient {
        CompletableFuture<Object> getLastPosition(Long deviceId);
    }
}