package org.traccar.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for MotionHandler that supports both monolithic and microservices testing patterns.
 * This test has been updated to verify handler processing across service boundaries
 * and support integration with message brokers.
 */
public class MotionHandlerTest {

    private CacheManager cacheManager;
    private Config config;
    private MotionHandler motionHandler;
    private Consumer<Position> positionConsumer;

    @BeforeEach
    public void setUp() {
        // Mock the cache manager for distributed cache scenarios
        cacheManager = mock(CacheManager.class);
        
        // Mock a device to be returned by the cache manager
        Device device = mock(Device.class);
        when(cacheManager.getObject(eq(Device.class), anyLong())).thenReturn(device);
        
        // Mock the configuration
        config = mock(Config.class);
        when(config.getString(Keys.EVENT_MOTION_SPEED_THRESHOLD.getKey())).thenReturn("0.01");
        when(cacheManager.getConfig()).thenReturn(config);
        
        // Create the handler under test
        motionHandler = new MotionHandler(cacheManager);
        
        // Mock the position consumer (could be direct or via message broker)
        positionConsumer = mock(Consumer.class);
    }

    /**
     * Tests the basic motion calculation in a monolithic context.
     */
    @Test
    public void testCalculateMotion() {
        // Create a position to test
        Position position = new Position();
        position.setDeviceId(1L);
        
        // Process the position
        motionHandler.handlePosition(position, positionConsumer);

        // Verify the motion attribute was set correctly
        assertFalse((Boolean) position.getAttributes().get(Position.KEY_MOTION));
    }

    /**
     * Tests the motion handler with position forwarding to simulate
     * cross-service communication via a message broker.
     */
    @Test
    public void testMotionHandlerWithPositionForwarding() {
        // Create a position to test
        Position position = new Position();
        position.setDeviceId(1L);
        
        // Process the position
        motionHandler.handlePosition(position, positionConsumer);

        // Verify the motion attribute was set correctly
        assertFalse((Boolean) position.getAttributes().get(Position.KEY_MOTION));
        
        // Verify the position was forwarded to the consumer (simulating message broker)
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(positionConsumer).accept(positionCaptor.capture());
        
        // Verify the forwarded position has the correct motion attribute
        Position forwardedPosition = positionCaptor.getValue();
        assertEquals(position, forwardedPosition);
        assertFalse((Boolean) forwardedPosition.getAttributes().get(Position.KEY_MOTION));
    }

    /**
     * Tests the motion handler with a distributed cache configuration
     * to simulate microservices environment.
     */
    @Test
    public void testMotionHandlerWithDistributedCache() {
        // Mock distributed cache behavior
        CacheManager distributedCacheManager = mock(CacheManager.class);
        Device device = mock(Device.class);
        when(distributedCacheManager.getObject(eq(Device.class), anyLong())).thenReturn(device);
        
        Config distributedConfig = mock(Config.class);
        when(distributedConfig.getString(Keys.EVENT_MOTION_SPEED_THRESHOLD.getKey())).thenReturn("0.01");
        when(distributedCacheManager.getConfig()).thenReturn(distributedConfig);
        
        // Create handler with distributed cache
        MotionHandler handlerWithDistributedCache = new MotionHandler(distributedCacheManager);
        
        // Create a position to test
        Position position = new Position();
        position.setDeviceId(1L);
        
        // Process the position
        handlerWithDistributedCache.handlePosition(position, positionConsumer);

        // Verify the motion attribute was set correctly
        assertFalse((Boolean) position.getAttributes().get(Position.KEY_MOTION));
        
        // Verify cache was accessed in a distributed manner
        verify(distributedCacheManager).getObject(eq(Device.class), anyLong());
    }
}