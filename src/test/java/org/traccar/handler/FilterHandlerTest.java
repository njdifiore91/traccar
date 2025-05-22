package org.traccar.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.ArgumentCaptor;
import org.traccar.BaseTest;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

// Imports for message broker integration testing
import org.traccar.messaging.MessageBroker;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageConsumer;

import java.util.Date;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

/**
 * Test for FilterHandler that supports both monolithic and microservices testing patterns.
 * This test will be gradually migrated to the Position Service test folder as part of the
 * microservices architecture transition.
 */
public class FilterHandlerTest extends BaseTest {

    private FilterHandler passingHandler;
    private FilterHandler filteringHandler;
    private MessageProducer messageProducer;
    private CacheManager cacheManager;
    private Config config;

    /**
     * Setup for passing handler in both monolithic and microservices mode
     */
    @BeforeEach
    public void passingHandler() {
        config = mock(Config.class);
        when(config.getBoolean(Keys.FILTER_ENABLE)).thenReturn(true);
        
        // Setup distributed cache manager with fallback to local cache
        cacheManager = mock(CacheManager.class);
        when(cacheManager.getConfig()).thenReturn(config);
        when(cacheManager.getObject(any(), anyLong())).thenReturn(mock(Device.class));
        when(cacheManager.isDistributedCache()).thenReturn(false); // Default to local cache for backward compatibility
        
        // Setup message broker for microservices testing
        messageProducer = mock(MessageProducer.class);
        when(messageProducer.send(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        
        passingHandler = new FilterHandler(config, cacheManager, messageProducer, null);
    }

    /**
     * Setup for filtering handler in both monolithic and microservices mode
     */
    @BeforeEach
    public void filteringHandler() {
        config = mock(Config.class);
        when(config.getBoolean(Keys.FILTER_ENABLE)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_INVALID)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_ZERO)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_DUPLICATE)).thenReturn(true);
        when(config.getLong(Keys.FILTER_FUTURE)).thenReturn(5 * 60L);
        when(config.getBoolean(Keys.FILTER_APPROXIMATE)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_STATIC)).thenReturn(true);
        when(config.getInteger(Keys.FILTER_DISTANCE)).thenReturn(10);
        when(config.getInteger(Keys.FILTER_MAX_SPEED)).thenReturn(500);
        when(config.getLong(Keys.FILTER_SKIP_LIMIT)).thenReturn(10L);
        when(config.getBoolean(Keys.FILTER_SKIP_ATTRIBUTES_ENABLE)).thenReturn(true);
        when(config.getString(Keys.FILTER_SKIP_ATTRIBUTES.getKey())).thenReturn("alarm,result");
        
        // Setup distributed cache manager with fallback to local cache
        cacheManager = mock(CacheManager.class);
        when(cacheManager.getConfig()).thenReturn(config);
        when(cacheManager.getObject(any(), anyLong())).thenReturn(mock(Device.class));
        when(cacheManager.isDistributedCache()).thenReturn(false); // Default to local cache for backward compatibility
        
        // Setup message broker for microservices testing
        messageProducer = mock(MessageProducer.class);
        when(messageProducer.send(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
        
        filteringHandler = new FilterHandler(config, cacheManager, messageProducer, null);
    }

    private Position createPosition(Date time, boolean valid, double speed) {
        Position position = new Position();
        position.setDeviceId(0);
        position.setTime(time);
        position.setValid(valid);
        position.setLatitude(10);
        position.setLongitude(10);
        position.setAltitude(10);
        position.setSpeed(speed);
        position.setCourse(10);
        return position;
    }

    @Test
    public void testFilter() {
        Position position = createPosition(new Date(), true, 10);

        assertFalse(filteringHandler.filter(position));
        assertFalse(passingHandler.filter(position));

        position = createPosition(new Date(Long.MAX_VALUE), true, 10);

        assertTrue(filteringHandler.filter(position));
        assertFalse(passingHandler.filter(position));

        position = createPosition(new Date(), false, 10);

        assertTrue(filteringHandler.filter(position));
        assertFalse(passingHandler.filter(position));
    }

    @Test
    public void testSkipAttributes() {
        Position position = createPosition(new Date(), true, 0);
        position.addAlarm(Position.ALARM_GENERAL);

        assertFalse(filteringHandler.filter(position));
    }
    
    /**
     * Test filter handler in microservices mode with distributed cache
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testFilterWithDistributedCache() {
        // Setup distributed cache scenario
        when(cacheManager.isDistributedCache()).thenReturn(true);
        
        Position position = createPosition(new Date(), true, 10);
        
        assertFalse(filteringHandler.filter(position));
        
        // Verify cache interactions in distributed mode
        verify(cacheManager, times(1)).getObject(eq(Device.class), eq(0L));
    }
    
    /**
     * Test integration with message broker for cross-service communication
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testFilterWithMessageBroker() {
        // Create a valid position that should pass filtering
        Position position = createPosition(new Date(), true, 10);
        
        // Configure handler to use message broker
        FilterHandler handlerWithBroker = new FilterHandler(config, cacheManager, messageProducer, null);
        
        // Process position
        assertFalse(handlerWithBroker.filter(position));
        
        // Verify message was published to broker for cross-service processing
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messageProducer, times(1)).send(eq("positions"), positionCaptor.capture());
        
        // Verify the position data sent to the broker
        Position capturedPosition = positionCaptor.getValue();
        assertEquals(position.getDeviceId(), capturedPosition.getDeviceId());
        assertEquals(position.getLatitude(), capturedPosition.getLatitude(), 0.0001);
        assertEquals(position.getLongitude(), capturedPosition.getLongitude(), 0.0001);
    }
}