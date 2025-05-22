package org.traccar.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.helper.DistanceCalculator;

// For microservices testing
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageConsumer;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;

/**
 * Test for DistanceHandler that supports both monolithic and microservices testing patterns.
 * This test can be used in the current monolithic architecture and is prepared for
 * migration to the Position Service.
 */
@ExtendWith(MockitoExtension.class)
public class DistanceHandlerTest {

    @Mock
    private CacheManager cacheManager;
    
    @Mock
    private Config config;
    
    @Mock
    private MessageProducer messageProducer;
    
    @Mock
    private MessageConsumer messageConsumer;
    
    private DistanceHandler distanceHandler;
    
    @BeforeEach
    public void setup() {
        // Configure the config mock with default values
        when(config.getBoolean(Keys.COORDINATES_FILTER)).thenReturn(false);
        when(config.getInteger(Keys.COORDINATES_MIN_ERROR)).thenReturn(0);
        when(config.getInteger(Keys.COORDINATES_MAX_ERROR)).thenReturn(0);
        
        // Create the handler with mocked dependencies
        distanceHandler = new DistanceHandler(config, cacheManager);
    }

    /**
     * Test the basic distance calculation in monolithic mode.
     * This is the original test case, maintained for backward compatibility.
     */
    @Test
    public void testCalculateDistance() {
        Position position = new Position();
        distanceHandler.handlePosition(position, p -> {});

        assertEquals(0.0, position.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(0.0, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));

        position.set(Position.KEY_DISTANCE, 100);

        distanceHandler.handlePosition(position, p -> {});

        assertEquals(100.0, position.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(100.0, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
    }
    
    /**
     * Test distance calculation with a previous position in the cache.
     * Tests the handler's ability to calculate distance between positions.
     */
    @Test
    public void testCalculateDistanceWithPreviousPosition() {
        // Create a previous position
        Position lastPosition = new Position();
        lastPosition.setLatitude(10.0);
        lastPosition.setLongitude(10.0);
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 1000.0);
        lastPosition.setValid(true);
        
        // Mock the cache to return the previous position
        when(cacheManager.getPosition(anyLong())).thenReturn(lastPosition);
        
        // Create a new position
        Position position = new Position();
        position.setDeviceId(1L);
        position.setLatitude(10.1); // Moved north
        position.setLongitude(10.1); // Moved east
        position.setValid(true);
        
        // Process the position
        distanceHandler.handlePosition(position, p -> {});
        
        // The distance should be calculated based on the coordinates
        double expectedDistance = DistanceCalculator.distance(
                position.getLatitude(), position.getLongitude(),
                lastPosition.getLatitude(), lastPosition.getLongitude());
        
        assertEquals(expectedDistance, position.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(1000.0 + expectedDistance, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
    }
    
    /**
     * Test the coordinate filtering functionality.
     * Tests the handler's ability to filter out erroneous position updates.
     */
    @Test
    public void testCoordinateFiltering() {
        // Enable coordinate filtering
        when(config.getBoolean(Keys.COORDINATES_FILTER)).thenReturn(true);
        when(config.getInteger(Keys.COORDINATES_MIN_ERROR)).thenReturn(10); // Min 10 meters
        when(config.getInteger(Keys.COORDINATES_MAX_ERROR)).thenReturn(1000); // Max 1000 meters
        
        // Create a previous position
        Position lastPosition = new Position();
        lastPosition.setLatitude(10.0);
        lastPosition.setLongitude(10.0);
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 1000.0);
        lastPosition.setValid(true);
        
        // Mock the cache to return the previous position
        when(cacheManager.getPosition(anyLong())).thenReturn(lastPosition);
        
        // Create a new position with a very small movement (less than min error)
        Position position = new Position();
        position.setDeviceId(1L);
        position.setLatitude(10.00001); // Very small movement
        position.setLongitude(10.00001); // Very small movement
        position.setValid(true);
        
        // Process the position
        distanceHandler.handlePosition(position, p -> {});
        
        // The position should be filtered (coordinates replaced with last position)
        assertEquals(lastPosition.getLatitude(), position.getLatitude());
        assertEquals(lastPosition.getLongitude(), position.getLongitude());
        assertEquals(0.0, position.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(1000.0, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
    }
    
    /**
     * Test the microservices integration with message broker.
     * This test simulates how the handler would work in a microservices architecture
     * where positions are received and processed via a message broker.
     */
    @Test
    public void testMicroservicesIntegration() {
        // Setup position data
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1L);
        lastPosition.setLatitude(10.0);
        lastPosition.setLongitude(10.0);
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 1000.0);
        lastPosition.setValid(true);
        
        Position newPosition = new Position();
        newPosition.setDeviceId(1L);
        newPosition.setLatitude(10.1);
        newPosition.setLongitude(10.1);
        newPosition.setValid(true);
        
        // Mock the distributed cache to return the last position
        when(cacheManager.getPosition(eq(1L))).thenReturn(lastPosition);
        
        // Mock the message producer to capture the processed position
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        when(messageProducer.send(eq("enriched-positions"), positionCaptor.capture()))
                .thenReturn(CompletableFuture.completedFuture(null));
        
        // Simulate receiving a position from the message broker
        messageConsumer.receive("raw-positions", newPosition, (position) -> {
            // Process the position with the distance handler
            distanceHandler.handlePosition(position, p -> {
                // After processing, send to the next topic
                messageProducer.send("enriched-positions", p);
            });
        });
        
        // Verify the position was processed and published
        verify(messageProducer, times(1)).send(eq("enriched-positions"), any(Position.class));
        
        // Get the captured position
        Position processedPosition = positionCaptor.getValue();
        assertNotNull(processedPosition);
        
        // Calculate expected distance
        double expectedDistance = DistanceCalculator.distance(
                newPosition.getLatitude(), newPosition.getLongitude(),
                lastPosition.getLatitude(), lastPosition.getLongitude());
        
        // Verify distance calculations
        assertEquals(expectedDistance, processedPosition.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(1000.0 + expectedDistance, processedPosition.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
    }
    
    /**
     * Test the handler's behavior with a distributed cache in a microservices environment.
     * This test simulates cache misses and network delays that might occur in a distributed system.
     */
    @Test
    public void testDistributedCacheScenarios() {
        // First test with a cache miss (no previous position)
        when(cacheManager.getPosition(anyLong())).thenReturn(null);
        
        Position position = new Position();
        position.setDeviceId(1L);
        position.setLatitude(10.0);
        position.setLongitude(10.0);
        position.setValid(true);
        
        // Process the position with a cache miss
        distanceHandler.handlePosition(position, p -> {});
        
        // With no previous position, distance should be 0
        assertEquals(0.0, position.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(0.0, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
        
        // Now simulate a cache update and a second position
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1L);
        lastPosition.setLatitude(10.0);
        lastPosition.setLongitude(10.0);
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 500.0);
        lastPosition.setValid(true);
        
        // Update the mock to return the last position
        when(cacheManager.getPosition(eq(1L))).thenReturn(lastPosition);
        
        Position secondPosition = new Position();
        secondPosition.setDeviceId(1L);
        secondPosition.setLatitude(10.2);
        secondPosition.setLongitude(10.2);
        secondPosition.setValid(true);
        
        // Process the second position
        distanceHandler.handlePosition(secondPosition, p -> {});
        
        // Calculate expected distance
        double expectedDistance = DistanceCalculator.distance(
                secondPosition.getLatitude(), secondPosition.getLongitude(),
                lastPosition.getLatitude(), lastPosition.getLongitude());
        
        // Verify distance calculations
        assertEquals(expectedDistance, secondPosition.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(500.0 + expectedDistance, secondPosition.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
    }
}