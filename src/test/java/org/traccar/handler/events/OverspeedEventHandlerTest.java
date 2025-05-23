package org.traccar.handler.events;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.BaseTest;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.state.OverspeedProcessor;
import org.traccar.session.state.OverspeedState;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Test for OverspeedEventHandler
 * Supports both direct method calls (monolithic) and message broker interactions (microservices)
 */
@ExtendWith(MockitoExtension.class)
public class OverspeedEventHandlerTest extends BaseTest {

    @Mock
    private Consumer<Event> eventConsumer;

    @Mock
    private MessageBrokerPublisher messageBrokerPublisher;

    private Position position(String time, double speed) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.setSpeed(speed);
        return position;
    }

    private void verifyState(OverspeedState overspeedState, boolean state, long geofenceId) {
        assertEquals(state, overspeedState.getOverspeedState());
        assertEquals(geofenceId, overspeedState.getOverspeedGeofenceId());
    }

    private void testOverspeedWithPosition(long geofenceId) throws ParseException {
        OverspeedState state = new OverspeedState();

        OverspeedProcessor.updateState(state, position("2017-01-01 00:00:00", 50), 40, 1, 15000, geofenceId);
        assertNull(state.getEvent());
        verifyState(state, true, geofenceId);

        OverspeedProcessor.updateState(state, position("2017-01-01 00:00:10", 55), 40, 1, 15000, geofenceId);
        assertNull(state.getEvent());

        OverspeedProcessor.updateState(state, position("2017-01-01 00:00:20", 55), 40, 1, 15000, geofenceId);
        assertNotNull(state.getEvent());
        assertEquals(Event.TYPE_DEVICE_OVERSPEED, state.getEvent().getType());
        assertEquals(55, state.getEvent().getDouble("speed"), 0.1);
        assertEquals(40, state.getEvent().getDouble("speedLimit"), 0.1);
        assertEquals(geofenceId, state.getEvent().getGeofenceId());
        verifyState(state, true, 0);

        OverspeedProcessor.updateState(state, position("2017-01-01 00:00:30", 55), 40, 1, 15000, geofenceId);
        assertNull(state.getEvent());
        verifyState(state, true, 0);

        OverspeedProcessor.updateState(state, position("2017-01-01 00:00:30", 30), 40, 1, 15000, geofenceId);
        assertNull(state.getEvent());
        verifyState(state, false, 0);
    }

    /**
     * Original test method - maintained for backward compatibility
     */
    @Test
    public void testOverspeedEventHandler() throws Exception {
        testOverspeedWithPosition(0);
        testOverspeedWithPosition(1);
    }

    /**
     * Test for asynchronous event processing with message broker
     * Only runs when system property "test.environment" is set to "microservices"
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testOverspeedEventHandlerWithMessageBroker() throws Exception {
        // Setup
        OverspeedEventHandler handler = new OverspeedEventHandler(messageBrokerPublisher);
        Position position = position("2017-01-01 00:00:00", 50);
        position.set(Position.KEY_DEVICE_ID, 1L);
        
        // Create a future to wait for the asynchronous event
        CompletableFuture<Event> eventFuture = new CompletableFuture<>();
        
        // Configure mock to complete the future when publishEvent is called
        Mockito.doAnswer(invocation -> {
            Event event = invocation.getArgument(0);
            eventFuture.complete(event);
            return null;
        }).when(messageBrokerPublisher).publishEvent(any(Event.class));
        
        // Execute
        handler.analyzePosition(position);
        
        // Send another position to trigger the overspeed event
        Position position2 = position("2017-01-01 00:00:20", 55);
        position2.set(Position.KEY_DEVICE_ID, 1L);
        handler.analyzePosition(position2);
        
        // Wait for the event to be published to the message broker
        Event event = eventFuture.get(5, TimeUnit.SECONDS);
        
        // Verify
        assertNotNull(event);
        assertEquals(Event.TYPE_DEVICE_OVERSPEED, event.getType());
        verify(messageBrokerPublisher, times(1)).publishEvent(any(Event.class));
    }

    /**
     * Test for direct event consumer with callback
     * Only runs when system property "test.environment" is set to "microservices"
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservices")
    public void testOverspeedEventHandlerWithCallback() throws Exception {
        // Setup
        OverspeedEventHandler handler = new OverspeedEventHandler();
        Position position = position("2017-01-01 00:00:00", 50);
        position.set(Position.KEY_DEVICE_ID, 1L);
        
        // Execute
        handler.analyzePosition(position, eventConsumer);
        
        // No event should be generated for the first position
        verify(eventConsumer, times(0)).accept(any(Event.class));
        
        // Send another position to trigger the overspeed event
        Position position2 = position("2017-01-01 00:00:20", 55);
        position2.set(Position.KEY_DEVICE_ID, 1L);
        handler.analyzePosition(position2, eventConsumer);
        
        // Verify that the event consumer was called
        verify(eventConsumer, times(1)).accept(any(Event.class));
    }

    /**
     * Interface for message broker publishing
     * This would be implemented by the actual message broker adapter (Kafka/RabbitMQ)
     */
    public interface MessageBrokerPublisher {
        void publishEvent(Event event);
    }
}