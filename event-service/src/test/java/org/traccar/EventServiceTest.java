package org.traccar;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.traccar.handler.EventProducer;
import org.traccar.handler.PositionConsumer;
import org.traccar.handler.events.BaseEventHandler;
import org.traccar.handler.events.MotionEventHandler;
import org.traccar.handler.events.OverspeedEventHandler;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.state.MotionProcessor;
import org.traccar.session.state.MotionState;
import org.traccar.session.state.OverspeedProcessor;
import org.traccar.session.state.OverspeedState;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Integration tests for the Event Processing Service's core functionality.
 * Validates the service's ability to consume position messages from the message broker,
 * detect events based on business rules, and publish detected events back to the broker.
 */
@ExtendWith(MockitoExtension.class)
public class EventServiceTest {

    @Mock
    private KafkaTemplate<String, Position> positionKafkaTemplate;

    @Mock
    private KafkaTemplate<String, Event> eventKafkaTemplate;

    private PositionConsumer positionConsumer;
    private EventProducer eventProducer;
    private MotionEventHandler motionEventHandler;
    private OverspeedEventHandler overspeedEventHandler;

    /**
     * Helper method to create a position object with specific attributes for testing
     */
    private Position createPosition(String time, double speed, boolean motion, double distance) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.setSpeed(speed);
        position.set(Position.KEY_MOTION, motion);
        position.set(Position.KEY_TOTAL_DISTANCE, distance);
        position.setDeviceId(1L);
        position.setValid(true);
        position.setLatitude(40.7128);
        position.setLongitude(-74.0060);
        return position;
    }

    @BeforeEach
    public void setUp() {
        // Initialize the event producer
        eventProducer = new EventProducer(eventKafkaTemplate);
        
        // Initialize event handlers
        motionEventHandler = spy(new MotionEventHandler());
        overspeedEventHandler = spy(new OverspeedEventHandler());
        
        // Initialize the position consumer with the event handlers
        positionConsumer = new PositionConsumer(positionKafkaTemplate);
        positionConsumer.setMotionEventHandler(motionEventHandler);
        positionConsumer.setOverspeedEventHandler(overspeedEventHandler);
        positionConsumer.setEventProducer(eventProducer);
    }

    /**
     * Test that the service correctly processes position data and detects motion events
     */
    @Test
    public void testMotionEventDetection() throws ParseException {
        // Create a sequence of positions that should trigger a motion event
        Position position1 = createPosition("2023-01-01 00:00:00", 0, false, 0);
        Position position2 = createPosition("2023-01-01 00:02:00", 10, true, 100);
        Position position3 = createPosition("2023-01-01 00:04:00", 20, true, 700);

        // Mock the motion state to simulate the state maintained by the handler
        MotionState motionState = new MotionState();
        ReflectionTestUtils.setField(motionEventHandler, "motionState", motionState);

        // Capture the events published to the message broker
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(eventKafkaTemplate.send(eq("events"), any(String.class), eventCaptor.capture()))
                .thenReturn(mock(CompletableFuture.class));

        // Process the positions
        positionConsumer.processPosition(position1);
        positionConsumer.processPosition(position2);
        positionConsumer.processPosition(position3);

        // Verify that the motion event handler was called for each position
        verify(motionEventHandler, times(3)).analyzePosition(any(Position.class), any(BaseEventHandler.Callback.class));

        // Verify that a motion event was detected and published
        verify(eventKafkaTemplate, times(1)).send(eq("events"), any(String.class), any(Event.class));
        
        // Verify the event details
        Event capturedEvent = eventCaptor.getValue();
        assertEquals(Event.TYPE_DEVICE_MOVING, capturedEvent.getType());
        assertEquals(1L, capturedEvent.getDeviceId());
    }

    /**
     * Test that the service correctly processes position data and detects overspeed events
     */
    @Test
    public void testOverspeedEventDetection() throws ParseException {
        // Create a sequence of positions that should trigger an overspeed event
        Position position1 = createPosition("2023-01-01 00:00:00", 50, true, 0);
        Position position2 = createPosition("2023-01-01 00:00:10", 55, true, 100);
        Position position3 = createPosition("2023-01-01 00:00:20", 55, true, 200);

        // Mock the overspeed state to simulate the state maintained by the handler
        OverspeedState overspeedState = new OverspeedState();
        ReflectionTestUtils.setField(overspeedEventHandler, "overspeedState", overspeedState);
        
        // Mock the speed limit retrieval
        doAnswer(invocation -> {
            Position position = invocation.getArgument(0);
            BaseEventHandler.Callback callback = invocation.getArgument(1);
            
            // Simulate overspeed detection logic
            OverspeedProcessor.updateState(overspeedState, position, 40, 1, 15000, 0);
            if (overspeedState.getEvent() != null) {
                callback.eventDetected(overspeedState.getEvent());
            }
            return null;
        }).when(overspeedEventHandler).onPosition(any(Position.class), any(BaseEventHandler.Callback.class));

        // Capture the events published to the message broker
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(eventKafkaTemplate.send(eq("events"), any(String.class), eventCaptor.capture()))
                .thenReturn(mock(CompletableFuture.class));

        // Process the positions
        positionConsumer.processPosition(position1);
        positionConsumer.processPosition(position2);
        positionConsumer.processPosition(position3);

        // Verify that the overspeed event handler was called for each position
        verify(overspeedEventHandler, times(3)).analyzePosition(any(Position.class), any(BaseEventHandler.Callback.class));

        // Verify that an overspeed event was detected and published
        verify(eventKafkaTemplate, times(1)).send(eq("events"), any(String.class), any(Event.class));
        
        // Verify the event details
        Event capturedEvent = eventCaptor.getValue();
        assertEquals(Event.TYPE_DEVICE_OVERSPEED, capturedEvent.getType());
        assertEquals(1L, capturedEvent.getDeviceId());
        assertEquals(55.0, capturedEvent.getDouble("speed"), 0.1);
        assertEquals(40.0, capturedEvent.getDouble("speedLimit"), 0.1);
    }

    /**
     * Test that the service correctly handles message broker failures
     */
    @Test
    public void testMessageBrokerFailureHandling() throws ParseException {
        // Create a position that should trigger an event
        Position position = createPosition("2023-01-01 00:00:00", 50, true, 0);

        // Mock the event handler to generate an event
        doAnswer(invocation -> {
            BaseEventHandler.Callback callback = invocation.getArgument(1);
            Event event = new Event(Event.TYPE_DEVICE_OVERSPEED, position);
            event.set("speed", 50.0);
            event.set("speedLimit", 40.0);
            callback.eventDetected(event);
            return null;
        }).when(overspeedEventHandler).onPosition(any(Position.class), any(BaseEventHandler.Callback.class));

        // Simulate a message broker failure
        when(eventKafkaTemplate.send(anyString(), anyString(), any(Event.class)))
                .thenThrow(new RuntimeException("Broker connection failed"));

        // Process should not throw exception even when broker fails
        assertDoesNotThrow(() -> positionConsumer.processPosition(position));

        // Verify that the event handler was called
        verify(overspeedEventHandler).analyzePosition(any(Position.class), any(BaseEventHandler.Callback.class));
        
        // Verify that an attempt was made to publish the event
        verify(eventKafkaTemplate).send(eq("events"), any(String.class), any(Event.class));
    }

    /**
     * Test that the service correctly enriches events with additional context
     */
    @Test
    public void testEventEnrichment() throws ParseException {
        // Create a position that should trigger an event
        Position position = createPosition("2023-01-01 00:00:00", 50, true, 0);
        position.setAddress("123 Test Street, New York, NY");

        // Mock the event handler to generate an event
        doAnswer(invocation -> {
            BaseEventHandler.Callback callback = invocation.getArgument(1);
            Event event = new Event(Event.TYPE_DEVICE_OVERSPEED, position);
            callback.eventDetected(event);
            return null;
        }).when(overspeedEventHandler).onPosition(any(Position.class), any(BaseEventHandler.Callback.class));

        // Capture the events published to the message broker
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(eventKafkaTemplate.send(eq("events"), any(String.class), eventCaptor.capture()))
                .thenReturn(mock(CompletableFuture.class));

        // Process the position
        positionConsumer.processPosition(position);

        // Verify that the event was enriched with position data
        Event capturedEvent = eventCaptor.getValue();
        assertEquals(position.getId(), capturedEvent.getPositionId());
        assertEquals(position.getDeviceId(), capturedEvent.getDeviceId());
        assertEquals(position.getDeviceTime(), capturedEvent.getEventTime());
    }

    /**
     * Test that the service correctly processes multiple event types from a single position
     */
    @Test
    public void testMultipleEventTypes() throws ParseException {
        // Create a position that should trigger multiple events
        Position position = createPosition("2023-01-01 00:00:00", 50, true, 0);

        // Mock the event handlers to generate different types of events
        doAnswer(invocation -> {
            BaseEventHandler.Callback callback = invocation.getArgument(1);
            Event event = new Event(Event.TYPE_DEVICE_MOVING, position);
            callback.eventDetected(event);
            return null;
        }).when(motionEventHandler).onPosition(any(Position.class), any(BaseEventHandler.Callback.class));

        doAnswer(invocation -> {
            BaseEventHandler.Callback callback = invocation.getArgument(1);
            Event event = new Event(Event.TYPE_DEVICE_OVERSPEED, position);
            callback.eventDetected(event);
            return null;
        }).when(overspeedEventHandler).onPosition(any(Position.class), any(BaseEventHandler.Callback.class));

        // Capture the events published to the message broker
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(eventKafkaTemplate.send(eq("events"), any(String.class), eventCaptor.capture()))
                .thenReturn(mock(CompletableFuture.class));

        // Process the position
        positionConsumer.processPosition(position);

        // Verify that both event handlers were called
        verify(motionEventHandler).analyzePosition(any(Position.class), any(BaseEventHandler.Callback.class));
        verify(overspeedEventHandler).analyzePosition(any(Position.class), any(BaseEventHandler.Callback.class));

        // Verify that both events were published
        verify(eventKafkaTemplate, times(2)).send(eq("events"), any(String.class), any(Event.class));
        
        // Verify the event types
        assertEquals(2, eventCaptor.getAllValues().size());
        assertTrue(eventCaptor.getAllValues().stream()
                .anyMatch(event -> Event.TYPE_DEVICE_MOVING.equals(event.getType())));
        assertTrue(eventCaptor.getAllValues().stream()
                .anyMatch(event -> Event.TYPE_DEVICE_OVERSPEED.equals(event.getType())));
    }

    /**
     * Test that the service correctly prioritizes events based on business rules
     */
    @Test
    public void testEventPrioritization() throws ParseException {
        // Create a position that should trigger multiple events
        Position position = createPosition("2023-01-01 00:00:00", 50, true, 0);

        // Mock the event handlers to generate different types of events with priorities
        doAnswer(invocation -> {
            BaseEventHandler.Callback callback = invocation.getArgument(1);
            Event event = new Event(Event.TYPE_DEVICE_MOVING, position);
            event.set("priority", 2); // Lower priority
            callback.eventDetected(event);
            return null;
        }).when(motionEventHandler).onPosition(any(Position.class), any(BaseEventHandler.Callback.class));

        doAnswer(invocation -> {
            BaseEventHandler.Callback callback = invocation.getArgument(1);
            Event event = new Event(Event.TYPE_DEVICE_OVERSPEED, position);
            event.set("priority", 1); // Higher priority
            callback.eventDetected(event);
            return null;
        }).when(overspeedEventHandler).onPosition(any(Position.class), any(BaseEventHandler.Callback.class));

        // Capture the events published to the message broker
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        when(eventKafkaTemplate.send(eq("events"), any(String.class), eventCaptor.capture()))
                .thenReturn(mock(CompletableFuture.class));

        // Process the position
        positionConsumer.processPosition(position);

        // Verify that both events were published
        verify(eventKafkaTemplate, times(2)).send(eq("events"), any(String.class), any(Event.class));
        
        // Verify the order of events (higher priority should be first)
        assertEquals(Event.TYPE_DEVICE_OVERSPEED, eventCaptor.getAllValues().get(0).getType());
        assertEquals(Event.TYPE_DEVICE_MOVING, eventCaptor.getAllValues().get(1).getType());
    }
}