package org.traccar.handler.events;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.BaseEventTest;
import org.traccar.messaging.EventProducer;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for AlarmEventHandler class.
 * 
 * This test verifies that the AlarmEventHandler correctly processes positions with alarm attributes,
 * generates appropriate events, and publishes them to the message broker. It also tests the integration
 * with OpenTelemetry for distributed tracing and metrics collection.
 */
@ExtendWith(MockitoExtension.class)
public class AlarmEventHandlerTest extends BaseEventTest {

    @Mock
    private CacheManager cacheManager;
    
    @Mock
    private Storage storage;
    
    @Mock
    private EventProducer eventProducer;
    
    @Mock
    private Tracer tracer;
    
    @Mock
    private SpanBuilder spanBuilder;
    
    @Mock
    private Span span;
    
    private MeterRegistry meterRegistry;
    private AlarmEventHandler alarmEventHandler;
    private Device device;
    private String correlationId;
    
    @BeforeEach
    public void setUp() {
        // Initialize a simple meter registry for metrics testing
        meterRegistry = new SimpleMeterRegistry();
        
        // Set up the correlation ID for tracing
        correlationId = "test-correlation-id-" + System.currentTimeMillis();
        
        // Set up the device mock
        device = mock(Device.class);
        when(device.getId()).thenReturn(1L);
        when(device.getUniqueId()).thenReturn("test-device-123");
        
        // Set up the cache manager to return our mock device
        when(cacheManager.getObject(eq(Device.class), eq(1L))).thenReturn(device);
        
        // Set up the tracer mock
        when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);
        when(spanBuilder.setParent(any())).thenReturn(spanBuilder);
        when(spanBuilder.setAttribute(anyString(), anyString())).thenReturn(spanBuilder);
        when(spanBuilder.setAttribute(anyString(), eq(1L))).thenReturn(spanBuilder);
        when(spanBuilder.startSpan()).thenReturn(span);
        
        // Set up the event producer to return a completed future
        when(eventProducer.publishEvent(any(Event.class), anyString()))
                .thenReturn(CompletableFuture.completedFuture(new Event("test", 1L)));
        
        // Create the alarm event handler with our mocks
        alarmEventHandler = new AlarmEventHandler(cacheManager, storage, tracer, meterRegistry, eventProducer);
    }
    
    /**
     * Test that the handler correctly processes a position with a general alarm attribute
     * and publishes an event to the message broker.
     */
    @Test
    public void testGeneralAlarm() {
        // Create a position with a general alarm
        Position position = createAlarmPosition(1L, Position.ALARM_GENERAL);
        
        // Set up the storage mock to successfully add the event
        when(storage.addObject(any(Event.class), anyMap())).thenReturn(true);
        
        // Process the position
        CompletableFuture<Event> future = alarmEventHandler.detectEvent(position, span, correlationId);
        
        // Wait for the future to complete
        Event event = future.join();
        
        // Verify the event was created correctly
        assertNotNull(event);
        assertEquals(Event.TYPE_ALARM, event.getType());
        assertEquals(1L, event.getDeviceId());
        assertEquals(Position.ALARM_GENERAL, event.getString(Position.KEY_ALARM));
        assertEquals(correlationId, event.getString(Event.KEY_CORRELATION_ID));
        
        // Verify the event was stored
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(storage).addObject(eventCaptor.capture(), anyMap());
        assertEquals(Event.TYPE_ALARM, eventCaptor.getValue().getType());
        assertEquals(Position.ALARM_GENERAL, eventCaptor.getValue().getString(Position.KEY_ALARM));
        
        // Verify the event was published to the message broker
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        assertEquals(Event.TYPE_ALARM, eventCaptor.getValue().getType());
        assertEquals(Position.ALARM_GENERAL, eventCaptor.getValue().getString(Position.KEY_ALARM));
    }
    
    /**
     * Test that the handler correctly processes a position with an SOS alarm attribute
     * and publishes an event to the message broker.
     */
    @Test
    public void testSosAlarm() {
        // Create a position with an SOS alarm
        Position position = createAlarmPosition(1L, Position.ALARM_SOS);
        
        // Set up the storage mock to successfully add the event
        when(storage.addObject(any(Event.class), anyMap())).thenReturn(true);
        
        // Process the position
        CompletableFuture<Event> future = alarmEventHandler.detectEvent(position, span, correlationId);
        
        // Wait for the future to complete
        Event event = future.join();
        
        // Verify the event was created correctly
        assertNotNull(event);
        assertEquals(Event.TYPE_ALARM, event.getType());
        assertEquals(1L, event.getDeviceId());
        assertEquals(Position.ALARM_SOS, event.getString(Position.KEY_ALARM));
        assertEquals(correlationId, event.getString(Event.KEY_CORRELATION_ID));
        
        // Verify the event was stored
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(storage).addObject(eventCaptor.capture(), anyMap());
        assertEquals(Event.TYPE_ALARM, eventCaptor.getValue().getType());
        assertEquals(Position.ALARM_SOS, eventCaptor.getValue().getString(Position.KEY_ALARM));
        
        // Verify the event was published to the message broker
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        assertEquals(Event.TYPE_ALARM, eventCaptor.getValue().getType());
        assertEquals(Position.ALARM_SOS, eventCaptor.getValue().getString(Position.KEY_ALARM));
    }
    
    /**
     * Test that the handler correctly processes a position with no alarm attribute
     * and does not publish an event to the message broker.
     */
    @Test
    public void testNoAlarm() {
        // Create a position with no alarm
        Position position = createPosition(1L);
        
        // Process the position
        CompletableFuture<Event> future = alarmEventHandler.detectEvent(position, span, correlationId);
        
        // Wait for the future to complete
        Event event = future.join();
        
        // Verify no event was created
        assertEquals(null, event);
    }
    
    /**
     * Test that the handler correctly processes a position with an alarm attribute
     * but the device is not found, and does not publish an event to the message broker.
     */
    @Test
    public void testDeviceNotFound() {
        // Create a position with an alarm
        Position position = createAlarmPosition(2L, Position.ALARM_GENERAL);
        
        // Set up the cache manager to return null for the device
        when(cacheManager.getObject(eq(Device.class), eq(2L))).thenReturn(null);
        
        // Process the position
        CompletableFuture<Event> future = alarmEventHandler.detectEvent(position, span, correlationId);
        
        // Wait for the future to complete
        Event event = future.join();
        
        // Verify no event was created
        assertEquals(null, event);
    }
    
    /**
     * Test that the handler correctly processes a position with an alarm attribute
     * and includes additional attributes in the event.
     */
    @Test
    public void testAlarmWithAttributes() {
        // Create a position with an alarm and additional attributes
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Position.KEY_ALARM, Position.ALARM_OVERSPEED);
        attributes.put("speed", 85.5);
        attributes.put("speedLimit", 70.0);
        Position position = createPosition(1L, null, 0, 0, attributes);
        
        // Set up the storage mock to successfully add the event
        when(storage.addObject(any(Event.class), anyMap())).thenReturn(true);
        
        // Process the position
        CompletableFuture<Event> future = alarmEventHandler.detectEvent(position, span, correlationId);
        
        // Wait for the future to complete
        Event event = future.join();
        
        // Verify the event was created correctly
        assertNotNull(event);
        assertEquals(Event.TYPE_ALARM, event.getType());
        assertEquals(1L, event.getDeviceId());
        assertEquals(Position.ALARM_OVERSPEED, event.getString(Position.KEY_ALARM));
        assertEquals(correlationId, event.getString(Event.KEY_CORRELATION_ID));
        
        // Verify the event was stored
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(storage).addObject(eventCaptor.capture(), anyMap());
        assertEquals(Event.TYPE_ALARM, eventCaptor.getValue().getType());
        assertEquals(Position.ALARM_OVERSPEED, eventCaptor.getValue().getString(Position.KEY_ALARM));
        
        // Verify the event was published to the message broker
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        assertEquals(Event.TYPE_ALARM, eventCaptor.getValue().getType());
        assertEquals(Position.ALARM_OVERSPEED, eventCaptor.getValue().getString(Position.KEY_ALARM));
    }
    
    /**
     * Test that the handler correctly processes a position message from the message broker.
     */
    @Test
    public void testProcessPositionMessage() {
        // Create a position with an alarm
        Position position = createAlarmPosition(1L, Position.ALARM_GENERAL);
        
        // Set up the storage mock to successfully add the event
        when(storage.addObject(any(Event.class), anyMap())).thenReturn(true);
        
        // Process the position message
        CompletableFuture<Event> future = alarmEventHandler.processPositionMessage(position, correlationId);
        
        // Wait for the future to complete
        Event event = future.join();
        
        // Verify the event was created correctly
        assertNotNull(event);
        assertEquals(Event.TYPE_ALARM, event.getType());
        assertEquals(1L, event.getDeviceId());
        assertEquals(Position.ALARM_GENERAL, event.getString(Position.KEY_ALARM));
        assertEquals(correlationId, event.getString(Event.KEY_CORRELATION_ID));
        
        // Verify the event was published to the message broker
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        assertEquals(Event.TYPE_ALARM, eventCaptor.getValue().getType());
        assertEquals(Position.ALARM_GENERAL, eventCaptor.getValue().getString(Position.KEY_ALARM));
    }
    
    /**
     * Test that the handler correctly handles storage exceptions when processing a position.
     */
    @Test
    public void testStorageException() {
        // Create a position with an alarm
        Position position = createAlarmPosition(1L, Position.ALARM_GENERAL);
        
        // Set up the storage mock to throw an exception
        when(storage.addObject(any(Event.class), anyMap()))
                .thenThrow(new RuntimeException("Storage error"));
        
        // Process the position
        CompletableFuture<Event> future = alarmEventHandler.detectEvent(position, span, correlationId);
        
        // Wait for the future to complete
        Event event = future.join();
        
        // Verify no event was returned due to the exception
        assertEquals(null, event);
        
        // Verify the span was marked with an error
        verify(span).recordException(any(Exception.class));
        verify(span).setStatus(eq(io.opentelemetry.api.trace.StatusCode.ERROR), anyString());
    }
}