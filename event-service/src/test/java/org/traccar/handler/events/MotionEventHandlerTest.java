package org.traccar.handler.events;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.traccar.BaseTest;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.EventProducer;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.reports.common.TripsConfig;
import org.traccar.session.cache.CacheManager;
import org.traccar.session.state.MotionState;
import org.traccar.storage.Storage;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MotionEventHandlerTest extends BaseTest {

    private CacheManager cacheManager;
    private Storage storage;
    private Tracer tracer;
    private TextMapPropagator propagator;
    private MeterRegistry meterRegistry;
    private EventProducer eventProducer;
    private MotionEventHandler motionEventHandler;
    private Span span;
    private Timer motionDetectionTimer;
    private Counter motionStartCounter;
    private Counter motionStopCounter;
    
    @BeforeEach
    public void setUp() {
        // Mock dependencies
        cacheManager = mock(CacheManager.class);
        storage = mock(Storage.class);
        tracer = mock(Tracer.class);
        propagator = mock(TextMapPropagator.class);
        meterRegistry = new SimpleMeterRegistry();
        eventProducer = mock(EventProducer.class);
        span = mock(Span.class);
        
        // Mock tracer behavior
        when(tracer.spanBuilder(anyString()))
                .thenReturn(Mockito.mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(anyString()).setParent(any(Context.class)))
                .thenReturn(Mockito.mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(anyString()).setSpanKind(any(SpanKind.class)))
                .thenReturn(Mockito.mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(anyString()).setSpanKind(any(SpanKind.class)).startSpan())
                .thenReturn(span);
        
        // Mock event producer behavior
        when(eventProducer.publishEvent(any(Event.class), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        
        // Mock metrics
        motionDetectionTimer = meterRegistry.timer("motion.detection.duration");
        motionStartCounter = meterRegistry.counter("motion.events", "type", "start");
        motionStopCounter = meterRegistry.counter("motion.events", "type", "stop");
        
        // Create the handler under test
        motionEventHandler = new MotionEventHandler(cacheManager, storage, tracer, propagator, meterRegistry);
    }

    private Position position(String time, boolean motion) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.set(Position.KEY_MOTION, motion);
        position.setDeviceId(1);
        position.setId(100);
        position.setValid(true);
        return position;
    }
    
    private void mockDeviceAndPositionChecks(Device device, boolean isLatest, boolean processInvalid) {
        // Mock device retrieval
        when(cacheManager.getObject(eq(Device.class), eq(1L)))
                .thenReturn(device);
        
        // Mock position check
        when(PositionUtil.isLatest(eq(cacheManager), any(Position.class)))
                .thenReturn(isLatest);
        
        // Mock attribute check
        when(AttributeUtil.lookup(eq(cacheManager), eq(Keys.EVENT_MOTION_PROCESS_INVALID_POSITIONS), eq(1L)))
                .thenReturn(processInvalid);
    }

    @Test
    public void testMotionStartEventDetection() throws Exception {
        // Create a device with initial state of not moving
        Device device = new Device();
        device.setId(1);
        device.setMotionState(MotionState.STATE_IDLE);
        device.setMotionTime(System.currentTimeMillis() - 10000); // Set motion time in the past
        device.setMotionDistance(0.0);
        
        // Mock device and position checks
        mockDeviceAndPositionChecks(device, true, true);
        
        // Create a position with motion=true
        Position position = position("2023-01-01 12:00:00", true);
        
        // Create a mock callback
        MotionEventHandler.Callback callback = mock(MotionEventHandler.Callback.class);
        
        // Process the position
        motionEventHandler.onPosition(position, callback);
        
        // Verify device state was updated in storage
        verify(storage).updateObject(eq(device), any(Request.class));
        
        // Capture and verify the event
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(callback).eventDetected(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_DEVICE_MOVING, capturedEvent.getType());
        assertEquals(1L, capturedEvent.getDeviceId());
        
        // Verify metrics were recorded
        assertEquals(1, motionStartCounter.count());
    }

    @Test
    public void testMotionStopEventDetection() throws Exception {
        // Create a device with initial state of moving
        Device device = new Device();
        device.setId(1);
        device.setMotionState(MotionState.STATE_MOVING);
        device.setMotionTime(System.currentTimeMillis() - 10000); // Set motion time in the past
        device.setMotionDistance(100.0); // Set some distance traveled
        
        // Mock device and position checks
        mockDeviceAndPositionChecks(device, true, true);
        
        // Create a position with motion=false
        Position position = position("2023-01-01 12:00:00", false);
        
        // Create a mock callback
        MotionEventHandler.Callback callback = mock(MotionEventHandler.Callback.class);
        
        // Process the position
        motionEventHandler.onPosition(position, callback);
        
        // Verify device state was updated in storage
        verify(storage).updateObject(eq(device), any(Request.class));
        
        // Capture and verify the event
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(callback).eventDetected(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_DEVICE_STOPPED, capturedEvent.getType());
        assertEquals(1L, capturedEvent.getDeviceId());
        
        // Verify metrics were recorded
        assertEquals(1, motionStopCounter.count());
    }

    @Test
    public void testNoEventWhenPositionNotLatest() throws Exception {
        // Create a device
        Device device = new Device();
        device.setId(1);
        
        // Mock device retrieval
        when(cacheManager.getObject(eq(Device.class), eq(1L)))
                .thenReturn(device);
        
        // Mock position check to return false (not latest)
        when(PositionUtil.isLatest(eq(cacheManager), any(Position.class)))
                .thenReturn(false);
        
        // Create a position
        Position position = position("2023-01-01 12:00:00", true);
        
        // Create a callback
        MotionEventHandler.Callback callback = mock(MotionEventHandler.Callback.class);
        
        // Process the position
        motionEventHandler.onPosition(position, callback);
        
        // Verify no event was created
        verify(callback, never()).eventDetected(any(Event.class));
        
        // Verify device state was not updated
        verify(storage, never()).updateObject(any(Device.class), any(Request.class));
    }

    @Test
    public void testNoEventWhenInvalidPositionAndNotProcessingInvalid() throws Exception {
        // Create a device
        Device device = new Device();
        device.setId(1);
        
        // Mock device retrieval
        when(cacheManager.getObject(eq(Device.class), eq(1L)))
                .thenReturn(device);
        
        // Mock position check to return true (latest)
        when(PositionUtil.isLatest(eq(cacheManager), any(Position.class)))
                .thenReturn(true);
        
        // Mock attribute check to return false (don't process invalid)
        when(AttributeUtil.lookup(eq(cacheManager), eq(Keys.EVENT_MOTION_PROCESS_INVALID_POSITIONS), eq(1L)))
                .thenReturn(false);
        
        // Create an invalid position
        Position position = position("2023-01-01 12:00:00", true);
        position.setValid(false);
        
        // Create a callback
        MotionEventHandler.Callback callback = mock(MotionEventHandler.Callback.class);
        
        // Process the position
        motionEventHandler.onPosition(position, callback);
        
        // Verify no event was created
        verify(callback, never()).eventDetected(any(Event.class));
        
        // Verify device state was not updated
        verify(storage, never()).updateObject(any(Device.class), any(Request.class));
    }

    @Test
    public void testEventPublishedToMessageBroker() throws Exception {
        // Create a device with initial state of not moving
        Device device = new Device();
        device.setId(1);
        device.setMotionState(MotionState.STATE_IDLE);
        device.setMotionTime(System.currentTimeMillis() - 10000);
        device.setMotionDistance(0.0);
        
        // Mock device and position checks
        mockDeviceAndPositionChecks(device, true, true);
        
        // Create a position with motion=true
        Position position = position("2023-01-01 12:00:00", true);
        
        // Set up span to return a trace ID for correlation
        when(span.getSpanContext().getTraceId()).thenReturn("test-trace-id");
        
        // Create a custom callback that will publish to the message broker
        MotionEventHandler.Callback callback = event -> {
            // Add correlation ID to the event
            event.set("correlationId", "test-trace-id");
            // Publish to message broker
            eventProducer.publishEvent(event, "test-trace-id");
        };
        
        // Process the position
        motionEventHandler.onPosition(position, callback);
        
        // Capture the event published to the message broker
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq("test-trace-id"));
        
        // Verify the event properties
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_DEVICE_MOVING, capturedEvent.getType());
        assertEquals(1L, capturedEvent.getDeviceId());
        assertEquals("test-trace-id", capturedEvent.getString("correlationId"));
    }

    @Test
    public void testOpenTelemetryIntegration() throws Exception {
        // Create a device with initial state of not moving
        Device device = new Device();
        device.setId(1);
        device.setMotionState(MotionState.STATE_IDLE);
        
        // Mock device and position checks
        mockDeviceAndPositionChecks(device, true, true);
        
        // Create a position with motion=true
        Position position = position("2023-01-01 12:00:00", true);
        
        // Create a callback
        MotionEventHandler.Callback callback = mock(MotionEventHandler.Callback.class);
        
        // Process the position
        motionEventHandler.onPosition(position, callback);
        
        // Verify span was created and attributes were set
        verify(span).setAttribute(eq(AttributeKey.stringKey("device.id")), eq("1"));
        
        // Verify span was ended
        verify(span).end();
    }

    @Test
    public void testNoStateChangeNoEvent() throws Exception {
        // Create a device already in motion state
        Device device = new Device();
        device.setId(1);
        device.setMotionState(MotionState.STATE_MOVING);
        device.setMotionTime(System.currentTimeMillis() - 10000);
        device.setMotionDistance(100.0);
        
        // Mock device and position checks
        mockDeviceAndPositionChecks(device, true, true);
        
        // Create a position with motion=true (same as current state)
        Position position = position("2023-01-01 12:00:00", true);
        
        // Create a callback
        MotionEventHandler.Callback callback = mock(MotionEventHandler.Callback.class);
        
        // Process the position
        motionEventHandler.onPosition(position, callback);
        
        // Verify no event was created
        verify(callback, never()).eventDetected(any(Event.class));
    }
}