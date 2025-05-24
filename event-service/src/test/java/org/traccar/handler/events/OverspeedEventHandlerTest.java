package org.traccar.handler.events;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.traccar.BaseTest;
import org.traccar.messaging.EventProducer;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OverspeedEventHandlerTest extends BaseTest {

    private CacheManager cacheManager;
    private Storage storage;
    private Tracer tracer;
    private MeterRegistry meterRegistry;
    private EventProducer eventProducer;
    private OverspeedEventHandler overspeedEventHandler;
    private Span span;
    
    @BeforeEach
    public void setUp() {
        // Mock dependencies
        cacheManager = mock(CacheManager.class);
        storage = mock(Storage.class);
        tracer = mock(Tracer.class);
        meterRegistry = new SimpleMeterRegistry();
        eventProducer = mock(EventProducer.class);
        span = mock(Span.class);
        
        // Mock tracer behavior
        when(tracer.spanBuilder(anyString()))
                .thenReturn(Mockito.mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(anyString()).setSpanKind(any(SpanKind.class)))
                .thenReturn(Mockito.mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(anyString()).setSpanKind(any(SpanKind.class)).startSpan())
                .thenReturn(span);
        
        // Mock event producer behavior
        when(eventProducer.publishEvent(any(Event.class), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        
        // Create the handler under test
        overspeedEventHandler = new OverspeedEventHandler(cacheManager, storage, tracer, meterRegistry, eventProducer);
    }

    private Position position(String time, double speed) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.setSpeed(speed);
        position.setDeviceId(1);
        position.setId(100);
        position.setValid(true);
        return position;
    }

    @Test
    public void testOverspeedEventDetection() throws Exception {
        // Mock device with speed limit
        org.traccar.model.Device device = new org.traccar.model.Device();
        device.setId(1);
        device.set("speedLimit", 40.0);
        when(cacheManager.getObject(eq(org.traccar.model.Device.class), eq(1L)))
                .thenReturn(device);
        
        // Create a position exceeding the speed limit
        Position position = position("2023-01-01 12:00:00", 55.0);
        
        // Set up correlation ID for tracing
        String correlationId = "test-correlation-id";
        
        // Process the position message
        CompletableFuture<Event> future = overspeedEventHandler.processPositionMessage(position, correlationId);
        
        // Verify event was created and published
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        
        // Verify event properties
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_DEVICE_OVERSPEED, capturedEvent.getType());
        assertEquals(55.0, capturedEvent.getDouble("speed"), 0.1);
        assertEquals(40.0, capturedEvent.getDouble("speedLimit"), 0.1);
        assertEquals(1L, capturedEvent.getDeviceId());
        assertEquals(correlationId, capturedEvent.getString(Event.KEY_CORRELATION_ID));
    }

    @Test
    public void testNoOverspeedEventWhenSpeedBelowLimit() throws Exception {
        // Mock device with speed limit
        org.traccar.model.Device device = new org.traccar.model.Device();
        device.setId(1);
        device.set("speedLimit", 60.0);
        when(cacheManager.getObject(eq(org.traccar.model.Device.class), eq(1L)))
                .thenReturn(device);
        
        // Create a position below the speed limit
        Position position = position("2023-01-01 12:00:00", 55.0);
        
        // Set up correlation ID for tracing
        String correlationId = "test-correlation-id";
        
        // Process the position message
        CompletableFuture<Event> future = overspeedEventHandler.processPositionMessage(position, correlationId);
        
        // Verify no event was published
        Mockito.verify(eventProducer, Mockito.never()).publishEvent(any(Event.class), anyString());
    }

    @Test
    public void testNoOverspeedEventWhenNoSpeedLimit() throws Exception {
        // Mock device with no speed limit
        org.traccar.model.Device device = new org.traccar.model.Device();
        device.setId(1);
        // No speed limit set
        when(cacheManager.getObject(eq(org.traccar.model.Device.class), eq(1L)))
                .thenReturn(device);
        
        // Create a position with speed
        Position position = position("2023-01-01 12:00:00", 100.0);
        
        // Set up correlation ID for tracing
        String correlationId = "test-correlation-id";
        
        // Process the position message
        CompletableFuture<Event> future = overspeedEventHandler.processPositionMessage(position, correlationId);
        
        // Verify no event was published
        Mockito.verify(eventProducer, Mockito.never()).publishEvent(any(Event.class), anyString());
    }

    @Test
    public void testOverspeedEventPublishedToMessageBroker() throws Exception {
        // Mock device with speed limit
        org.traccar.model.Device device = new org.traccar.model.Device();
        device.setId(1);
        device.set("speedLimit", 40.0);
        when(cacheManager.getObject(eq(org.traccar.model.Device.class), eq(1L)))
                .thenReturn(device);
        
        // Create a position exceeding the speed limit
        Position position = position("2023-01-01 12:00:00", 55.0);
        
        // Set up correlation ID for tracing
        String correlationId = "test-correlation-id";
        
        // Process the position message
        CompletableFuture<Event> future = overspeedEventHandler.processPositionMessage(position, correlationId);
        
        // Verify event was published to the message broker with the correct event type as key
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        
        // Verify the event type is used as the message key
        Event capturedEvent = eventCaptor.getValue();
        assertEquals(Event.TYPE_DEVICE_OVERSPEED, capturedEvent.getType());
    }
}