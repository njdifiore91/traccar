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

public class CommandResultEventHandlerTest extends BaseTest {

    private CacheManager cacheManager;
    private Storage storage;
    private Tracer tracer;
    private MeterRegistry meterRegistry;
    private EventProducer eventProducer;
    private CommandResultEventHandler commandResultEventHandler;
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
        commandResultEventHandler = new CommandResultEventHandler(cacheManager, storage, tracer, meterRegistry, eventProducer);
    }

    private Position position(String time, String result, String command) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.setDeviceId(1);
        position.setId(100);
        position.setValid(true);
        position.set("result", result);
        if (command != null) {
            position.set("command", command);
        }
        return position;
    }

    @Test
    public void testCommandResultEventDetection() throws Exception {
        // Create a position with command result attributes
        Position position = position("2023-01-01 12:00:00", "success", "engineStop");
        
        // Set up correlation ID for tracing
        String correlationId = "test-correlation-id";
        
        // Process the position message
        CompletableFuture<Event> future = commandResultEventHandler.processPositionMessage(position, correlationId);
        
        // Verify event was created and published
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        
        // Verify event properties
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_COMMAND_RESULT, capturedEvent.getType());
        assertEquals("success", capturedEvent.getString("result"));
        assertEquals("engineStop", capturedEvent.getString("command"));
        assertEquals(1L, capturedEvent.getDeviceId());
        assertEquals(correlationId, capturedEvent.getString(Event.KEY_CORRELATION_ID));
    }

    @Test
    public void testCommandResultEventWithoutCommand() throws Exception {
        // Create a position with only result attribute (no command)
        Position position = position("2023-01-01 12:00:00", "failure", null);
        
        // Set up correlation ID for tracing
        String correlationId = "test-correlation-id";
        
        // Process the position message
        CompletableFuture<Event> future = commandResultEventHandler.processPositionMessage(position, correlationId);
        
        // Verify event was created and published
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        
        // Verify event properties
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_COMMAND_RESULT, capturedEvent.getType());
        assertEquals("failure", capturedEvent.getString("result"));
        assertEquals(null, capturedEvent.getString("command"));
        assertEquals(1L, capturedEvent.getDeviceId());
        assertEquals(correlationId, capturedEvent.getString(Event.KEY_CORRELATION_ID));
    }

    @Test
    public void testNoCommandResultEventWhenNoResultAttribute() throws Exception {
        // Create a position without result attribute
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse("2023-01-01 12:00:00"));
        position.setDeviceId(1);
        position.setId(100);
        position.setValid(true);
        
        // Set up correlation ID for tracing
        String correlationId = "test-correlation-id";
        
        // Process the position message
        CompletableFuture<Event> future = commandResultEventHandler.processPositionMessage(position, correlationId);
        
        // Verify no event was published
        Mockito.verify(eventProducer, Mockito.never()).publishEvent(any(Event.class), anyString());
    }

    @Test
    public void testCommandResultEventPublishedToMessageBroker() throws Exception {
        // Create a position with command result attributes
        Position position = position("2023-01-01 12:00:00", "success", "engineStop");
        
        // Set up correlation ID for tracing
        String correlationId = "test-correlation-id";
        
        // Process the position message
        CompletableFuture<Event> future = commandResultEventHandler.processPositionMessage(position, correlationId);
        
        // Verify event was published to the message broker with the correct event type as key
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq(correlationId));
        
        // Verify the event type is used as the message key
        Event capturedEvent = eventCaptor.getValue();
        assertEquals(Event.TYPE_COMMAND_RESULT, capturedEvent.getType());
    }
}