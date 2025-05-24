package org.traccar.handler.events;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.traccar.BaseTest;
import org.traccar.messaging.EventProducer;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

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

public class IgnitionEventHandlerTest extends BaseTest {

    private CacheManager cacheManager;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private RetryRegistry retryRegistry;
    private Tracer tracer;
    private Meter meter;
    private MeterRegistry meterRegistry;
    private EventProducer eventProducer;
    private IgnitionEventHandler ignitionEventHandler;
    private Span span;
    private Counter ignitionOnCounter;
    private Counter ignitionOffCounter;
    private Timer ignitionEventProcessingTimer;
    
    @BeforeEach
    public void setUp() {
        // Mock dependencies
        cacheManager = mock(CacheManager.class);
        circuitBreakerRegistry = mock(CircuitBreakerRegistry.class);
        retryRegistry = mock(RetryRegistry.class);
        tracer = mock(Tracer.class);
        meter = mock(Meter.class);
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
        ignitionOnCounter = meterRegistry.counter("events.ignition.on");
        ignitionOffCounter = meterRegistry.counter("events.ignition.off");
        ignitionEventProcessingTimer = meterRegistry.timer("events.ignition.processing.time");
        
        // Create the handler under test
        ignitionEventHandler = new IgnitionEventHandler(cacheManager, circuitBreakerRegistry, retryRegistry, tracer, meter);
    }

    private Position position(String time, Boolean ignition) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.set(Position.KEY_IGNITION, ignition);
        position.setDeviceId(1);
        position.setId(100);
        position.setValid(true);
        return position;
    }
    
    private void mockPreviousPosition(Position previousPosition) {
        // Mock cache manager to return the previous position
        when(cacheManager.getPosition(eq(1L)))
                .thenReturn(previousPosition);
    }

    @Test
    public void testIgnitionOnEventDetection() throws Exception {
        // Create a previous position with ignition=false
        Position previousPosition = position("2023-01-01 11:55:00", false);
        mockPreviousPosition(previousPosition);
        
        // Create a current position with ignition=true
        Position currentPosition = position("2023-01-01 12:00:00", true);
        
        // Create a mock callback
        IgnitionEventHandler.Callback callback = mock(IgnitionEventHandler.Callback.class);
        
        // Process the position
        ignitionEventHandler.analyzePosition(currentPosition, callback);
        
        // Capture and verify the event
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(callback).eventDetected(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_IGNITION_ON, capturedEvent.getType());
        assertEquals(1L, capturedEvent.getDeviceId());
    }

    @Test
    public void testIgnitionOffEventDetection() throws Exception {
        // Create a previous position with ignition=true
        Position previousPosition = position("2023-01-01 11:55:00", true);
        mockPreviousPosition(previousPosition);
        
        // Create a current position with ignition=false
        Position currentPosition = position("2023-01-01 12:00:00", false);
        
        // Create a mock callback
        IgnitionEventHandler.Callback callback = mock(IgnitionEventHandler.Callback.class);
        
        // Process the position
        ignitionEventHandler.analyzePosition(currentPosition, callback);
        
        // Capture and verify the event
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(callback).eventDetected(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_IGNITION_OFF, capturedEvent.getType());
        assertEquals(1L, capturedEvent.getDeviceId());
    }

    @Test
    public void testNoEventWhenIgnitionUnchanged() throws Exception {
        // Create a previous position with ignition=true
        Position previousPosition = position("2023-01-01 11:55:00", true);
        mockPreviousPosition(previousPosition);
        
        // Create a current position with ignition=true (unchanged)
        Position currentPosition = position("2023-01-01 12:00:00", true);
        
        // Create a mock callback
        IgnitionEventHandler.Callback callback = mock(IgnitionEventHandler.Callback.class);
        
        // Process the position
        ignitionEventHandler.analyzePosition(currentPosition, callback);
        
        // Verify no event was created
        verify(callback, never()).eventDetected(any(Event.class));
    }

    @Test
    public void testNoEventWhenIgnitionNull() throws Exception {
        // Create a previous position with ignition=true
        Position previousPosition = position("2023-01-01 11:55:00", true);
        mockPreviousPosition(previousPosition);
        
        // Create a current position with ignition=null
        Position currentPosition = position("2023-01-01 12:00:00", null);
        
        // Create a mock callback
        IgnitionEventHandler.Callback callback = mock(IgnitionEventHandler.Callback.class);
        
        // Process the position
        ignitionEventHandler.analyzePosition(currentPosition, callback);
        
        // Verify no event was created
        verify(callback, never()).eventDetected(any(Event.class));
    }

    @Test
    public void testNoEventWhenPreviousPositionNull() throws Exception {
        // Mock cache manager to return null for previous position
        when(cacheManager.getPosition(eq(1L)))
                .thenReturn(null);
        
        // Create a current position with ignition=true
        Position currentPosition = position("2023-01-01 12:00:00", true);
        
        // Create a mock callback
        IgnitionEventHandler.Callback callback = mock(IgnitionEventHandler.Callback.class);
        
        // Process the position
        ignitionEventHandler.analyzePosition(currentPosition, callback);
        
        // Verify no event was created
        verify(callback, never()).eventDetected(any(Event.class));
    }

    @Test
    public void testEventPublishedToMessageBroker() throws Exception {
        // Create a previous position with ignition=false
        Position previousPosition = position("2023-01-01 11:55:00", false);
        mockPreviousPosition(previousPosition);
        
        // Create a current position with ignition=true
        Position currentPosition = position("2023-01-01 12:00:00", true);
        
        // Set up span to return a trace ID for correlation
        when(span.getSpanContext().getTraceId()).thenReturn("test-trace-id");
        
        // Create a custom callback that will publish to the message broker
        IgnitionEventHandler.Callback callback = event -> {
            // Add correlation ID to the event
            event.set("correlationId", "test-trace-id");
            // Publish to message broker
            eventProducer.publishEvent(event, "test-trace-id");
        };
        
        // Process the position
        ignitionEventHandler.analyzePosition(currentPosition, callback);
        
        // Capture the event published to the message broker
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventProducer).publishEvent(eventCaptor.capture(), eq("test-trace-id"));
        
        // Verify the event properties
        Event capturedEvent = eventCaptor.getValue();
        assertNotNull(capturedEvent);
        assertEquals(Event.TYPE_IGNITION_ON, capturedEvent.getType());
        assertEquals(1L, capturedEvent.getDeviceId());
        assertEquals("test-trace-id", capturedEvent.getString("correlationId"));
    }

    @Test
    public void testOpenTelemetryIntegration() throws Exception {
        // Create a previous position with ignition=false
        Position previousPosition = position("2023-01-01 11:55:00", false);
        mockPreviousPosition(previousPosition);
        
        // Create a current position with ignition=true
        Position currentPosition = position("2023-01-01 12:00:00", true);
        
        // Create a mock callback
        IgnitionEventHandler.Callback callback = mock(IgnitionEventHandler.Callback.class);
        
        // Process the position
        ignitionEventHandler.analyzePosition(currentPosition, callback);
        
        // Verify span was created and attributes were set
        verify(span).setAttribute(eq(AttributeKey.longKey("device.id")), eq(1L));
        verify(span).setAttribute(eq(AttributeKey.booleanKey("ignition.state")), eq(true));
        
        // Verify span was ended
        verify(span).end();
    }
}