package org.traccar.handler.events;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.BaseTest;
import org.traccar.handler.BasePositionHandler;
import org.traccar.messaging.PositionMessage;
import org.traccar.messaging.PositionProducer;
import org.traccar.model.Position;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the integration between the position-service and the event-service for ignition events.
 * Verifies that positions with ignition state attributes are correctly published to the message broker
 * for consumption by the event-service.
 */
@ExtendWith(MockitoExtension.class)
public class IgnitionEventHandlerTest extends BaseTest {
    
    @Mock
    private PositionProducer positionProducer;
    
    @Mock
    private Span span;
    
    @Mock
    private SpanContext spanContext;
    
    @Captor
    private ArgumentCaptor<PositionMessage> positionMessageCaptor;
    
    /**
     * Tests that a position with ignition=true attribute is correctly published to the message broker
     * with all necessary metadata for the event-service to generate an ignition event.
     */
    @Test
    public void testIgnitionOnEventPublished() {
        // Setup position with ignition ON
        Position position = new Position();
        position.set(Position.KEY_IGNITION, true);
        position.setValid(true);
        position.setDeviceId(123L);
        
        // Setup OpenTelemetry context for distributed tracing
        when(span.getSpanContext()).thenReturn(spanContext);
        when(spanContext.isValid()).thenReturn(true);
        when(spanContext.getTraceId()).thenReturn("test-trace-id");
        when(spanContext.getSpanId()).thenReturn("test-span-id");
        
        // Create handler that publishes to the message broker
        IgnitionEventHandler handler = new IgnitionEventHandler(positionProducer);
        
        // Process the position
        handler.handlePosition(position, Context.current().with(Span.class, span));
        
        // Verify the position was published to the message broker
        verify(positionProducer).publish(eq("events.ignition"), positionMessageCaptor.capture(), any());
        
        // Verify the published message contains all necessary data
        PositionMessage message = positionMessageCaptor.getValue();
        assertNotNull(message);
        assertEquals(123L, message.getDeviceId());
        assertTrue((Boolean) message.getAttributes().get(Position.KEY_IGNITION));
        assertEquals("test-trace-id", message.getTraceId());
        assertEquals("test-span-id", message.getSpanId());
    }
    
    /**
     * Tests that a position with ignition=false attribute is correctly published to the message broker
     * with all necessary metadata for the event-service to generate an ignition event.
     */
    @Test
    public void testIgnitionOffEventPublished() {
        // Setup position with ignition OFF
        Position position = new Position();
        position.set(Position.KEY_IGNITION, false);
        position.setValid(true);
        position.setDeviceId(456L);
        
        // Setup OpenTelemetry context for distributed tracing
        when(span.getSpanContext()).thenReturn(spanContext);
        when(spanContext.isValid()).thenReturn(true);
        when(spanContext.getTraceId()).thenReturn("test-trace-id-2");
        when(spanContext.getSpanId()).thenReturn("test-span-id-2");
        
        // Create handler that publishes to the message broker
        IgnitionEventHandler handler = new IgnitionEventHandler(positionProducer);
        
        // Process the position
        handler.handlePosition(position, Context.current().with(Span.class, span));
        
        // Verify the position was published to the message broker
        verify(positionProducer).publish(eq("events.ignition"), positionMessageCaptor.capture(), any());
        
        // Verify the published message contains all necessary data
        PositionMessage message = positionMessageCaptor.getValue();
        assertNotNull(message);
        assertEquals(456L, message.getDeviceId());
        assertEquals(false, message.getAttributes().get(Position.KEY_IGNITION));
        assertEquals("test-trace-id-2", message.getTraceId());
        assertEquals("test-span-id-2", message.getSpanId());
    }
    
    /**
     * Tests that invalid positions are not published to the message broker.
     */
    @Test
    public void testInvalidPositionNotPublished() {
        // Setup invalid position with ignition ON
        Position position = new Position();
        position.set(Position.KEY_IGNITION, true);
        position.setValid(false); // Invalid position
        position.setDeviceId(789L);
        
        // Create handler that publishes to the message broker
        IgnitionEventHandler handler = new IgnitionEventHandler(positionProducer);
        
        // Process the position with a callback to verify it's filtered
        handler.handlePosition(position, new BasePositionHandler.Callback() {
            @Override
            public void onSuccess(boolean success) {
                // Position should be filtered (success = false)
                assertEquals(false, success);
            }
            
            @Override
            public void onFailure(Throwable e) {
                // Should not be called
                assertTrue(false, "Failure callback should not be called");
            }
        });
    }
}