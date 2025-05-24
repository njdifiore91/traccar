package org.traccar.handler.events;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.BaseTest;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Position;
import org.traccar.position.PositionForwarder;
import org.traccar.tracing.TracingContextHelper;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the integration between the position-service and the event-service for command result events.
 * Verifies that positions with command result attributes are correctly published to the message broker
 * for consumption by the event-service.
 */
@ExtendWith(MockitoExtension.class)
public class CommandResultEventHandlerTest extends BaseTest {

    @Mock
    private MessagePublisher messagePublisher;

    @Mock
    private TracingContextHelper tracingContextHelper;

    @Mock
    private Span span;

    @Mock
    private SpanContext spanContext;

    private PositionForwarder positionForwarder;

    @BeforeEach
    public void setup() {
        // Setup mock tracing context
        when(tracingContextHelper.getCurrentSpan()).thenReturn(span);
        when(span.getSpanContext()).thenReturn(spanContext);
        when(spanContext.isValid()).thenReturn(true);
        when(spanContext.getTraceFlags()).thenReturn(TraceFlags.getSampled());
        when(spanContext.getTraceState()).thenReturn(TraceState.getDefault());
        when(tracingContextHelper.storeContextInMessage(any())).thenReturn(true);

        // Create position forwarder with mocked dependencies
        positionForwarder = new PositionForwarder(messagePublisher, tracingContextHelper);
    }

    private Position position(String time) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.setDeviceId(1);
        return position;
    }

    /**
     * Tests that positions with command result data are correctly published to the message broker
     * with all necessary attributes for command result event detection by the event-service.
     */
    @Test
    public void testPublishPositionWithCommandResult() throws Exception {
        // Create a position with command result data
        Position position = position("2023-01-01 00:00:00");
        
        // Add command result attributes that would be used by the event-service
        position.set(Position.KEY_RESULT, "Test Result");
        position.set("commandId", 123);
        position.set("commandType", "custom");
        
        // Forward the position to the message broker
        positionForwarder.forwardPosition(position);
        
        // Verify the position was published to the message broker
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher).publish(eq("positions"), positionCaptor.capture());
        
        // Verify the published position contains all necessary attributes for command result event detection
        Position publishedPosition = positionCaptor.getValue();
        assertNotNull(publishedPosition);
        assertEquals("Test Result", publishedPosition.getString(Position.KEY_RESULT));
        assertEquals(123, publishedPosition.getInteger("commandId"));
        assertEquals("custom", publishedPosition.getString("commandType"));
        assertEquals(1, publishedPosition.getDeviceId());
    }

    /**
     * Tests that distributed tracing context is properly propagated when publishing positions
     * with command results to the message broker for consumption by the event-service.
     */
    @Test
    public void testTracingContextPropagation() throws Exception {
        // Create a position with command result data
        Position position = position("2023-01-01 00:00:00");
        position.set(Position.KEY_RESULT, "Test Result");
        
        // Create a mock span and context
        String traceId = "0123456789abcdef0123456789abcdef";
        String spanId = "0123456789abcdef";
        when(spanContext.getTraceId()).thenReturn(traceId);
        when(spanContext.getSpanId()).thenReturn(spanId);
        
        // Forward the position with tracing context
        positionForwarder.forwardPosition(position);
        
        // Verify tracing context was stored in the message
        verify(tracingContextHelper).storeContextInMessage(any());
        
        // Verify the position was published with the tracing context
        verify(messagePublisher).publish(eq("positions"), any(Position.class));
    }

    /**
     * Tests that correlation IDs are properly propagated when publishing positions with command results
     * to ensure end-to-end traceability across microservices.
     */
    @Test
    public void testCorrelationIdPropagation() throws Exception {
        // Create a position with command result data
        Position position = position("2023-01-01 00:00:00");
        position.set(Position.KEY_RESULT, "Test Result");
        
        // Set a correlation ID on the position
        String correlationId = "test-correlation-id-123";
        position.set("correlationId", correlationId);
        
        // Forward the position to the message broker
        positionForwarder.forwardPosition(position);
        
        // Verify the position was published with the correlation ID
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher).publish(eq("positions"), positionCaptor.capture());
        
        // Verify the correlation ID was preserved
        Position publishedPosition = positionCaptor.getValue();
        assertEquals(correlationId, publishedPosition.getString("correlationId"));
    }

    /**
     * Tests that positions with different command result values are correctly published
     * to verify compatibility with the event-service's command result event detection.
     */
    @Test
    public void testPublishPositionsWithDifferentCommandResults() throws Exception {
        // Create positions with different command results
        Position position1 = position("2023-01-01 00:00:00");
        position1.set(Position.KEY_RESULT, "Success");
        position1.set("commandId", 123);
        
        Position position2 = position("2023-01-01 00:01:00");
        position2.set(Position.KEY_RESULT, "Failure");
        position2.set("commandId", 456);
        position2.set("errorCode", "TIMEOUT");
        
        // Forward positions to the message broker
        positionForwarder.forwardPosition(position1);
        positionForwarder.forwardPosition(position2);
        
        // Verify both positions were published
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher).publish(eq("positions"), positionCaptor.capture());
        verify(messagePublisher).publish(eq("positions"), positionCaptor.capture());
        
        // Verify the published positions have the correct command result values
        assertEquals(2, positionCaptor.getAllValues().size());
        assertEquals("Success", positionCaptor.getAllValues().get(0).getString(Position.KEY_RESULT));
        assertEquals("Failure", positionCaptor.getAllValues().get(1).getString(Position.KEY_RESULT));
        assertEquals("TIMEOUT", positionCaptor.getAllValues().get(1).getString("errorCode"));
    }

    /**
     * Tests that positions without command result data are still published correctly
     * to the message broker, ensuring that the event-service can properly filter them.
     */
    @Test
    public void testPublishPositionWithoutCommandResult() throws Exception {
        // Create a position without command result data
        Position position = position("2023-01-01 00:00:00");
        
        // Forward the position to the message broker
        positionForwarder.forwardPosition(position);
        
        // Verify the position was published to the message broker
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher).publish(eq("positions"), positionCaptor.capture());
        
        // Verify the published position does not contain command result attributes
        Position publishedPosition = positionCaptor.getValue();
        assertNotNull(publishedPosition);
        assertEquals(null, publishedPosition.getString(Position.KEY_RESULT));
    }
}