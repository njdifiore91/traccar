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
 * Tests the integration between the position-service and the event-service for overspeed events.
 * Verifies that positions with speed attributes are correctly published to the message broker
 * for consumption by the event-service.
 */
@ExtendWith(MockitoExtension.class)
public class OverspeedEventHandlerTest extends BaseTest {

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

    private Position position(String time, double speed) throws ParseException {
        Position position = new Position();
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.setSpeed(speed);
        position.setDeviceId(1);
        return position;
    }

    /**
     * Tests that positions with speed data are correctly published to the message broker
     * with all necessary attributes for overspeed detection by the event-service.
     */
    @Test
    public void testPublishPositionWithSpeedData() throws Exception {
        // Create a position with speed data
        Position position = position("2023-01-01 00:00:00", 55);
        
        // Add a speed limit attribute that would be used by the event-service
        position.set("speedLimit", 40);
        
        // Forward the position to the message broker
        positionForwarder.forwardPosition(position);
        
        // Verify the position was published to the message broker
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher).publish(eq("positions"), positionCaptor.capture());
        
        // Verify the published position contains all necessary attributes for overspeed detection
        Position publishedPosition = positionCaptor.getValue();
        assertNotNull(publishedPosition);
        assertEquals(55, publishedPosition.getSpeed(), 0.1);
        assertEquals(40, publishedPosition.getDouble("speedLimit"), 0.1);
        assertEquals(1, publishedPosition.getDeviceId());
    }

    /**
     * Tests that distributed tracing context is properly propagated when publishing positions
     * to the message broker for consumption by the event-service.
     */
    @Test
    public void testTracingContextPropagation() throws Exception {
        // Create a position with speed data
        Position position = position("2023-01-01 00:00:00", 55);
        
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
     * Tests that correlation IDs are properly propagated when publishing positions
     * to ensure end-to-end traceability across microservices.
     */
    @Test
    public void testCorrelationIdPropagation() throws Exception {
        // Create a position with speed data
        Position position = position("2023-01-01 00:00:00", 55);
        
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
     * Tests that positions with different speed values are correctly published
     * to verify compatibility with the event-service's overspeed detection.
     */
    @Test
    public void testPublishPositionsWithDifferentSpeeds() throws Exception {
        // Create positions with different speeds
        Position position1 = position("2023-01-01 00:00:00", 30); // Below speed limit
        Position position2 = position("2023-01-01 00:01:00", 50); // Above speed limit
        
        // Set speed limit
        double speedLimit = 40;
        position1.set("speedLimit", speedLimit);
        position2.set("speedLimit", speedLimit);
        
        // Forward positions to the message broker
        positionForwarder.forwardPosition(position1);
        positionForwarder.forwardPosition(position2);
        
        // Verify both positions were published
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher).publish(eq("positions"), positionCaptor.capture());
        verify(messagePublisher).publish(eq("positions"), positionCaptor.capture());
        
        // Verify the published positions have the correct speed values
        assertEquals(2, positionCaptor.getAllValues().size());
        assertEquals(30, positionCaptor.getAllValues().get(0).getSpeed(), 0.1);
        assertEquals(50, positionCaptor.getAllValues().get(1).getSpeed(), 0.1);
    }
}