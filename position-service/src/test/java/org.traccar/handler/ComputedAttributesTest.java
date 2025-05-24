package org.traccar.handler;

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
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Attribute;
import org.traccar.model.Position;

import java.util.Date;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the ComputedAttributes handler in the position-service which processes position data
 * to calculate additional attributes based on expressions. This test ensures that the handler
 * correctly evaluates expressions and adds computed attributes to positions before they are
 * published to the message broker for consumption by other services.
 */
@ExtendWith(MockitoExtension.class)
public class ComputedAttributesTest {

    @Mock
    private Config config;

    @Mock
    private MessagePublisher messagePublisher;

    @Mock
    private Span span;

    private ComputedAttributesHandler handler;
    private Position position;
    private Attribute attribute;
    private Date date;

    @BeforeEach
    public void setUp() {
        // Initialize handler with mocks
        handler = new ComputedAttributesHandler(config, messagePublisher, true);

        // Create a test position with various attributes
        date = new Date();
        position = new Position();
        position.setTime(date);
        position.setSpeed(42);
        position.setValid(false);
        position.set("adc1", 128);
        position.set("booleanFlag", true);
        position.set("adc2", 100);
        position.set("bitFlag", 7);
        position.set("event", 42);
        position.set("result", "success");

        // Create a test attribute
        attribute = new Attribute();
    }

    /**
     * Tests the core functionality of computing attributes based on expressions.
     * This verifies that the handler correctly evaluates different types of expressions
     * and returns the expected values.
     */
    @Test
    public void testComputeAttribute() {
        // Test simple attribute reference
        attribute.setExpression("adc1");
        assertEquals(128, handler.computeAttribute(attribute, position));

        // Test boolean negation
        attribute.setExpression("!booleanFlag");
        assertEquals(false, handler.computeAttribute(attribute, position));

        // Test arithmetic expression
        attribute.setExpression("adc2 * 2 + 50");
        assertEquals(250, handler.computeAttribute(attribute, position));

        // Test bitwise operation
        attribute.setExpression("(bitFlag & 4) != 0");
        assertEquals(true, handler.computeAttribute(attribute, position));

        // Test conditional expression
        attribute.setExpression("event == 42 ? \"lowBattery\" : null");
        assertEquals("lowBattery", handler.computeAttribute(attribute, position));

        // Test logical expression with position properties
        attribute.setExpression("speed > 5 && valid");
        assertEquals(false, handler.computeAttribute(attribute, position));

        // Test date property access
        attribute.setExpression("fixTime");
        assertEquals(date, handler.computeAttribute(attribute, position));

        // Test math function
        attribute.setExpression("math:pow(adc1, 2)");
        assertEquals(16384.0, handler.computeAttribute(attribute, position));
    }

    /**
     * Tests that modification expressions are not allowed to change position attributes.
     * This ensures that computed attributes are read-only and don't modify the original position data.
     */
    @Test
    public void testModificationExpressions() {
        // Test assignment expression (should not modify position)
        attribute.setExpression("adc1 = 256");
        handler.computeAttribute(attribute, position);
        assertEquals(128, position.getInteger("adc1"));

        // Test string assignment (should not modify position)
        attribute.setExpression("result = \"fail\"");
        handler.computeAttribute(attribute, position);
        assertEquals("success", position.getString("result"));

        // Test date assignment (should not modify position)
        attribute.setExpression("fixTime = \"2017-10-18 10:00:01\"");
        handler.computeAttribute(attribute, position);
        assertEquals(date, position.getFixTime());
    }

    /**
     * Tests that computed attributes are correctly added to positions before they are
     * published to the message broker. This ensures that downstream services receive
     * positions with all computed attributes included.
     */
    @Test
    public void testComputedAttributesInPublishedPositions() {
        // Setup test data
        position.setDeviceId(1);  // Required for handler to process
        attribute.setDeviceId(1); // Match with position's device
        attribute.setExpression("speed * 2");
        attribute.setDescription("doubleSpeed");
        attribute.setId(100L);
        attribute.setAttribute("doubleSpeed");

        // Configure handler to return our test attribute
        ComputedAttributesHandler handlerSpy = Mockito.spy(handler);
        Mockito.doReturn(java.util.Collections.singletonList(attribute)).when(handlerSpy).getAttributes(position.getDeviceId());

        // Process the position
        handlerSpy.handlePosition(position);

        // Verify the position has the computed attribute added
        assertEquals(84, position.getDouble("doubleSpeed"));

        // Verify the position was published to the message broker
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher, times(1)).publishPosition(positionCaptor.capture());
        Position publishedPosition = positionCaptor.getValue();

        // Verify the published position contains the computed attribute
        assertEquals(84, publishedPosition.getDouble("doubleSpeed"));
    }

    /**
     * Tests that the handler correctly propagates OpenTelemetry trace context when processing
     * positions. This ensures that distributed tracing works across microservices.
     */
    @Test
    public void testTracingContextPropagation() {
        // Setup a mock span and trace context
        String traceId = "00000000000000000000000000000001";
        String spanId = "0000000000000002";
        SpanContext spanContext = SpanContext.create(
                traceId,
                spanId,
                TraceFlags.getSampled(),
                TraceState.getDefault());
        
        when(span.getSpanContext()).thenReturn(spanContext);
        
        // Set current span in OpenTelemetry context
        try (Scope scope = Context.current().with(Span.wrap(spanContext)).makeCurrent()) {
            // Setup test data
            position.setDeviceId(1);
            attribute.setDeviceId(1);
            attribute.setExpression("speed * 2");
            attribute.setAttribute("doubleSpeed");
            
            // Configure handler to return our test attribute
            ComputedAttributesHandler handlerSpy = Mockito.spy(handler);
            Mockito.doReturn(java.util.Collections.singletonList(attribute)).when(handlerSpy).getAttributes(position.getDeviceId());
            
            // Process the position
            handlerSpy.handlePosition(position);
            
            // Verify the position was published with trace context
            ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
            verify(messagePublisher, times(1)).publishPosition(positionCaptor.capture());
            Position publishedPosition = positionCaptor.getValue();
            
            // Verify trace context was propagated
            assertNotNull(publishedPosition.getString("otel.trace_id"));
            assertNotNull(publishedPosition.getString("otel.span_id"));
            assertEquals(traceId, publishedPosition.getString("otel.trace_id"));
        }
    }

    /**
     * Tests that the handler correctly propagates correlation IDs across services.
     * This ensures that requests can be tracked across the microservices architecture.
     */
    @Test
    public void testCorrelationIdPropagation() {
        // Setup a correlation ID
        String correlationId = UUID.randomUUID().toString();
        position.set("correlationId", correlationId);
        position.setDeviceId(1);
        
        // Process the position
        handler.handlePosition(position);
        
        // Verify the position was published with the correlation ID
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        verify(messagePublisher, times(1)).publishPosition(positionCaptor.capture());
        Position publishedPosition = positionCaptor.getValue();
        
        // Verify correlation ID was preserved
        assertEquals(correlationId, publishedPosition.getString("correlationId"));
    }

    /**
     * Tests that the handler correctly handles positions with no applicable computed attributes.
     * This ensures that positions without computed attributes are still published correctly.
     */
    @Test
    public void testPositionWithNoComputedAttributes() {
        // Setup position with a device ID that has no attributes
        position.setDeviceId(999); // No attributes configured for this device
        
        // Process the position
        handler.handlePosition(position);
        
        // Verify the position was still published to the message broker
        verify(messagePublisher, times(1)).publishPosition(any(Position.class));
    }
}