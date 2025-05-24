package org.traccar.handler.events;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.messaging.MessageConstants;
import org.traccar.messaging.PositionMessage;
import org.traccar.messaging.PositionProducer;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the integration between the position-service and the event-service for alarm events.
 * Verifies that positions with alarm attributes are correctly published to the message broker
 * for consumption by the event-service.
 */
@ExtendWith(MockitoExtension.class)
public class AlarmEventHandlerTest {

    @Mock
    private PositionProducer positionProducer;
    
    @Mock
    private Tracer tracer;
    
    @Mock
    private Span span;
    
    @Mock
    private Config config;
    
    @Mock
    private CacheManager cacheManager;
    
    @Captor
    private ArgumentCaptor<PositionMessage> positionMessageCaptor;
    
    private org.traccar.handler.EnrichedPositionProducer enrichedPositionProducer;
    
    @BeforeEach
    public void setUp() {
        // Mock the tracer to return our mock span
        when(tracer.spanBuilder(anyString())).thenReturn(mock(io.opentelemetry.api.trace.SpanBuilder.class));
        when(tracer.spanBuilder(anyString()).startSpan()).thenReturn(span);
        
        // Mock the context for distributed tracing
        Context context = mock(Context.class);
        when(context.with(any(Span.class))).thenReturn(context);
        
        // Mock the position producer to return a completed future
        when(positionProducer.publish(anyString(), any(PositionMessage.class), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        
        // Create the enriched position producer with our mocks
        enrichedPositionProducer = new org.traccar.handler.EnrichedPositionProducer(
                positionProducer, config, tracer, mock(io.opentelemetry.api.metrics.Meter.class));
    }
    
    /**
     * Tests that a position with an alarm attribute is correctly published to the message broker
     * with all necessary alarm data for consumption by the event-service.
     */
    @Test
    public void testAlarmPositionPublishing() throws Exception {
        // Create a position with an alarm attribute
        Position position = new Position();
        position.setId(1L);
        position.setDeviceId(123L);
        position.setProtocol("test");
        position.addAlarm(Position.ALARM_GENERAL);
        
        // Publish the position to the message broker
        enrichedPositionProducer.publishPositionSync(position);
        
        // Verify that the position was published to the correct topic
        verify(positionProducer).publish(
                eq(MessageConstants.TOPIC_ENRICHED_POSITIONS),
                positionMessageCaptor.capture(),
                eq(String.valueOf(position.getDeviceId())));
        
        // Get the captured position message
        PositionMessage capturedMessage = positionMessageCaptor.getValue();
        
        // Verify the position data was correctly included in the message
        assertEquals(position.getId(), capturedMessage.getPositionId());
        assertEquals(position.getDeviceId(), capturedMessage.getDeviceId());
        assertEquals(position.getProtocol(), capturedMessage.getProtocol());
        
        // Verify the alarm attribute was correctly included in the message
        assertNotNull(capturedMessage.getAttributes());
        assertTrue(capturedMessage.getAttributes().containsKey(Position.KEY_ALARM));
        assertEquals(Position.ALARM_GENERAL, capturedMessage.getAttributes().get(Position.KEY_ALARM));
        
        // Verify that a correlation ID was included for distributed tracing
        assertNotNull(capturedMessage.getCorrelationId());
    }
    
    /**
     * Tests that multiple alarm types are correctly published to the message broker.
     */
    @Test
    public void testMultipleAlarmTypes() throws Exception {
        // Test different alarm types
        String[] alarmTypes = {
                Position.ALARM_SOS,
                Position.ALARM_POWER_CUT,
                Position.ALARM_OVERSPEED,
                Position.ALARM_GEOFENCE_ENTER,
                Position.ALARM_GEOFENCE_EXIT
        };
        
        for (String alarmType : alarmTypes) {
            // Create a position with the specific alarm type
            Position position = new Position();
            position.setId(1L);
            position.setDeviceId(123L);
            position.setProtocol("test");
            position.addAlarm(alarmType);
            
            // Publish the position to the message broker
            enrichedPositionProducer.publishPositionSync(position);
            
            // Verify that the position was published with the correct alarm type
            verify(positionProducer).publish(
                    eq(MessageConstants.TOPIC_ENRICHED_POSITIONS),
                    positionMessageCaptor.capture(),
                    eq(String.valueOf(position.getDeviceId())));
            
            // Get the captured position message
            PositionMessage capturedMessage = positionMessageCaptor.getValue();
            
            // Verify the alarm attribute was correctly included in the message
            assertNotNull(capturedMessage.getAttributes());
            assertTrue(capturedMessage.getAttributes().containsKey(Position.KEY_ALARM));
            assertEquals(alarmType, capturedMessage.getAttributes().get(Position.KEY_ALARM));
        }
    }
}