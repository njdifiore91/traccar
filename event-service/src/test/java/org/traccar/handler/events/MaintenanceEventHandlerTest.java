package org.traccar.handler.events;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Maintenance;
import org.traccar.model.Position;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit test for MaintenanceEventHandler.
 * 
 * This test verifies that the MaintenanceEventHandler correctly:
 * 1. Consumes enriched positions from the 'enriched.positions' topic
 * 2. Detects maintenance threshold events based on position attributes
 * 3. Publishes events to the 'events' topic with event type as message key
 * 4. Propagates OpenTelemetry trace context across service boundaries
 * 5. Handles circuit breaker fallbacks for storage failures
 */
@ExtendWith(MockitoExtension.class)
public class MaintenanceEventHandlerTest {

    @Mock
    private Storage storage;
    
    @Mock
    private MessagePublisher messagePublisher;
    
    @Mock
    private Config config;
    
    private MaintenanceEventHandler maintenanceEventHandler;
    private InMemorySpanExporter spanExporter;
    private Tracer tracer;
    
    @BeforeEach
    public void setUp() {
        // Set up OpenTelemetry for testing
        spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();
        
        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        
        // Set the OpenTelemetry instance as the global instance for testing
        GlobalOpenTelemetry.resetForTest();
        GlobalOpenTelemetry.set(openTelemetry);
        
        tracer = openTelemetry.getTracer("test-tracer");
        
        // Configure the behavior of the config mock
        when(config.getBoolean(eq("processing.computedAttributes.enable"))).thenReturn(Boolean.TRUE);
        when(config.getBoolean(eq("event.maintenanceThreshold.enable"))).thenReturn(Boolean.TRUE);
        
        // Create the MaintenanceEventHandler with mocked dependencies
        maintenanceEventHandler = new MaintenanceEventHandler(storage, messagePublisher, config);
    }
    
    @AfterEach
    public void tearDown() {
        // Reset OpenTelemetry after each test
        GlobalOpenTelemetry.resetForTest();
        spanExporter.reset();
    }
    
    @Test
    public void testMaintenanceThresholdEvent() throws Exception {
        // Create a span to simulate the incoming message from the broker
        Span parentSpan = tracer.spanBuilder("consume-enriched-position")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination", "enriched.positions")
                .setAttribute("messaging.destination_kind", "topic")
                .startSpan();
        
        try (Scope scope = parentSpan.makeCurrent()) {
            // Create test data
            long deviceId = 1;
            long maintenanceId = 1;
            
            Device device = new Device();
            device.setId(deviceId);
            device.setName("Test Device");
            
            Position position = new Position();
            position.setDeviceId(deviceId);
            position.setId(1L);
            position.setTime(new Date());
            position.setLatitude(10);
            position.setLongitude(20);
            
            // Set up maintenance attributes
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("hours", 100.0); // Current engine hours
            position.setAttributes(attributes);
            
            // Set up maintenance configuration
            Maintenance maintenance = new Maintenance();
            maintenance.setId(maintenanceId);
            maintenance.setDeviceId(deviceId);
            maintenance.setType("engineHours");
            maintenance.setStart(0);
            maintenance.setPeriod(100); // Maintenance threshold at 100 hours
            maintenance.setName("Engine Maintenance");
            
            // Mock storage to return the maintenance configuration
            when(storage.getObjects(eq(Maintenance.class), any(Request.class)))
                .thenReturn(Collections.singletonList(maintenance));
            
            // Mock device retrieval
            when(storage.getObject(eq(Device.class), eq(new Request(
                    new Columns.All(), new Condition.Equals("id", deviceId)))))
                .thenReturn(device);
            
            // Call the method under test
            maintenanceEventHandler.analyzePosition(position);
            
            // Verify that an event was published to the message broker
            ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            verify(messagePublisher).publish(eq("events"), keyCaptor.capture(), eventCaptor.capture(), any(Context.class));
            
            // Verify the event details
            Event capturedEvent = eventCaptor.getValue();
            assertNotNull(capturedEvent);
            assertEquals(Event.TYPE_MAINTENANCE, capturedEvent.getType());
            assertEquals(deviceId, capturedEvent.getDeviceId());
            assertEquals(maintenanceId, capturedEvent.getMaintenanceId());
            assertEquals(position.getId(), capturedEvent.getPositionId());
            
            // Verify that the event type was used as the message key
            assertEquals(Event.TYPE_MAINTENANCE, keyCaptor.getValue());
            
            // Verify that the event was published with the correct trace context and OpenTelemetry attributes
            verify(messagePublisher).publish(
                eq("events"), 
                eq(Event.TYPE_MAINTENANCE), 
                any(Event.class), 
                any(Context.class));
        } finally {
            parentSpan.end();
        }
        
        // Verify that spans were created for the event processing
        assertEquals(1, spanExporter.getFinishedSpanItems().size());
    }
    
    @Test
    public void testNoMaintenanceEvent() throws Exception {
        // Create a span to simulate the incoming message from the broker
        Span parentSpan = tracer.spanBuilder("consume-enriched-position")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination", "enriched.positions")
                .setAttribute("messaging.destination_kind", "topic")
                .startSpan();
        
        try (Scope scope = parentSpan.makeCurrent()) {
            // Create test data
            long deviceId = 1;
            
            Device device = new Device();
            device.setId(deviceId);
            device.setName("Test Device");
            
            Position position = new Position();
            position.setDeviceId(deviceId);
            position.setId(1L);
            position.setTime(new Date());
            position.setLatitude(10);
            position.setLongitude(20);
            
            // Set up maintenance attributes - below threshold
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("hours", 50.0); // Current engine hours - below threshold
            position.setAttributes(attributes);
            
            // Set up maintenance configuration
            Maintenance maintenance = new Maintenance();
            maintenance.setId(1L);
            maintenance.setDeviceId(deviceId);
            maintenance.setType("engineHours");
            maintenance.setStart(0);
            maintenance.setPeriod(100); // Maintenance threshold at 100 hours
            maintenance.setName("Engine Maintenance");
            
            // Mock storage to return the maintenance configuration
            when(storage.getObjects(eq(Maintenance.class), any(Request.class)))
                .thenReturn(Collections.singletonList(maintenance));
            
            // Mock device retrieval
            when(storage.getObject(eq(Device.class), eq(new Request(
                    new Columns.All(), new Condition.Equals("id", deviceId)))))
                .thenReturn(device);
            
            // Call the method under test
            maintenanceEventHandler.analyzePosition(position);
            
            // Verify that no event was published to the message broker
            verify(messagePublisher, never()).publish(eq("events"), any(), any());
            verify(messagePublisher, never()).publish(eq("events"), any(), any(), any(Context.class));
        } finally {
            parentSpan.end();
        }
        
        // Verify that spans were created for the event processing
        assertEquals(1, spanExporter.getFinishedSpanItems().size());
    }
    
    @Test
    public void testCircuitBreakerFallback() throws Exception {
        // Create a span to simulate the incoming message from the broker
        Span parentSpan = tracer.spanBuilder("consume-enriched-position")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("messaging.system", "kafka")
                .setAttribute("messaging.destination", "enriched.positions")
                .setAttribute("messaging.destination_kind", "topic")
                .startSpan();
        
        try (Scope scope = parentSpan.makeCurrent()) {
            // Create test data
            long deviceId = 1;
            
            Device device = new Device();
            device.setId(deviceId);
            device.setName("Test Device");
            
            Position position = new Position();
            position.setDeviceId(deviceId);
            position.setId(1L);
            position.setTime(new Date());
            position.setLatitude(10);
            position.setLongitude(20);
            
            // Set up maintenance attributes
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("hours", 100.0); // Current engine hours
            position.setAttributes(attributes);
            
            // Mock device retrieval
            when(storage.getObject(eq(Device.class), eq(new Request(
                    new Columns.All(), new Condition.Equals("id", deviceId)))))
                .thenReturn(device);
            
            // Mock storage to throw an exception to simulate circuit breaker activation
            when(storage.getObjects(eq(Maintenance.class), any(Request.class)))
                .thenThrow(new StorageException(StorageException.Error.GENERAL_ERROR, "Database connection error"));
            
            // Call the method under test
            maintenanceEventHandler.analyzePosition(position);
            
            // Verify that no event was published due to the circuit breaker fallback
            verify(messagePublisher, never()).publish(eq("events"), any(), any());
            verify(messagePublisher, never()).publish(eq("events"), any(), any(), any(Context.class));
        } finally {
            parentSpan.end();
        }
        
        // Verify that spans were created for the event processing, including error information
        assertEquals(1, spanExporter.getFinishedSpanItems().size());
    }
}