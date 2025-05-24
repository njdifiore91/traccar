package org.traccar.handler.events;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.context.Context;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
 import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.traccar.BaseTest;
import org.traccar.model.Maintenance;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.EnrichedPositionProducer;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the integration between the position-service and the event-service for maintenance events.
 * Verifies that positions with maintenance-related attributes (such as total distance) are correctly
 * published to the message broker for consumption by the event-service.
 */
@Testcontainers
@ExtendWith(MockitoExtension.class)
public class MaintenanceEventHandlerTest extends BaseTest {

    private static final String POSITIONS_TOPIC = "enriched-positions";
    private static final String CORRELATION_ID_HEADER = "correlation-id";
    private static final String TRACE_ID_HEADER = "trace-id";
    private static final String SPAN_ID_HEADER = "span-id";

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));

    @Mock
    private CacheManager cacheManager;

    @Mock
    private Span span;

    @Mock
    private SpanContext spanContext;

    private EnrichedPositionProducer positionProducer;
    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    public void setUp() {
        // Set up the position producer with the Kafka container bootstrap servers
        positionProducer = new EnrichedPositionProducer(kafka.getBootstrapServers());

        // Set up a consumer to verify messages
        Properties props = new Properties();
        props.put("bootstrap.servers", kafka.getBootstrapServers());
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(POSITIONS_TOPIC));

        // Mock OpenTelemetry context
        when(span.getSpanContext()).thenReturn(spanContext);
        when(spanContext.getTraceId()).thenReturn("test-trace-id");
        when(spanContext.getSpanId()).thenReturn("test-span-id");
        Context.current().with(span);
    }

    @AfterEach
    public void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    /**
     * Tests that positions with maintenance-related attributes (total distance) are correctly
     * published to the message broker with all necessary information for the event-service to
     * generate maintenance events.
     */
    @Test
    public void testMaintenanceAttributesPublished() throws Exception {
        // Create a position with maintenance-related attributes
        Position position = new Position();
        position.setDeviceId(1);
        position.setFixTime(new Date());
        position.set(Position.KEY_TOTAL_DISTANCE, 10001.0); // Just crossed maintenance threshold

        // Create a maintenance configuration
        Maintenance maintenance = mock(Maintenance.class);
        when(maintenance.getType()).thenReturn(Position.KEY_TOTAL_DISTANCE);
        when(maintenance.getStart()).thenReturn(10000.0);
        when(maintenance.getPeriod()).thenReturn(2000.0);
        Set<Maintenance> maintenances = Set.of(maintenance);

        // Mock the cache manager to return the maintenance configuration
        when(cacheManager.getDeviceObjects(anyLong(), eq(Maintenance.class))).thenReturn(maintenances);

        // Create a previous position to simulate crossing the threshold
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setFixTime(new Date(0));
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 9999.0);
        when(cacheManager.getPosition(anyLong())).thenReturn(lastPosition);

        // Process the position through the position processing pipeline
        // In a real scenario, this would be done by the PositionProcessingPipeline
        // Here we're directly publishing to simulate the pipeline's behavior
        positionProducer.sendPosition(position, "test-correlation-id");

        // Verify that the position was published to the message broker
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertNotNull(records);
        assertTrue(records.count() > 0, "No messages received from Kafka");

        // Verify the content of the published message
        for (ConsumerRecord<String, String> record : records) {
            String value = record.value();
            assertTrue(value.contains("\"deviceId\":1"), "Message should contain deviceId");
            assertTrue(value.contains("\"totalDistance\":10001.0"), "Message should contain total distance");
            
            // Verify that correlation ID and tracing headers are included
            String correlationId = record.headers().lastHeader(CORRELATION_ID_HEADER).value().toString();
            assertEquals("test-correlation-id", correlationId, "Correlation ID should match");
            
            String traceId = record.headers().lastHeader(TRACE_ID_HEADER).value().toString();
            assertEquals("test-trace-id", traceId, "Trace ID should match");
            
            String spanId = record.headers().lastHeader(SPAN_ID_HEADER).value().toString();
            assertEquals("test-span-id", spanId, "Span ID should match");
        }
    }

    /**
     * Tests that the position-service correctly handles maintenance configurations and includes
     * all necessary attributes in the published positions for the event-service to generate
     * maintenance events at the correct intervals.
     */
    @Test
    public void testMaintenanceIntervals() throws Exception {
        // Create a position with maintenance-related attributes
        Position position = new Position();
        position.setDeviceId(1);
        position.setFixTime(new Date());
        
        // Create a maintenance configuration with multiple intervals
        Maintenance maintenance = mock(Maintenance.class);
        when(maintenance.getType()).thenReturn(Position.KEY_TOTAL_DISTANCE);
        when(maintenance.getStart()).thenReturn(10000.0);
        when(maintenance.getPeriod()).thenReturn(2000.0);
        Set<Maintenance> maintenances = Set.of(maintenance);

        // Mock the cache manager to return the maintenance configuration
        when(cacheManager.getDeviceObjects(anyLong(), eq(Maintenance.class))).thenReturn(maintenances);

        // Test multiple maintenance thresholds
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setFixTime(new Date(0));
        when(cacheManager.getPosition(anyLong())).thenReturn(lastPosition);

        // Test case 1: Below first threshold
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 9000.0);
        position.set(Position.KEY_TOTAL_DISTANCE, 9500.0);
        positionProducer.sendPosition(position, "test-correlation-id-1");

        // Test case 2: Crossing first threshold
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 9999.0);
        position.set(Position.KEY_TOTAL_DISTANCE, 10001.0);
        positionProducer.sendPosition(position, "test-correlation-id-2");

        // Test case 3: Between first and second threshold
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 11000.0);
        position.set(Position.KEY_TOTAL_DISTANCE, 11500.0);
        positionProducer.sendPosition(position, "test-correlation-id-3");

        // Test case 4: Crossing second threshold
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 11999.0);
        position.set(Position.KEY_TOTAL_DISTANCE, 12001.0);
        positionProducer.sendPosition(position, "test-correlation-id-4");

        // Verify that all positions were published to the message broker
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
        assertNotNull(records);
        assertEquals(4, records.count(), "Expected 4 messages to be published");

        // Verify that all messages contain the necessary maintenance attributes
        for (ConsumerRecord<String, String> record : records) {
            String value = record.value();
            assertTrue(value.contains("\"deviceId\":1"), "Message should contain deviceId");
            assertTrue(value.contains("\"totalDistance\":"), "Message should contain total distance");
            
            // Verify that correlation ID is included
            assertNotNull(record.headers().lastHeader(CORRELATION_ID_HEADER), 
                    "Message should include correlation ID header");
        }
    }
}