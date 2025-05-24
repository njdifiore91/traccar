package org.traccar.handler;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class DistanceHandlerTest {

    @Mock
    private Config config;

    @Mock
    private CacheManager cacheManager;

    @Mock
    private Tracer tracer;

    @Mock
    private SpanBuilder spanBuilder;

    @Mock
    private Span span;

    @Mock
    private Scope scope;

    private DistanceHandler distanceHandler;

    @BeforeEach
    public void setUp() {
        // Configure mocks
        when(config.getBoolean(any())).thenReturn(false);
        when(config.getInteger(any())).thenReturn(0);
        
        // Mock OpenTelemetry behavior
        when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);
        when(spanBuilder.startSpan()).thenReturn(span);
        when(span.makeCurrent()).thenReturn(scope);
        
        // Create handler with mocked dependencies
        distanceHandler = new DistanceHandler(config, cacheManager);
        
        // Use reflection to inject mocked tracer
        try {
            java.lang.reflect.Field tracerField = DistanceHandler.class.getDeclaredField("tracer");
            tracerField.setAccessible(true);
            tracerField.set(distanceHandler, tracer);
        } catch (Exception e) {
            throw new RuntimeException("Failed to inject mocked tracer", e);
        }
    }

    @Test
    public void testCalculateDistance() {
        // Create a position
        Position position = new Position();
        
        // Process the position
        distanceHandler.handlePosition(position, p -> {});

        // Verify initial values
        assertEquals(0.0, position.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(0.0, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));

        // Set distance and process again
        position.set(Position.KEY_DISTANCE, 100);
        distanceHandler.handlePosition(position, p -> {});

        // Verify updated values
        assertEquals(100.0, position.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(100.0, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
    }

    @Test
    public void testOpenTelemetryInstrumentation() {
        // Create a position
        Position position = new Position();
        position.setDeviceId(123L);
        
        // Process the position
        distanceHandler.handlePosition(position, p -> {});
        
        // Verify that OpenTelemetry span was created
        verify(tracer).spanBuilder("calculateDistance");
        verify(spanBuilder).startSpan();
        verify(span).setAttribute("deviceId", 123L);
        verify(span).setAttribute(eq("positionId"), any());
        verify(span).addEvent("no previous position found");
        verify(span).end();
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        // Number of concurrent threads
        int threadCount = 10;
        
        // Create a shared position
        Position position = new Position();
        position.setDeviceId(123L);
        position.set(Position.KEY_DISTANCE, 10.0);
        
        // Create a latch to synchronize thread execution
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadCount);
        
        // Create thread pool
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        
        // Create a reference to store any exceptions
        AtomicReference<Throwable> exception = new AtomicReference<>();
        
        // Mock previous position
        Position previousPosition = new Position();
        previousPosition.set(Position.KEY_TOTAL_DISTANCE, 0.0);
        when(cacheManager.getPosition(position.getDeviceId())).thenReturn(previousPosition);
        
        // Submit tasks to thread pool
        for (int i = 0; i < threadCount; i++) {
            executorService.submit(() -> {
                try {
                    // Wait for all threads to be ready
                    startLatch.await();
                    
                    // Process the position
                    distanceHandler.handlePosition(position, p -> {});
                } catch (Throwable t) {
                    exception.set(t);
                } finally {
                    finishLatch.countDown();
                }
            });
        }
        
        // Start all threads simultaneously
        startLatch.countDown();
        
        // Wait for all threads to finish
        finishLatch.await(5, TimeUnit.SECONDS);
        
        // Shutdown executor
        executorService.shutdown();
        
        // Check if any exceptions occurred
        if (exception.get() != null) {
            throw new AssertionError("Exception in thread: " + exception.get().getMessage(), exception.get());
        }
        
        // Verify that the total distance is correctly calculated
        // Each thread adds 10.0 to the total distance, so the final value should be 100.0
        assertEquals(100.0, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
    }

    @Test
    public void testMicroservicesCompatibility() {
        // Create a position with attributes that would be set in a microservices environment
        Position position = new Position();
        position.setDeviceId(123L);
        position.set("serviceId", "position-service");
        position.set("messageId", "msg-123456");
        
        // Mock previous position from cache
        Position previousPosition = new Position();
        previousPosition.set(Position.KEY_TOTAL_DISTANCE, 50.0);
        when(cacheManager.getPosition(position.getDeviceId())).thenReturn(previousPosition);
        
        // Process the position
        distanceHandler.handlePosition(position, p -> {});
        
        // Verify that the handler correctly processes the position in a microservices context
        assertNotNull(position.getAttributes().get(Position.KEY_DISTANCE));
        assertEquals(50.0, position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));
        
        // Verify that the OpenTelemetry span was created with appropriate attributes
        verify(span).setAttribute("deviceId", 123L);
        verify(span).setAttribute(eq("positionId"), any());
    }
}