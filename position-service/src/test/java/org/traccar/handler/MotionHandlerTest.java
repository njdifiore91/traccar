package org.traccar.handler;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MotionHandlerTest {

    @Mock
    private Storage storage;
    
    @Mock
    private Config config;
    
    @Mock
    private Tracer tracer;
    
    @Mock
    private Span span;
    
    private MotionHandler motionHandler;

    @BeforeEach
    public void setup() {
        when(config.getString(Keys.EVENT_MOTION_SPEED_THRESHOLD.getKey())).thenReturn("0.01");
        when(tracer.spanBuilder(any())).thenReturn(mock(Span.Builder.class));
        when(tracer.spanBuilder(any()).startSpan()).thenReturn(span);
        
        motionHandler = new MotionHandler(storage, config, tracer);
    }

    @Test
    public void testCalculateMotion() throws StorageException {
        // Setup
        Device device = mock(Device.class);
        when(storage.getObject(eq(Device.class), any(Request.class))).thenReturn(Optional.of(device));

        Position position = new Position();
        position.setDeviceId(1L);
        
        // Execute
        motionHandler.handlePosition(position, p -> {});

        // Verify
        assertEquals(false, position.getAttributes().get(Position.KEY_MOTION));
        verify(storage).getObject(eq(Device.class), any(Request.class));
    }
    
    @Test
    public void testWithOpenTelemetryInstrumentation() throws StorageException {
        // Setup
        Device device = mock(Device.class);
        when(storage.getObject(eq(Device.class), any(Request.class))).thenReturn(Optional.of(device));
        when(span.storeInContext(any())).thenReturn(Context.current());

        Position position = new Position();
        position.setDeviceId(1L);
        
        // Execute
        motionHandler.handlePosition(position, p -> {});

        // Verify
        verify(tracer).spanBuilder(eq("MotionHandler.handlePosition"));
        verify(span).setAttribute(eq("deviceId"), eq(1L));
        verify(span).end();
    }
    
    @ParameterizedTest
    @ValueSource(doubles = {0.0, 0.005, 0.009})
    public void testBelowThresholdMotion(double speed) throws StorageException {
        // Setup
        Device device = mock(Device.class);
        when(storage.getObject(eq(Device.class), any(Request.class))).thenReturn(Optional.of(device));

        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(speed);
        
        // Execute
        motionHandler.handlePosition(position, p -> {});

        // Verify
        assertFalse((Boolean) position.getAttributes().get(Position.KEY_MOTION));
    }
    
    @ParameterizedTest
    @ValueSource(doubles = {0.01, 0.02, 0.1})
    public void testAboveThresholdMotion(double speed) throws StorageException {
        // Setup
        Device device = mock(Device.class);
        when(storage.getObject(eq(Device.class), any(Request.class))).thenReturn(Optional.of(device));

        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(speed);
        
        // Execute
        motionHandler.handlePosition(position, p -> {});

        // Verify
        assertTrue((Boolean) position.getAttributes().get(Position.KEY_MOTION));
    }
    
    @Test
    public void testThreadSafety() throws StorageException, InterruptedException {
        // Setup
        int threadCount = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        Device device = mock(Device.class);
        when(storage.getObject(eq(Device.class), any(Request.class))).thenReturn(Optional.of(device));
        
        // Execute
        for (int i = 0; i < threadCount; i++) {
            final long deviceId = i + 1;
            executorService.submit(() -> {
                try {
                    Position position = new Position();
                    position.setDeviceId(deviceId);
                    motionHandler.handlePosition(position, p -> {});
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        
        // Wait for all threads to complete
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        executorService.shutdown();
        
        // Verify
        verify(storage, times(threadCount)).getObject(eq(Device.class), any(Request.class));
    }
}