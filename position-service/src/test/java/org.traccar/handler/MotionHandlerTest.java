/*
 * Copyright 2017 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Andrey Kunitsyn (andrey@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.handler;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.extension.ExtendWith;
import org.traccar.config.Keys;
import org.traccar.messaging.PositionMessage;
import org.traccar.messaging.PositionProducer;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class MotionHandlerTest {

    @RegisterExtension
    static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();
    
    @Mock
    private CacheManager cacheManager;
    
    @Mock
    private Tracer tracer;
    
    @Mock
    private Meter meter;
    
    @Mock
    private LongCounter motionDetectionCounter;
    
    @Mock
    private Span span;
    
    @Mock
    private PositionProducer positionProducer;
    
    private MotionHandler motionHandler;

    @BeforeEach
    public void setup() {
        // Setup tracer mock
        Span.Builder spanBuilder = mock(Span.Builder.class);
        when(tracer.spanBuilder(anyString())).thenReturn(spanBuilder);
        when(spanBuilder.setParent(any(Context.class))).thenReturn(spanBuilder);
        when(spanBuilder.setAttribute(anyString(), anyString())).thenReturn(spanBuilder);
        when(spanBuilder.startSpan()).thenReturn(span);
        
        // Setup meter mock
        when(meter.counterBuilder(eq("motion.detection.count")))
                .thenReturn(mock(LongCounter.Builder.class));
        when(meter.counterBuilder(eq("motion.detection.count")).setDescription(anyString()))
                .thenReturn(mock(LongCounter.Builder.class));
        when(meter.counterBuilder(eq("motion.detection.count")).setDescription(anyString()).build())
                .thenReturn(motionDetectionCounter);
        
        // Setup default threshold value
        when(cacheManager.lookup(eq(Keys.EVENT_MOTION_SPEED_THRESHOLD), anyLong()))
                .thenReturn(0.01);
        
        // Create handler with mocked dependencies
        motionHandler = new MotionHandler(cacheManager, tracer, meter);
    }

    @Test
    public void testNoMotionDetection() {
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(0.0);
        
        // Process position
        motionHandler.onPosition(position, p -> {});

        // Verify motion attribute was set to false
        assertEquals(false, position.getAttributes().get(Position.KEY_MOTION));
        
        // Verify cache manager was used to get the threshold
        verify(cacheManager).lookup(eq(Keys.EVENT_MOTION_SPEED_THRESHOLD), eq(1L));
        
        // Verify motion detection counter was incremented
        verify(motionDetectionCounter).add(1);
    }
    
    @ParameterizedTest
    @ValueSource(doubles = {0.0, 0.005, 0.009})
    public void testBelowThresholdMotion(double speed) {
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(speed);
        
        // Process position
        motionHandler.onPosition(position, p -> {});

        // Verify motion attribute was set to false
        assertFalse((Boolean) position.getAttributes().get(Position.KEY_MOTION));
        
        // Verify span attributes were set correctly
        verify(span).setAttribute(eq("motion.detected"), eq(false));
        verify(span).setAttribute(eq("motion.speed"), eq(speed));
        verify(span).setAttribute(eq("motion.threshold"), eq(0.01));
    }
    
    @ParameterizedTest
    @ValueSource(doubles = {0.01, 0.02, 0.1})
    public void testAboveThresholdMotion(double speed) {
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(speed);
        
        // Process position
        motionHandler.onPosition(position, p -> {});

        // Verify motion attribute was set to true
        assertTrue((Boolean) position.getAttributes().get(Position.KEY_MOTION));
        
        // Verify span attributes were set correctly
        verify(span).setAttribute(eq("motion.detected"), eq(true));
        verify(span).setAttribute(eq("motion.speed"), eq(speed));
        verify(span).setAttribute(eq("motion.threshold"), eq(0.01));
    }
    
    @Test
    public void testPreDefinedMotionAttribute() {
        Position position = new Position();
        position.setDeviceId(1L);
        position.set(Position.KEY_MOTION, true);
        
        // Process position
        motionHandler.onPosition(position, p -> {});

        // Verify motion attribute remains true
        assertTrue((Boolean) position.getAttributes().get(Position.KEY_MOTION));
        
        // Verify that motion detection counter was not incremented
        verify(motionDetectionCounter, times(0)).add(1);
        
        // Verify span attribute for predefined motion was set
        verify(span).setAttribute(eq("motion.predefined"), eq(true));
    }
    
    @Test
    public void testOpenTelemetryInstrumentation() {
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(0.0);
        
        // Process position through handler
        motionHandler.onPosition(position, p -> {});
        
        // Verify span was created with correct attributes
        verify(tracer).spanBuilder(eq("motion.detection"));
        verify(span).setAttribute(eq("deviceId"), eq("1"));
        verify(span).setStatus(eq(StatusCode.OK));
        verify(span).end();
        
        // Verify metrics were recorded
        verify(motionDetectionCounter).add(1);
    }
    
    @Test
    public void testCorrelationIdPropagation() {
        // Create a position with correlation ID in attributes
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(0.0);
        position.set("correlationId", "test-correlation-id");
        
        // Setup context propagation
        Context parentContext = mock(Context.class);
        when(Context.current()).thenReturn(parentContext);
        
        // Process position
        motionHandler.onPosition(position, p -> {});
        
        // Verify span was created with parent context
        verify(tracer.spanBuilder(anyString())).setParent(eq(parentContext));
    }
    
    @Test
    public void testExceptionHandling() {
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(0.0);
        
        // Setup exception in cache manager
        RuntimeException testException = new RuntimeException("Test exception");
        when(cacheManager.lookup(eq(Keys.EVENT_MOTION_SPEED_THRESHOLD), anyLong()))
                .thenThrow(testException);
        
        // Process position and capture callback
        ArgumentCaptor<BasePositionHandler.Callback> callbackCaptor = 
                ArgumentCaptor.forClass(BasePositionHandler.Callback.class);
        
        // Execute with callback capture
        motionHandler.onPosition(position, callbackCaptor.capture());
        
        // Verify exception was recorded in span
        verify(span).recordException(eq(testException));
        verify(span).setStatus(eq(StatusCode.ERROR), anyString());
    }
    
    @Test
    public void testThreadSafety() throws InterruptedException {
        int threadCount = 10;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        // Process positions from multiple threads
        for (int i = 0; i < threadCount; i++) {
            final long deviceId = i + 1;
            executorService.submit(() -> {
                try {
                    Position position = new Position();
                    position.setDeviceId(deviceId);
                    position.setSpeed(0.0);
                    
                    // Setup specific threshold for this device
                    when(cacheManager.lookup(eq(Keys.EVENT_MOTION_SPEED_THRESHOLD), eq(deviceId)))
                            .thenReturn(0.01);
                    
                    motionHandler.onPosition(position, p -> {});
                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }
        
        // Wait for all threads to complete
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        executorService.shutdown();
        
        // Verify cache manager was called for each device
        verify(cacheManager, times(threadCount)).lookup(eq(Keys.EVENT_MOTION_SPEED_THRESHOLD), anyLong());
        
        // Verify motion detection counter was incremented for each position
        verify(motionDetectionCounter, times(threadCount)).add(1);
    }
    
    @Test
    public void testMessageBrokerIntegration() {
        // Create a position with motion
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(0.02); // Above threshold
        
        // Setup position producer mock
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        
        // Process position with message broker integration
        motionHandler.handlePosition(position, positionProducer::publishPosition);
        
        // Verify motion attribute was set to true
        assertTrue((Boolean) position.getAttributes().get(Position.KEY_MOTION));
        
        // Verify position was published to the message broker
        verify(positionProducer).publishPosition(positionCaptor.capture());
        
        // Verify the published position has the motion attribute
        Position publishedPosition = positionCaptor.getValue();
        assertTrue((Boolean) publishedPosition.getAttributes().get(Position.KEY_MOTION));
    }
    
    @Test
    public void testMessageBrokerWithCorrelationId() {
        // Create a position with correlation ID
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(0.0);
        String correlationId = "test-correlation-id-" + System.currentTimeMillis();
        position.set("correlationId", correlationId);
        
        // Setup position producer mock
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        ArgumentCaptor<Map<String, String>> headersCaptor = ArgumentCaptor.forClass(Map.class);
        
        // Process position with message broker integration
        motionHandler.handlePosition(position, p -> {
            positionProducer.publishPosition(p, headersCaptor.getValue());
        });
        
        // Verify position was published to the message broker
        verify(positionProducer).publishPosition(positionCaptor.capture(), headersCaptor.capture());
        
        // Verify the correlation ID was propagated in the message headers
        Map<String, String> headers = headersCaptor.getValue();
        assertEquals(correlationId, headers.get("correlationId"));
    }
    
    @Test
    public void testMessageBrokerErrorHandling() {
        // Create a position
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(0.0);
        
        // Setup position producer mock to throw exception
        RuntimeException brokerException = new RuntimeException("Message broker connection error");
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        
        // Process position with message broker integration that fails
        motionHandler.handlePosition(position, p -> {
            positionProducer.publishPosition(p);
            throw brokerException;
        });
        
        // Verify the exception was recorded in the span
        verify(span).recordException(any(RuntimeException.class));
        verify(span).setStatus(eq(StatusCode.ERROR), anyString());
    }
    
    @Test
    public void testDistributedTracingWithMessageBroker() {
        // Create a position
        Position position = new Position();
        position.setDeviceId(1L);
        position.setSpeed(0.0);
        
        // Setup OpenTelemetry context
        Context parentContext = mock(Context.class);
        when(Context.current()).thenReturn(parentContext);
        
        // Setup position producer mock
        ArgumentCaptor<Position> positionCaptor = ArgumentCaptor.forClass(Position.class);
        ArgumentCaptor<Map<String, String>> headersCaptor = ArgumentCaptor.forClass(Map.class);
        
        // Process position with message broker integration
        motionHandler.handlePosition(position, p -> {
            positionProducer.publishPosition(p, headersCaptor.getValue());
        });
        
        // Verify position was published to the message broker
        verify(positionProducer).publishPosition(positionCaptor.capture(), headersCaptor.capture());
        
        // Verify the trace context was propagated in the message headers
        Map<String, String> headers = headersCaptor.getValue();
        assertTrue(headers.containsKey("traceparent"));
    }
}