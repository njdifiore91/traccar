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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Attribute;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.position.cache.CacheManager;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ComputedAttributesTest {

    @RegisterExtension
    static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

    @Mock
    private Config config;

    @Mock
    private CacheManager cacheManager;

    private ComputedAttributesHandler handler;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // Configure default behavior for config
        when(config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_LOCAL_VARIABLES)).thenReturn(true);
        when(config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_LOOPS)).thenReturn(true);
        when(config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_NEW_INSTANCE_CREATION)).thenReturn(false);
        when(config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_DEVICE_ATTRIBUTES)).thenReturn(true);
        when(config.getBoolean(Keys.PROCESSING_COMPUTED_ATTRIBUTES_LAST_ATTRIBUTES)).thenReturn(true);
        
        handler = new ComputedAttributesHandler(config, cacheManager, false);
    }

    @Test
    public void testBasicExpressionEvaluation() {
        Date date = new Date();
        Position position = new Position();
        position.setTime(date);
        position.setSpeed(42);
        position.setValid(false);
        position.set("adc1", 128);
        position.set("booleanFlag", true);
        position.set("adc2", 100);
        position.set("bitFlag", 7);
        position.set("event", 42);
        position.set("result", "success");
        Attribute attribute = new Attribute();

        // Mock empty device attributes
        Device device = new Device();
        device.setId(1);
        when(cacheManager.getObject(eq(Device.class), anyLong())).thenReturn(device);
        when(cacheManager.getPosition(anyLong())).thenReturn(null);

        attribute.setExpression("adc1");
        assertEquals(128, handler.computeAttribute(attribute, position));

        attribute.setExpression("!booleanFlag");
        assertEquals(false, handler.computeAttribute(attribute, position));

        attribute.setExpression("adc2 * 2 + 50");
        assertEquals(250, handler.computeAttribute(attribute, position));

        attribute.setExpression("(bitFlag & 4) != 0");
        assertEquals(true, handler.computeAttribute(attribute, position));

        attribute.setExpression("event == 42 ? \"lowBattery\" : null");
        assertEquals("lowBattery", handler.computeAttribute(attribute, position));

        attribute.setExpression("speed > 5 && valid");
        assertEquals(false, handler.computeAttribute(attribute, position));

        attribute.setExpression("fixTime");
        assertEquals(date, handler.computeAttribute(attribute, position));

        attribute.setExpression("math:pow(adc1, 2)");
        assertEquals(16384.0, handler.computeAttribute(attribute, position));
    }

    @Test
    public void testModificationAttempts() {
        Date date = new Date();
        Position position = new Position();
        position.setTime(date);
        position.set("adc1", 128);
        position.set("result", "success");
        Attribute attribute = new Attribute();

        // Mock empty device attributes
        Device device = new Device();
        device.setId(1);
        when(cacheManager.getObject(eq(Device.class), anyLong())).thenReturn(device);
        when(cacheManager.getPosition(anyLong())).thenReturn(null);

        // Modification attempts should not affect the position
        attribute.setExpression("adc1 = 256");
        handler.computeAttribute(attribute, position);
        assertEquals(128, position.getInteger("adc1"));

        attribute.setExpression("result = \"fail\"");
        handler.computeAttribute(attribute, position);
        assertEquals("success", position.getString("result"));

        attribute.setExpression("fixTime = \"2017-10-18 10:00:01\"");
        handler.computeAttribute(attribute, position);
        assertEquals(date, position.getFixTime());
    }

    @Test
    public void testDeviceAttributesAccess() {
        Position position = new Position();
        position.setDeviceId(1);
        Attribute attribute = new Attribute();

        // Mock device with attributes
        Device device = new Device();
        device.setId(1);
        Map<String, Object> deviceAttributes = new HashMap<>();
        deviceAttributes.put("maxSpeed", 100);
        deviceAttributes.put("maintenance", true);
        device.setAttributes(deviceAttributes);
        
        when(cacheManager.getObject(eq(Device.class), eq(1L))).thenReturn(device);
        when(cacheManager.getPosition(eq(1L))).thenReturn(null);

        // Test access to device attributes
        attribute.setExpression("maxSpeed");
        assertEquals(100, handler.computeAttribute(attribute, position));

        attribute.setExpression("maintenance");
        assertEquals(true, handler.computeAttribute(attribute, position));

        attribute.setExpression("speed > maxSpeed");
        position.setSpeed(120);
        assertEquals(true, handler.computeAttribute(attribute, position));

        position.setSpeed(80);
        assertEquals(false, handler.computeAttribute(attribute, position));
    }

    @Test
    public void testLastPositionAttributesAccess() {
        Position position = new Position();
        position.setDeviceId(1);
        position.set("temp", 25);
        position.setSpeed(50);
        
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.set("temp", 20);
        lastPosition.setSpeed(30);
        
        Attribute attribute = new Attribute();

        // Mock device and last position
        Device device = new Device();
        device.setId(1);
        
        when(cacheManager.getObject(eq(Device.class), eq(1L))).thenReturn(device);
        when(cacheManager.getPosition(eq(1L))).thenReturn(lastPosition);

        // Test access to last position attributes
        attribute.setExpression("temp - lastTemp");
        assertEquals(5, handler.computeAttribute(attribute, position));

        attribute.setExpression("speed - lastSpeed");
        assertEquals(20.0, handler.computeAttribute(attribute, position));

        attribute.setExpression("temp > lastTemp");
        assertEquals(true, handler.computeAttribute(attribute, position));
    }

    @Test
    public void testOnPositionWithAttributes() {
        Position position = new Position();
        position.setDeviceId(1);
        
        // Create attributes with different priorities
        Attribute attribute1 = new Attribute();
        attribute1.setId(1);
        attribute1.setAttribute("engineTemp");
        attribute1.setExpression("temp * 2");
        attribute1.setPriority(1); // Late processing
        
        Attribute attribute2 = new Attribute();
        attribute2.setId(2);
        attribute2.setAttribute("fuelLevel");
        attribute2.setExpression("adc1 / 4");
        attribute2.setPriority(2); // Late processing, higher priority
        
        // Mock device and attributes
        Device device = new Device();
        device.setId(1);
        
        when(cacheManager.getObject(eq(Device.class), eq(1L))).thenReturn(device);
        when(cacheManager.getDeviceObjects(eq(1L), eq(Attribute.class)))
                .thenReturn(List.of(attribute1, attribute2));
        
        // Set position attributes for computation
        position.set("temp", 40);
        position.set("adc1", 100);
        
        // Test onPosition method
        AtomicBoolean processed = new AtomicBoolean(false);
        handler.onPosition(position, p -> processed.set(p));
        
        // Verify attributes were computed and added to position
        assertEquals(80, position.getInteger("engineTemp"));
        assertEquals(25, position.getInteger("fuelLevel"));
        assertFalse(processed); // processed flag should be false
    }

    @Test
    public void testOpenTelemetryInstrumentation() {
        Position position = new Position();
        position.setDeviceId(1);
        position.set("temp", 30);
        
        Attribute attribute = new Attribute();
        attribute.setId(1);
        attribute.setAttribute("test");
        attribute.setExpression("temp + 10");
        
        // Mock device
        Device device = new Device();
        device.setId(1);
        when(cacheManager.getObject(eq(Device.class), eq(1L))).thenReturn(device);
        
        // Execute with OpenTelemetry testing
        handler.computeAttribute(attribute, position);
        
        // Verify span was created with correct attributes
        otelTesting.assertTraces().hasTracesSatisfyingExactly(trace -> 
            trace.hasSpansSatisfyingExactly(span -> 
                span.hasName("computeAttribute")
                    .hasAttribute("attribute.id", 1L)
                    .hasAttribute("attribute.name", "test")
                    .hasAttribute("device.id", 1L)
                    .hasStatus(StatusCode.OK)
            )
        );
        
        // Test onPosition method with OpenTelemetry
        when(cacheManager.getDeviceObjects(eq(1L), eq(Attribute.class)))
                .thenReturn(Collections.singletonList(attribute));
        
        otelTesting.clearSpans();
        handler.onPosition(position, p -> {});
        
        // Verify onPosition span was created
        otelTesting.assertTraces().hasTracesSatisfyingExactly(trace -> 
            trace.hasSpansSatisfyingExactly(span -> 
                span.hasName("onPosition")
                    .hasAttribute("handler.type", "late")
                    .hasAttribute("device.id", 1L)
                    .hasAttribute("attributes.count", 1)
                    .hasStatus(StatusCode.OK)
            )
        );
    }

    @Test
    public void testThreadSafety() throws InterruptedException {
        // Create a position and attribute for testing
        Position position = new Position();
        position.setDeviceId(1);
        position.set("value", 10);
        
        Attribute attribute = new Attribute();
        attribute.setId(1);
        attribute.setAttribute("result");
        attribute.setExpression("value * 2");
        
        // Mock device
        Device device = new Device();
        device.setId(1);
        when(cacheManager.getObject(eq(Device.class), eq(1L))).thenReturn(device);
        
        // Create multiple threads to compute attributes concurrently
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    // Each thread computes the attribute
                    Object result = handler.computeAttribute(attribute, position);
                    assertEquals(20, result);
                } finally {
                    latch.countDown();
                }
            });
        }
        
        // Wait for all threads to complete
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Concurrent execution timed out");
        executor.shutdown();
        
        // Verify the position wasn't modified by concurrent access
        assertEquals(10, position.getInteger("value"));
    }

    @Test
    public void testConcurrentPositionProcessing() throws InterruptedException {
        // Create multiple positions for concurrent processing
        int positionCount = 5;
        Position[] positions = new Position[positionCount];
        
        for (int i = 0; i < positionCount; i++) {
            positions[i] = new Position();
            positions[i].setDeviceId(1);
            positions[i].set("index", i);
            positions[i].set("value", 10 + i);
        }
        
        // Create attribute for testing
        Attribute attribute = new Attribute();
        attribute.setId(1);
        attribute.setAttribute("computed");
        attribute.setExpression("value * 2");
        attribute.setPriority(1); // Late processing
        
        // Mock device and attributes
        Device device = new Device();
        device.setId(1);
        when(cacheManager.getObject(eq(Device.class), eq(1L))).thenReturn(device);
        when(cacheManager.getDeviceObjects(eq(1L), eq(Attribute.class)))
                .thenReturn(Collections.singletonList(attribute));
        
        // Process positions concurrently
        ExecutorService executor = Executors.newFixedThreadPool(positionCount);
        CountDownLatch latch = new CountDownLatch(positionCount);
        ArgumentCaptor<Boolean> processedCaptor = ArgumentCaptor.forClass(Boolean.class);
        
        for (int i = 0; i < positionCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    handler.onPosition(positions[index], p -> latch.countDown());
                } catch (Exception e) {
                    e.printStackTrace();
                    latch.countDown();
                }
            });
        }
        
        // Wait for all positions to be processed
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Concurrent processing timed out");
        executor.shutdown();
        
        // Verify all positions were processed correctly
        for (int i = 0; i < positionCount; i++) {
            assertEquals((10 + i) * 2, positions[i].getInteger("computed"), 
                    "Position " + i + " was not processed correctly");
        }
    }
}