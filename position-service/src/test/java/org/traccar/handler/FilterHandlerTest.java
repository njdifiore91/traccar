/*
 * Copyright 2014 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.traccar.BaseTest;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.metrics.PositionMetrics;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.storage.Storage;

import java.util.Date;
import java.util.List;
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

public class FilterHandlerTest extends BaseTest {

    @RegisterExtension
    static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

    private FilterHandler passingHandler;
    private FilterHandler filteringHandler;
    private PositionMetrics positionMetrics;
    private Storage storage;

    @BeforeEach
    public void passingHandler() {
        var config = mock(Config.class);
        when(config.getBoolean(Keys.FILTER_ENABLE)).thenReturn(true);
        var device = mock(Device.class);
        storage = mock(Storage.class);
        positionMetrics = mock(PositionMetrics.class);
        
        // Use direct mocking instead of CacheManager
        passingHandler = new FilterHandler(config, storage, null);
        passingHandler.setPositionMetrics(positionMetrics);
        passingHandler.setDeviceResolver(id -> device);
    }

    @BeforeEach
    public void filteringHandler() {
        var config = mock(Config.class);
        when(config.getBoolean(Keys.FILTER_ENABLE)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_INVALID)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_ZERO)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_DUPLICATE)).thenReturn(true);
        when(config.getLong(Keys.FILTER_FUTURE)).thenReturn(5 * 60L);
        when(config.getBoolean(Keys.FILTER_APPROXIMATE)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_STATIC)).thenReturn(true);
        when(config.getInteger(Keys.FILTER_DISTANCE)).thenReturn(10);
        when(config.getInteger(Keys.FILTER_MAX_SPEED)).thenReturn(500);
        when(config.getLong(Keys.FILTER_SKIP_LIMIT)).thenReturn(10L);
        when(config.getBoolean(Keys.FILTER_SKIP_ATTRIBUTES_ENABLE)).thenReturn(true);
        when(config.getString(Keys.FILTER_SKIP_ATTRIBUTES.getKey())).thenReturn("alarm,result");
        
        var device = mock(Device.class);
        storage = mock(Storage.class);
        positionMetrics = mock(PositionMetrics.class);
        
        // Use direct mocking instead of CacheManager
        filteringHandler = new FilterHandler(config, storage, null);
        filteringHandler.setPositionMetrics(positionMetrics);
        filteringHandler.setDeviceResolver(id -> device);
    }

    private Position createPosition(Date time, boolean valid, double speed) {
        Position position = new Position();
        position.setDeviceId(0);
        position.setTime(time);
        position.setValid(valid);
        position.setLatitude(10);
        position.setLongitude(10);
        position.setAltitude(10);
        position.setSpeed(speed);
        position.setCourse(10);
        return position;
    }

    @Test
    public void testFilter() {
        Position position = createPosition(new Date(), true, 10);

        assertFalse(filteringHandler.filter(position));
        assertFalse(passingHandler.filter(position));

        position = createPosition(new Date(Long.MAX_VALUE), true, 10);

        assertTrue(filteringHandler.filter(position));
        assertFalse(passingHandler.filter(position));

        position = createPosition(new Date(), false, 10);

        assertTrue(filteringHandler.filter(position));
        assertFalse(passingHandler.filter(position));
    }

    @Test
    public void testSkipAttributes() {
        Position position = createPosition(new Date(), true, 0);
        position.addAlarm(Position.ALARM_GENERAL);

        assertFalse(filteringHandler.filter(position));
    }
    
    @Test
    public void testOpenTelemetryInstrumentation() {
        Position position = createPosition(new Date(), true, 10);
        
        // Process position through handler
        filteringHandler.onPosition(position, processed -> {});
        
        // Verify span was created
        List<SpanData> spans = otelTesting.getSpanExporter().getFinishedSpanItems();
        assertEquals(1, spans.size());
        
        SpanData span = spans.get(0);
        assertEquals("FilterHandler.onPosition", span.getName());
        assertEquals(SpanKind.INTERNAL, span.getKind());
        
        // Verify span attributes
        assertTrue(span.getAttributes().asMap().containsKey(io.opentelemetry.api.common.AttributeKey.stringKey("deviceId")));
    }
    
    @Test
    public void testMetricsCollection() {
        Position position = createPosition(new Date(), false, 10);
        
        // Process position through handler
        filteringHandler.onPosition(position, processed -> {});
        
        // Verify metrics were recorded
        verify(positionMetrics, times(1)).recordFilteredPosition(eq("invalid"));
    }
    
    @Test
    public void testConcurrentProcessing() throws InterruptedException {
        int threadCount = 10;
        int positionsPerThread = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount * positionsPerThread);
        
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                for (int j = 0; j < positionsPerThread; j++) {
                    Position position = createPosition(new Date(), true, 10);
                    filteringHandler.onPosition(position, processed -> latch.countDown());
                }
            });
        }
        
        // Wait for all positions to be processed
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Timed out waiting for positions to be processed");
        
        // Verify metrics were recorded the correct number of times
        verify(positionMetrics, times(threadCount * positionsPerThread)).recordProcessedPosition();
        
        executor.shutdown();
    }
}