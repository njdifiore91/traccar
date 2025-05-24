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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
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
import org.traccar.config.Keys;
import org.traccar.metrics.MetricsService;
import org.traccar.model.Calendar;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.position.PositionCacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the FilterHandler in the position-service which filters out invalid or duplicate positions.
 * This test ensures that the handler correctly applies filtering rules and only valid positions are
 * published to the message broker for consumption by other services.
 */
@ExtendWith(MockitoExtension.class)
public class FilterHandlerTest {

    @Mock
    private Config config;
    
    @Mock
    private PositionCacheManager positionCacheManager;
    
    @Mock
    private Storage storage;
    
    @Mock
    private MetricsService metricsService;
    
    @Mock
    private Tracer tracer;
    
    @Mock
    private MeterRegistry meterRegistry;
    
    @Mock
    private Span span;
    
    @Mock
    private Scope scope;
    
    @Mock
    private Counter totalPositionsCounter;
    
    @Mock
    private Counter filteredPositionsCounter;
    
    @Mock
    private Counter invalidPositionsCounter;
    
    @Mock
    private Counter zeroPositionsCounter;
    
    @Mock
    private Counter duplicatePositionsCounter;
    
    @Mock
    private Counter outdatedPositionsCounter;
    
    @Mock
    private Counter futurePositionsCounter;
    
    @Mock
    private Counter pastPositionsCounter;
    
    @Mock
    private Counter accuracyPositionsCounter;
    
    @Mock
    private Counter approximatePositionsCounter;
    
    @Mock
    private Counter staticPositionsCounter;
    
    @Mock
    private Counter distancePositionsCounter;
    
    @Mock
    private Counter maxSpeedPositionsCounter;
    
    @Mock
    private Counter minPeriodPositionsCounter;
    
    @Mock
    private Counter dailyLimitPositionsCounter;
    
    @Mock
    private Counter calendarPositionsCounter;
    
    @Mock
    private Timer filterProcessingTimer;
    
    private FilterHandler filterHandler;
    
    @BeforeEach
    public void setup() {
        // Setup default config values
        when(config.getBoolean(Keys.FILTER_INVALID)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_ZERO)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_DUPLICATE)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_OUTDATED)).thenReturn(true);
        when(config.getLong(Keys.FILTER_FUTURE)).thenReturn(5L); // 5 seconds
        when(config.getLong(Keys.FILTER_PAST)).thenReturn(5L); // 5 seconds
        when(config.getInteger(Keys.FILTER_ACCURACY)).thenReturn(100); // 100 meters
        when(config.getBoolean(Keys.FILTER_APPROXIMATE)).thenReturn(true);
        when(config.getBoolean(Keys.FILTER_STATIC)).thenReturn(true);
        when(config.getInteger(Keys.FILTER_DISTANCE)).thenReturn(10); // 10 meters
        when(config.getInteger(Keys.FILTER_MAX_SPEED)).thenReturn(500); // 500 knots
        when(config.getInteger(Keys.FILTER_MIN_PERIOD)).thenReturn(1); // 1 second
        when(config.getInteger(Keys.FILTER_DAILY_LIMIT)).thenReturn(1000); // 1000 messages
        when(config.getInteger(Keys.FILTER_DAILY_LIMIT_INTERVAL)).thenReturn(60); // 60 seconds
        when(config.getBoolean(Keys.FILTER_RELATIVE)).thenReturn(false);
        when(config.getLong(Keys.FILTER_SKIP_LIMIT)).thenReturn(300L); // 300 seconds
        when(config.getBoolean(Keys.FILTER_SKIP_ATTRIBUTES_ENABLE)).thenReturn(false);
        
        // Setup counter mocks
        setupCounterMock("position.total", totalPositionsCounter);
        setupCounterMock("position.filtered", filteredPositionsCounter);
        setupCounterMock("position.filtered.invalid", invalidPositionsCounter);
        setupCounterMock("position.filtered.zero", zeroPositionsCounter);
        setupCounterMock("position.filtered.duplicate", duplicatePositionsCounter);
        setupCounterMock("position.filtered.outdated", outdatedPositionsCounter);
        setupCounterMock("position.filtered.future", futurePositionsCounter);
        setupCounterMock("position.filtered.past", pastPositionsCounter);
        setupCounterMock("position.filtered.accuracy", accuracyPositionsCounter);
        setupCounterMock("position.filtered.approximate", approximatePositionsCounter);
        setupCounterMock("position.filtered.static", staticPositionsCounter);
        setupCounterMock("position.filtered.distance", distancePositionsCounter);
        setupCounterMock("position.filtered.maxspeed", maxSpeedPositionsCounter);
        setupCounterMock("position.filtered.minperiod", minPeriodPositionsCounter);
        setupCounterMock("position.filtered.dailylimit", dailyLimitPositionsCounter);
        setupCounterMock("position.filtered.calendar", calendarPositionsCounter);
        
        // Setup timer mock
        Timer.Builder timerBuilder = mock(Timer.Builder.class);
        when(meterRegistry.timer(eq("position.filter.processing.time"))).thenReturn(filterProcessingTimer);
        when(meterRegistry.timer(anyString())).thenReturn(filterProcessingTimer);
        when(timerBuilder.description(anyString())).thenReturn(timerBuilder);
        when(timerBuilder.register(meterRegistry)).thenReturn(filterProcessingTimer);
        when(meterRegistry.timer(anyString())).thenReturn(filterProcessingTimer);
        
        // Setup OpenTelemetry mocks
        when(tracer.spanBuilder(anyString())).thenReturn(mock(Tracer.SpanBuilder.class));
        when(tracer.spanBuilder(anyString()).startSpan()).thenReturn(span);
        when(span.makeCurrent()).thenReturn(scope);
        
        // Setup timer record method
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return false;
        }).when(filterProcessingTimer).record(any(Runnable.class));
        
        // Create the filter handler
        filterHandler = new FilterHandler(config, positionCacheManager, storage, metricsService, tracer, meterRegistry);
    }
    
    private void setupCounterMock(String name, Counter counter) {
        Counter.Builder counterBuilder = mock(Counter.Builder.class);
        when(meterRegistry.counter(eq(name))).thenReturn(counter);
        when(counterBuilder.description(anyString())).thenReturn(counterBuilder);
        when(counterBuilder.register(meterRegistry)).thenReturn(counter);
    }
    
    @Test
    public void testValidPosition() {
        Position position = createValidPosition();
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(null);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(false); // Not filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter, never()).increment();
        verify(span).setAttribute("filtered", false);
    }
    
    @Test
    public void testInvalidPosition() {
        Position position = createValidPosition();
        position.setValid(false);
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(invalidPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testZeroCoordinates() {
        Position position = createValidPosition();
        position.setLatitude(0);
        position.setLongitude(0);
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(zeroPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testDuplicatePosition() {
        Position position = createValidPosition();
        Position lastPosition = createValidPosition(); // Same fix time
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(lastPosition);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(duplicatePositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testOutdatedPosition() {
        Position position = createValidPosition();
        position.setOutdated(true);
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(outdatedPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testFuturePosition() {
        Position position = createValidPosition();
        position.setFixTime(new Date(System.currentTimeMillis() + 10000)); // 10 seconds in future
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(futurePositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testPastPosition() {
        Position position = createValidPosition();
        position.setFixTime(new Date(System.currentTimeMillis() - 10000)); // 10 seconds in past
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(pastPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testAccuracyFilter() {
        Position position = createValidPosition();
        position.setAccuracy(200.0); // 200 meters, above the 100m threshold
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(accuracyPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testApproximateFilter() {
        Position position = createValidPosition();
        position.set(Position.KEY_APPROXIMATE, true);
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(approximatePositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testStaticFilter() {
        Position position = createValidPosition();
        position.setSpeed(0.0); // Static position
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(null);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        // Static filter should not apply without a previous position
        verify(callback).processed(false); // Not filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter, never()).increment();
        verify(staticPositionsCounter, never()).increment();
        verify(span).setAttribute("filtered", false);
    }
    
    @Test
    public void testStaticFilterWithPreviousPosition() {
        Position position = createValidPosition();
        position.setSpeed(0.0); // Static position
        
        Position lastPosition = createValidPosition();
        lastPosition.setFixTime(new Date(position.getFixTime().getTime() - 5000)); // 5 seconds earlier
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(lastPosition);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(staticPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testDistanceFilter() {
        Position position = createValidPosition();
        position.set(Position.KEY_DISTANCE, 5.0); // 5 meters, below the 10m threshold
        
        Position lastPosition = createValidPosition();
        lastPosition.setFixTime(new Date(position.getFixTime().getTime() - 5000)); // 5 seconds earlier
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(lastPosition);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(distancePositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testMaxSpeedFilter() {
        Position position = createValidPosition();
        position.set(Position.KEY_DISTANCE, 10000.0); // 10km in 5 seconds = 2000 m/s = ~3888 knots
        
        Position lastPosition = createValidPosition();
        lastPosition.setFixTime(new Date(position.getFixTime().getTime() - 5000)); // 5 seconds earlier
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(lastPosition);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(maxSpeedPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testMinPeriodFilter() {
        Position position = createValidPosition();
        
        Position lastPosition = createValidPosition();
        lastPosition.setFixTime(new Date(position.getFixTime().getTime() - 500)); // 0.5 seconds earlier, below 1s threshold
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(lastPosition);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(minPeriodPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testDailyLimitFilter() {
        Position position = createValidPosition();
        
        Position lastPosition = createValidPosition();
        lastPosition.setFixTime(new Date(position.getFixTime().getTime() - 5000)); // 5 seconds earlier
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(lastPosition);
        when(metricsService.getMessageCount(position.getDeviceId())).thenReturn(1001); // Above 1000 limit
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(dailyLimitPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testCalendarFilter() {
        Position position = createValidPosition();
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        device.setCalendarId(1L);
        
        Calendar calendar = mock(Calendar.class);
        when(calendar.checkMoment(any(Date.class))).thenReturn(false); // Outside calendar
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(null);
        when(positionCacheManager.getCalendar(1L)).thenReturn(calendar);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        verify(callback).processed(true); // Filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter).increment();
        verify(calendarPositionsCounter).increment();
        verify(span).setAttribute("filtered", true);
    }
    
    @Test
    public void testSkipLimitOverride() {
        Position position = createValidPosition();
        position.setSpeed(0.0); // Static position
        position.setServerTime(new Date(System.currentTimeMillis()));
        
        Position lastPosition = createValidPosition();
        lastPosition.setFixTime(new Date(position.getFixTime().getTime() - 5000)); // 5 seconds earlier
        lastPosition.setServerTime(new Date(System.currentTimeMillis() - 400000)); // 400 seconds earlier, above 300s threshold
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(lastPosition);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        // Should not be filtered due to skip limit
        verify(callback).processed(false); // Not filtered
        verify(totalPositionsCounter).increment();
        verify(filteredPositionsCounter, never()).increment();
        verify(staticPositionsCounter, never()).increment();
        verify(span).setAttribute("filtered", false);
    }
    
    @Test
    public void testOpenTelemetryInstrumentation() {
        Position position = createValidPosition();
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(null);
        
        // Mock current span for @WithSpan annotation
        SpanContext spanContext = mock(SpanContext.class);
        when(span.getSpanContext()).thenReturn(spanContext);
        when(spanContext.isValid()).thenReturn(true);
        Mockito.mockStatic(Span.class);
        when(Span.current()).thenReturn(span);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        // Verify span attributes
        verify(span).setAttribute("deviceId", position.getDeviceId());
        verify(span).setAttribute("fixTime", position.getFixTime().toString());
        verify(span).setAttribute("filtered", false);
        
        // Verify timer was used
        verify(filterProcessingTimer).record(any(Runnable.class));
    }
    
    @Test
    public void testGetPrecedingPosition() throws StorageException {
        Position position = createValidPosition();
        Position precedingPosition = createValidPosition();
        precedingPosition.setId(100L);
        
        // Configure relative filtering
        when(config.getBoolean(Keys.FILTER_RELATIVE)).thenReturn(true);
        filterHandler = new FilterHandler(config, positionCacheManager, storage, metricsService, tracer, meterRegistry);
        
        // Mock storage query
        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(precedingPosition);
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        // Verify storage was queried
        ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
        verify(storage).getObject(eq(Position.class), requestCaptor.capture());
        
        Request request = requestCaptor.getValue();
        assertTrue(request.getCondition() instanceof Condition.And);
        
        // Verify span was created for getPrecedingPosition
        verify(tracer).spanBuilder("getPrecedingPosition");
    }
    
    @Test
    public void testGetPrecedingPositionWithException() throws StorageException {
        Position position = createValidPosition();
        
        // Configure relative filtering
        when(config.getBoolean(Keys.FILTER_RELATIVE)).thenReturn(true);
        filterHandler = new FilterHandler(config, positionCacheManager, storage, metricsService, tracer, meterRegistry);
        
        // Mock storage query to throw exception
        when(storage.getObject(eq(Position.class), any(Request.class))).thenThrow(new StorageException("Test exception"));
        
        Device device = new Device();
        device.setId(position.getDeviceId());
        
        when(positionCacheManager.getDevice(position.getDeviceId())).thenReturn(device);
        when(positionCacheManager.getLastPosition(position.getDeviceId())).thenReturn(null);
        
        BasePositionHandler.Callback callback = mock(BasePositionHandler.Callback.class);
        filterHandler.onPosition(position, callback);
        
        // Verify storage was queried
        verify(storage).getObject(eq(Position.class), any(Request.class));
        
        // Verify span was created for getPrecedingPosition and recorded exception
        verify(tracer).spanBuilder("getPrecedingPosition");
        verify(span).recordException(any(StorageException.class));
        verify(span).setStatus(StatusCode.ERROR, "Failed to get preceding position");
    }
    
    private Position createValidPosition() {
        Position position = new Position();
        position.setDeviceId(1L);
        position.setProtocol("test");
        position.setValid(true);
        position.setFixTime(new Date());
        position.setLatitude(10.0);
        position.setLongitude(20.0);
        position.setAltitude(30.0);
        position.setSpeed(50.0);
        position.setCourse(90.0);
        position.setAccuracy(10.0);
        position.setServerTime(new Date());
        return position;
    }
}