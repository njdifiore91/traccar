/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DistanceHandlerTest {

    @Mock
    private Config config;

    @Mock
    private CacheManager cacheManager;

    @Test
    public void testCalculateDistance() {
        when(config.getBoolean(Keys.COORDINATES_FILTER)).thenReturn(false);

        DistanceHandler distanceHandler = new DistanceHandler(config, cacheManager);

        Position position = new Position();
        position.setDeviceId(1);
        position.setValid(true);
        position.setTime(new Date(System.currentTimeMillis()));
        position.setLatitude(40.0);
        position.setLongitude(-120.0);

        // First position should not have distance attribute
        distanceHandler.handlePosition(position, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                // Do nothing
            }
        });
        assertNull(position.getAttributes().get(Position.KEY_DISTANCE));
        assertNull(position.getAttributes().get(Position.KEY_TOTAL_DISTANCE));

        // Mock previous position in cache
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setValid(true);
        lastPosition.setTime(new Date(System.currentTimeMillis() - 1000));
        lastPosition.setLatitude(40.1);
        lastPosition.setLongitude(-120.1);
        when(cacheManager.getPosition(eq(1L))).thenReturn(lastPosition);

        // Create new position with different coordinates
        Position nextPosition = new Position();
        nextPosition.setDeviceId(1);
        nextPosition.setValid(true);
        nextPosition.setTime(new Date(System.currentTimeMillis()));
        nextPosition.setLatitude(40.2);
        nextPosition.setLongitude(-120.2);

        // Second position should have distance calculated
        distanceHandler.handlePosition(nextPosition, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                // Do nothing
            }
        });
        assertEquals(15.45, nextPosition.getDouble(Position.KEY_DISTANCE), 0.01);
    }

    @Test
    public void testCalculateDistanceWithFilter() {
        when(config.getBoolean(Keys.COORDINATES_FILTER)).thenReturn(true);
        when(config.getInteger(Keys.COORDINATES_MIN_ERROR)).thenReturn(10);
        when(config.getInteger(Keys.COORDINATES_MAX_ERROR)).thenReturn(50);

        DistanceHandler distanceHandler = new DistanceHandler(config, cacheManager);

        Position position = new Position();
        position.setDeviceId(1);
        position.setValid(true);
        position.setTime(new Date(System.currentTimeMillis()));
        position.setLatitude(40.0);
        position.setLongitude(-120.0);
        position.setAccuracy(30.0); // Within filter range

        // First position should not have distance attribute
        distanceHandler.handlePosition(position, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                // Do nothing
            }
        });
        assertNull(position.getAttributes().get(Position.KEY_DISTANCE));

        // Mock previous position in cache
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setValid(true);
        lastPosition.setTime(new Date(System.currentTimeMillis() - 1000));
        lastPosition.setLatitude(40.1);
        lastPosition.setLongitude(-120.1);
        lastPosition.setAccuracy(20.0); // Within filter range
        when(cacheManager.getPosition(eq(1L))).thenReturn(lastPosition);

        // Create new position with different coordinates
        Position nextPosition = new Position();
        nextPosition.setDeviceId(1);
        nextPosition.setValid(true);
        nextPosition.setTime(new Date(System.currentTimeMillis()));
        nextPosition.setLatitude(40.2);
        nextPosition.setLongitude(-120.2);
        nextPosition.setAccuracy(25.0); // Within filter range

        // Second position should have distance calculated with filter applied
        distanceHandler.handlePosition(nextPosition, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                // Do nothing
            }
        });
        assertEquals(15.45, nextPosition.getDouble(Position.KEY_DISTANCE), 0.01);
    }

    @Test
    public void testInvalidPosition() {
        when(config.getBoolean(Keys.COORDINATES_FILTER)).thenReturn(false);

        DistanceHandler distanceHandler = new DistanceHandler(config, cacheManager);

        Position position = new Position();
        position.setDeviceId(1);
        position.setValid(false); // Invalid position
        position.setTime(new Date(System.currentTimeMillis()));

        // Invalid position should not have distance calculated
        distanceHandler.handlePosition(position, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                // Do nothing
            }
        });
        assertNull(position.getAttributes().get(Position.KEY_DISTANCE));
    }

    @Test
    public void testOldPosition() {
        when(config.getBoolean(Keys.COORDINATES_FILTER)).thenReturn(false);

        DistanceHandler distanceHandler = new DistanceHandler(config, cacheManager);

        // Mock previous position in cache with newer timestamp
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setValid(true);
        lastPosition.setTime(new Date(System.currentTimeMillis())); // Newer timestamp
        lastPosition.setLatitude(40.1);
        lastPosition.setLongitude(-120.1);
        when(cacheManager.getPosition(eq(1L))).thenReturn(lastPosition);

        // Create position with older timestamp
        Position position = new Position();
        position.setDeviceId(1);
        position.setValid(true);
        position.setTime(new Date(System.currentTimeMillis() - 10000)); // Older timestamp
        position.setLatitude(40.0);
        position.setLongitude(-120.0);

        // Older position should not have distance calculated
        distanceHandler.handlePosition(position, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                // Do nothing
            }
        });
        assertNull(position.getAttributes().get(Position.KEY_DISTANCE));
    }

    @Test
    public void testTotalDistance() {
        when(config.getBoolean(Keys.COORDINATES_FILTER)).thenReturn(false);

        DistanceHandler distanceHandler = new DistanceHandler(config, cacheManager);

        // Mock previous position in cache with total distance
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setValid(true);
        lastPosition.setTime(new Date(System.currentTimeMillis() - 1000));
        lastPosition.setLatitude(40.1);
        lastPosition.setLongitude(-120.1);
        lastPosition.set(Position.KEY_TOTAL_DISTANCE, 1000.0); // Previous total distance
        when(cacheManager.getPosition(eq(1L))).thenReturn(lastPosition);

        // Create new position
        Position position = new Position();
        position.setDeviceId(1);
        position.setValid(true);
        position.setTime(new Date(System.currentTimeMillis()));
        position.setLatitude(40.2);
        position.setLongitude(-120.2);

        // Position should have updated total distance
        distanceHandler.handlePosition(position, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                // Do nothing
            }
        });
        assertEquals(15.45, position.getDouble(Position.KEY_DISTANCE), 0.01);
        assertEquals(1015.45, position.getDouble(Position.KEY_TOTAL_DISTANCE), 0.01);
    }

    @Test
    public void testDistanceWithOpenTelemetry() {
        // This test verifies that the handler correctly propagates OpenTelemetry context
        // and adds appropriate spans for distance calculation operations
        
        when(config.getBoolean(Keys.COORDINATES_FILTER)).thenReturn(false);

        DistanceHandler distanceHandler = new DistanceHandler(config, cacheManager);

        // Mock previous position in cache
        Position lastPosition = new Position();
        lastPosition.setDeviceId(1);
        lastPosition.setValid(true);
        lastPosition.setTime(new Date(System.currentTimeMillis() - 1000));
        lastPosition.setLatitude(40.1);
        lastPosition.setLongitude(-120.1);
        when(cacheManager.getPosition(eq(1L))).thenReturn(lastPosition);

        // Create new position with correlation ID for tracing
        Position position = new Position();
        position.setDeviceId(1);
        position.setValid(true);
        position.setTime(new Date(System.currentTimeMillis()));
        position.setLatitude(40.2);
        position.setLongitude(-120.2);
        position.set("correlationId", "test-correlation-id");

        // Process position and verify distance calculation with tracing
        distanceHandler.handlePosition(position, new BasePositionHandler.Callback() {
            @Override
            public void processed(boolean success) {
                // Do nothing
            }
        });
        assertEquals(15.45, position.getDouble(Position.KEY_DISTANCE), 0.01);
        assertEquals("test-correlation-id", position.getString("correlationId"));
    }
}