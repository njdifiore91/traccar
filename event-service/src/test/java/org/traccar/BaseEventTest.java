package org.traccar;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.traccar.handler.events.BaseEventHandler;
import org.traccar.messaging.EventProducer;
import org.traccar.messaging.MessageConverter;
import org.traccar.messaging.PositionConsumer;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.Position;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Base test class for Event Service tests that provides common mock setup and test utilities.
 * This class extends the functionality of the original BaseTest but is tailored specifically
 * for event processing needs in the microservice architecture.
 */
public class BaseEventTest {

    @Mock
    protected EventProducer eventProducer;

    @Mock
    protected PositionConsumer positionConsumer;

    @Mock
    protected MessageConverter messageConverter;

    @Mock
    protected Tracer tracer;

    @Mock
    protected Span span;

    /**
     * Initialize mocks before each test.
     */
    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        when(tracer.spanBuilder(any())).thenReturn(mock(Span.Builder.class));
        when(tracer.spanBuilder(any()).startSpan()).thenReturn(span);
    }

    /**
     * Injects common dependencies into an event handler for testing.
     *
     * @param handler The event handler to inject dependencies into
     * @param <T> The type of event handler
     * @return The handler with injected dependencies
     * @throws Exception If an error occurs during injection
     */
    protected <T extends BaseEventHandler> T inject(T handler) throws Exception {
        // Mock device for testing
        Device device = mock(Device.class);
        when(device.getId()).thenReturn(1L);
        when(device.getUniqueId()).thenReturn("123456789012345");

        // Mock cache manager
        var cacheManager = mock(org.traccar.session.cache.CacheManager.class);
        when(cacheManager.getObject(eq(Device.class), anyLong())).thenReturn(device);

        // Set up handler with mocked dependencies
        handler.setCacheManager(cacheManager);
        handler.setEventProducer(eventProducer);
        handler.setTracer(tracer);

        return handler;
    }

    /**
     * Creates a test position with the specified attributes.
     *
     * @param deviceId The device ID for the position
     * @param time The timestamp for the position
     * @param latitude The latitude coordinate
     * @param longitude The longitude coordinate
     * @param attributes Additional attributes for the position
     * @return A position object with the specified attributes
     */
    protected Position createPosition(long deviceId, Date time, double latitude, double longitude, Map<String, Object> attributes) {
        Position position = new Position();
        position.setId(1L);
        position.setDeviceId(deviceId);
        position.setProtocol("test");
        position.setTime(time);
        position.setLatitude(latitude);
        position.setLongitude(longitude);
        position.setValid(true);

        if (attributes != null) {
            position.setAttributes(attributes);
        } else {
            position.setAttributes(new HashMap<>());
        }

        return position;
    }

    /**
     * Creates a test position with default attributes.
     *
     * @param deviceId The device ID for the position
     * @return A position object with default attributes
     */
    protected Position createPosition(long deviceId) {
        return createPosition(deviceId, new Date(), 0, 0, null);
    }

    /**
     * Creates a test event with the specified attributes.
     *
     * @param type The event type
     * @param deviceId The device ID for the event
     * @param positionId The position ID associated with the event
     * @param attributes Additional attributes for the event
     * @return An event object with the specified attributes
     */
    protected Event createEvent(String type, long deviceId, long positionId, Map<String, Object> attributes) {
        Event event = new Event(type, deviceId, positionId);
        
        if (attributes != null) {
            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                event.set(entry.getKey(), entry.getValue());
            }
        }
        
        return event;
    }

    /**
     * Creates a test event with default attributes.
     *
     * @param type The event type
     * @param deviceId The device ID for the event
     * @param positionId The position ID associated with the event
     * @return An event object with default attributes
     */
    protected Event createEvent(String type, long deviceId, long positionId) {
        return createEvent(type, deviceId, positionId, null);
    }

    /**
     * Creates a position with motion attributes.
     *
     * @param deviceId The device ID for the position
     * @param motionState The motion state (true for moving, false for stopped)
     * @return A position object with motion attributes
     */
    protected Position createMotionPosition(long deviceId, boolean motionState) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Position.KEY_MOTION, motionState);
        return createPosition(deviceId, new Date(), 0, 0, attributes);
    }

    /**
     * Creates a position with ignition attributes.
     *
     * @param deviceId The device ID for the position
     * @param ignitionState The ignition state (true for on, false for off)
     * @return A position object with ignition attributes
     */
    protected Position createIgnitionPosition(long deviceId, boolean ignitionState) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Position.KEY_IGNITION, ignitionState);
        return createPosition(deviceId, new Date(), 0, 0, attributes);
    }

    /**
     * Creates a position with speed attributes.
     *
     * @param deviceId The device ID for the position
     * @param speed The speed value in knots
     * @return A position object with speed attributes
     */
    protected Position createSpeedPosition(long deviceId, double speed) {
        Position position = createPosition(deviceId, new Date(), 0, 0, null);
        position.setSpeed(speed);
        return position;
    }

    /**
     * Creates a position with alarm attributes.
     *
     * @param deviceId The device ID for the position
     * @param alarmType The alarm type string
     * @return A position object with alarm attributes
     */
    protected Position createAlarmPosition(long deviceId, String alarmType) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Position.KEY_ALARM, alarmType);
        return createPosition(deviceId, new Date(), 0, 0, attributes);
    }

    /**
     * Creates a position with command result attributes.
     *
     * @param deviceId The device ID for the position
     * @param success Whether the command was successful
     * @param result The command result string
     * @return A position object with command result attributes
     */
    protected Position createCommandResultPosition(long deviceId, boolean success, String result) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Position.KEY_RESULT, result);
        attributes.put(Position.KEY_SUCCESS, success);
        return createPosition(deviceId, new Date(), 0, 0, attributes);
    }

    /**
     * Creates a position with maintenance attributes.
     *
     * @param deviceId The device ID for the position
     * @param odometer The odometer value in kilometers
     * @param hours The engine hours value
     * @return A position object with maintenance attributes
     */
    protected Position createMaintenancePosition(long deviceId, double odometer, double hours) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(Position.KEY_ODOMETER, odometer);
        attributes.put(Position.KEY_HOURS, hours);
        return createPosition(deviceId, new Date(), 0, 0, attributes);
    }

    /**
     * Creates a position with geofence attributes.
     *
     * @param deviceId The device ID for the position
     * @param latitude The latitude coordinate
     * @param longitude The longitude coordinate
     * @return A position object with geofence-testable coordinates
     */
    protected Position createGeofencePosition(long deviceId, double latitude, double longitude) {
        return createPosition(deviceId, new Date(), latitude, longitude, null);
    }
}