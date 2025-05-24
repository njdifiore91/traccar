# Event Handlers Documentation

## Overview

The Event Processing Service is a critical component in the Traccar microservices architecture responsible for analyzing position data to detect significant events such as geofence transitions, speed violations, device status changes, and maintenance alerts. This service implements complex event processing with stateful operations for time-window events.

This document provides comprehensive documentation for all event handlers implemented in the Event Processing Service, including their functionality, configuration options, and examples.

## Architecture

The Event Processing Service follows an event-driven architecture pattern:

1. It consumes enriched position data from the Message Broker (published by the Position Processing Service)
2. Processes this data through a series of specialized event handlers
3. Detects events based on configurable rules and conditions
4. Publishes detected events to the Message Broker for consumption by other services (primarily the Notification Service)

Each event handler is responsible for detecting a specific type of event and operates independently of other handlers. The service maintains state where necessary for detecting events that depend on historical data.

## Event Handler Base Class

All event handlers extend the `BaseEventHandler` abstract class, which defines the core contract for event detection:

```java
public abstract class BaseEventHandler {
    public interface Callback {
        void eventDetected(Event event);
    }
    
    public abstract void onPosition(Position position, Callback callback);
}
```

Event handlers implement the `onPosition` method to analyze position data and call the `eventDetected` method on the callback when an event is detected.

## Event Handlers

### AlarmEventHandler

**Purpose**: Detects alarm events reported directly by devices.

**Configuration Parameters**:
- `event.ignoreDuplicateAlerts` - When enabled, prevents duplicate alarms of the same type from being generated consecutively

**Detection Logic**:
1. Extracts the alarm value from the position's attributes (`Position.KEY_ALARM`)
2. If the alarm string contains multiple alarms (comma-separated), processes each one individually
3. When `ignoreDuplicateAlerts` is enabled, compares with the previous position's alarms to filter out duplicates
4. Creates an event of type `Event.TYPE_ALARM` for each unique alarm

**Example Scenarios**:
- A device reports a low battery alarm
- A device reports multiple alarms simultaneously (e.g., "lowBattery,powerCut")
- A device continues to report the same alarm in consecutive positions (handled by duplicate filtering)

**Troubleshooting**:
- If alarms are not being detected, verify that the device protocol correctly populates the `Position.KEY_ALARM` attribute
- If duplicate alarms are being generated, check the `event.ignoreDuplicateAlerts` configuration
- Review the protocol documentation for your specific device to understand what alarm types it supports

### BehaviorEventHandler

**Purpose**: Detects harsh driving behavior events such as rapid acceleration and hard braking.

**Configuration Parameters**:
- `event.behavior.accelerationThreshold` - Acceleration threshold in m/s² for triggering acceleration events
- `event.behavior.brakingThreshold` - Deceleration threshold in m/s² for triggering braking events

**Detection Logic**:
1. Calculates acceleration between the current position and the previous position
2. Converts speed change from knots to m/s and divides by the time difference to get acceleration in m/s²
3. Compares the calculated acceleration with the configured thresholds
4. Creates an event of type `Event.TYPE_ALARM` with alarm type `Position.ALARM_ACCELERATION` or `Position.ALARM_BRAKING`

**Example Scenarios**:
- A vehicle accelerates rapidly from a stop, exceeding the acceleration threshold
- A vehicle brakes suddenly, exceeding the braking threshold

**Troubleshooting**:
- If behavior events are not being detected, check that the acceleration/braking thresholds are appropriate for your use case
- Ensure that the device is reporting accurate speed values and timestamps
- Higher frequency position reporting will provide more accurate behavior detection

### CommandResultEventHandler

**Purpose**: Detects and processes command result events reported by devices.

**Configuration Parameters**: None

**Detection Logic**:
1. Checks if the position contains a `Position.KEY_RESULT` attribute
2. If present, creates an event of type `Event.TYPE_COMMAND_RESULT` with the result value

**Example Scenarios**:
- A device reports the result of a previously sent command
- A device acknowledges receipt of a command with a success/failure status

**Troubleshooting**:
- If command results are not being detected, verify that the device protocol correctly populates the `Position.KEY_RESULT` attribute
- Check the protocol documentation for your specific device to understand how it reports command results

### DriverEventHandler

**Purpose**: Detects driver change events when a driver ID is reported by the device.

**Configuration Parameters**: None

**Detection Logic**:
1. Checks if the position contains a `Position.KEY_DRIVER_UNIQUE_ID` attribute
2. Compares with the previous position's driver ID (if available)
3. If the driver ID has changed, creates an event of type `Event.TYPE_DRIVER_CHANGED`

**Example Scenarios**:
- A driver logs in to the device using an RFID card or code
- A different driver takes over the vehicle and identifies themselves

**Troubleshooting**:
- If driver change events are not being detected, verify that the device protocol correctly populates the `Position.KEY_DRIVER_UNIQUE_ID` attribute
- Ensure that the device is configured to report driver identification
- Check that the driver identification method (RFID, code entry, etc.) is working correctly

### FuelEventHandler

**Purpose**: Detects fuel level changes that may indicate refilling or fuel theft.

**Configuration Parameters**:
- `event.fuel.increase.threshold` - Minimum fuel level increase to trigger a fuel increase event
- `event.fuel.drop.threshold` - Minimum fuel level decrease to trigger a fuel drop event

**Detection Logic**:
1. Checks if the position contains a `Position.KEY_FUEL_LEVEL` attribute
2. Compares with the previous position's fuel level
3. If the fuel level has increased by more than the increase threshold, creates an event of type `Event.TYPE_DEVICE_FUEL_INCREASE`
4. If the fuel level has decreased by more than the drop threshold, creates an event of type `Event.TYPE_DEVICE_FUEL_DROP`
5. Both events include the before and after fuel levels as attributes

**Example Scenarios**:
- A vehicle is refueled, causing a significant increase in fuel level
- Fuel is siphoned from a vehicle, causing a significant decrease in fuel level
- Normal fuel consumption during driving (typically below the drop threshold)

**Troubleshooting**:
- If fuel events are not being detected, verify that the device correctly reports fuel levels
- Adjust the threshold values based on the fuel sensor's accuracy and your specific requirements
- Consider the fuel sensor's precision and potential for fluctuations in readings

### GeofenceEventHandler

**Purpose**: Detects when a device enters or exits a geofence.

**Configuration Parameters**: None (geofences are configured as separate entities)

**Detection Logic**:
1. Compares the current position's geofence IDs with the previous position's geofence IDs
2. For geofences that are in the current position but not in the previous position, creates an event of type `Event.TYPE_GEOFENCE_ENTER`
3. For geofences that are in the previous position but not in the current position, creates an event of type `Event.TYPE_GEOFENCE_EXIT`
4. If a geofence has an associated calendar, checks if the current time is within the calendar's active periods before generating events

**Example Scenarios**:
- A vehicle enters a customer site geofence
- A vehicle exits a warehouse geofence
- A vehicle enters a geofence during non-working hours (handled by calendar check)

**Troubleshooting**:
- If geofence events are not being detected, verify that geofences are correctly defined and assigned to the device
- Check that the position's coordinates are accurate
- If using calendars with geofences, ensure that the calendar is correctly configured
- Geofence calculations are performed by the Position Processing Service, so ensure that it's correctly populating the geofence IDs

### IgnitionEventHandler

**Purpose**: Detects ignition on and off events.

**Configuration Parameters**: None

**Detection Logic**:
1. Checks if the position contains a `Position.KEY_IGNITION` attribute
2. Compares with the previous position's ignition state
3. If ignition has changed from off to on, creates an event of type `Event.TYPE_IGNITION_ON`
4. If ignition has changed from on to off, creates an event of type `Event.TYPE_IGNITION_OFF`

**Example Scenarios**:
- A vehicle's engine is started, turning ignition on
- A vehicle's engine is turned off, turning ignition off

**Troubleshooting**:
- If ignition events are not being detected, verify that the device correctly reports ignition status
- Some devices may determine ignition status from voltage, engine RPM, or movement; check the device configuration
- Ensure that the device protocol correctly populates the `Position.KEY_IGNITION` attribute

### MaintenanceEventHandler

**Purpose**: Detects when a device reaches a maintenance threshold based on various metrics.

**Configuration Parameters**:
- Maintenance entities with the following attributes:
  - `type` - The attribute to monitor (e.g., odometer, hours, totalDistance)
  - `start` - The starting value for the maintenance cycle
  - `period` - The interval at which maintenance is required

**Detection Logic**:
1. For each maintenance entity associated with the device:
   a. Gets the current value of the specified attribute from the position
   b. Gets the previous value from the last position
   c. If both values are non-zero and the current value is at or above the maintenance start value
   d. Calculates how many maintenance periods have been completed for both values
   e. If the current value has completed more periods than the previous value, creates an event of type `Event.TYPE_MAINTENANCE`

**Example Scenarios**:
- A vehicle reaches 10,000 km since the last oil change (with a 10,000 km maintenance period)
- An engine reaches 500 hours of operation (with a 500-hour maintenance period)
- A vehicle crosses the 50,000 km threshold for major service

**Troubleshooting**:
- If maintenance events are not being detected, verify that the maintenance entities are correctly configured
- Ensure that the device is reporting the attributes specified in the maintenance entity's type
- Check that the start and period values are appropriate for your maintenance schedule

### MediaEventHandler

**Purpose**: Detects when a device reports media content (images, video, audio).

**Configuration Parameters**: None

**Detection Logic**:
1. Checks if the position contains any of the media attributes: `Position.KEY_IMAGE`, `Position.KEY_VIDEO`, or `Position.KEY_AUDIO`
2. For each media attribute present, creates an event of type `Event.TYPE_MEDIA` with the media type and file information

**Example Scenarios**:
- A device captures and sends an image (e.g., from a dashcam)
- A device records and sends an audio clip
- A device uploads a video segment

**Troubleshooting**:
- If media events are not being detected, verify that the device protocol correctly populates the media attributes
- Ensure that the device is configured to capture and send media
- Check that the media file paths or URLs are correctly formatted

### MotionEventHandler

**Purpose**: Detects when a device starts or stops moving.

**Configuration Parameters**:
- `event.motion.processInvalidPositions` - Whether to process positions with invalid GPS fix
- Trip configuration parameters (from the `TripsConfig` class):
  - `device.tripDistance` - Minimum distance for a position to be considered moving
  - `device.minimalTripDistance` - Minimum total distance for a trip to be recorded
  - `device.minimalTripDuration` - Minimum duration for a trip to be recorded
  - `device.minimalParkingDuration` - Minimum duration for a stop to be considered parking
  - `device.minimalNoDataDuration` - Minimum duration without data to end a trip
  - `device.speedThreshold` - Speed threshold to determine if a device is moving

**Detection Logic**:
1. Uses the `MotionProcessor` to update the device's motion state based on the current position
2. The motion state tracks whether the device is moving, stopped, or unknown
3. When the state changes from stopped to moving, creates an event of type `Event.TYPE_DEVICE_MOVING`
4. When the state changes from moving to stopped, creates an event of type `Event.TYPE_DEVICE_STOPPED`
5. Updates the device's motion state in the database

**Example Scenarios**:
- A vehicle starts moving after being parked
- A vehicle stops and remains stationary for the minimum parking duration
- A device reports movement based on an accelerometer or motion sensor

**Troubleshooting**:
- If motion events are not being detected, check the trip configuration parameters
- Ensure that the device is reporting accurate speed and position data
- For devices that determine motion from sensors rather than GPS, verify that the `Position.KEY_MOTION` attribute is correctly populated

### OverspeedEventHandler

**Purpose**: Detects when a device exceeds a speed limit.

**Configuration Parameters**:
- `event.overspeed.minimalDuration` - Minimum duration (in seconds) of speeding before an event is generated
- `event.overspeed.preferLowest` - Whether to use the lowest applicable speed limit when multiple limits apply
- `event.overspeed.threshold.multiplier` - Multiplier applied to the speed limit to determine the actual threshold
- `event.overspeed.limit` - Default speed limit (if no other limits apply)

**Detection Logic**:
1. Determines the applicable speed limit from multiple sources (in order of precedence):
   a. Position-specific speed limit (`Position.KEY_SPEED_LIMIT`)
   b. Geofence-specific speed limit
   c. Device/group speed limit (`event.overspeed.limit`)
2. Uses the `OverspeedProcessor` to update the device's overspeed state based on the current position and speed limit
3. The overspeed state tracks whether the device is speeding, for how long, and which geofence's speed limit was exceeded
4. When speeding is detected for longer than the minimal duration, creates an event of type `Event.TYPE_DEVICE_OVERSPEED`
5. When the device returns to normal speed, creates an event of type `Event.TYPE_DEVICE_OVERSPEED_RESTORED`
6. Updates the device's overspeed state in the database

**Example Scenarios**:
- A vehicle exceeds the speed limit on a highway
- A vehicle exceeds a lower speed limit when entering a geofence (e.g., school zone)
- A vehicle returns to normal speed after speeding

**Troubleshooting**:
- If overspeed events are not being detected, verify that speed limits are correctly configured
- Check that the minimal duration is appropriate for your use case
- Ensure that the device is reporting accurate speed data
- For geofence-specific speed limits, verify that the geofences are correctly defined and have speed limit attributes

## Event Processing Flow

The Event Processing Service follows this general flow for processing positions and detecting events:

1. Receives enriched position data from the Message Broker
2. For each position, iterates through all registered event handlers
3. Each handler analyzes the position and potentially generates events
4. Detected events are stored in the database and published to the Message Broker
5. The Notification Service consumes these events and delivers notifications based on configured rules

## Integration with Other Services

The Event Processing Service integrates with other microservices in the Traccar ecosystem:

- **Position Processing Service**: Provides enriched position data with geofence information, speed limits, etc.
- **Notification Service**: Consumes detected events and delivers notifications to users
- **API Gateway**: Provides access to event data and configuration through the REST API
- **Database**: Stores event data and configuration
- **Message Broker**: Facilitates asynchronous communication between services

## Troubleshooting Guide

### Common Issues

1. **Events not being detected**
   - Verify that the device is reporting the necessary attributes for the event type
   - Check that the event handler is correctly configured
   - Ensure that the position data is being correctly processed by the Position Processing Service

2. **Duplicate events being generated**
   - Check for duplicate position reports from the device
   - Verify that the `event.ignoreDuplicateAlerts` setting is enabled for alarm events
   - Ensure that the event handler correctly compares with previous state

3. **Delayed event detection**
   - Check the `event.overspeed.minimalDuration` setting for overspeed events
   - Verify that the Position Processing Service is promptly processing positions
   - Ensure that the Message Broker is not experiencing delays

4. **Missing geofence events**
   - Verify that geofences are correctly defined and assigned to the device
   - Check that the Position Processing Service is correctly calculating geofence containment
   - Ensure that the device is reporting accurate position data

### Logging and Debugging

The Event Processing Service provides detailed logging to help diagnose issues:

- Set the log level to `DEBUG` or `TRACE` for more detailed information
- Check the logs for any exceptions or warnings related to event processing
- Monitor the Message Broker for message flow between services

### Performance Considerations

- Each event handler processes every position, so the number of handlers affects processing time
- Complex event detection logic (e.g., geofence calculations) can impact performance
- Consider the frequency of position reports when configuring event parameters
- The service can be scaled horizontally to handle increased load

## Configuration Reference

### Event Handler Configuration

The following configuration parameters affect event detection:

| Parameter | Default | Description |
| --- | --- | --- |
| `event.ignoreDuplicateAlerts` | false | Prevents duplicate alarm events from being generated consecutively |
| `event.behavior.accelerationThreshold` | 0 | Acceleration threshold in m/s² for triggering acceleration events |
| `event.behavior.brakingThreshold` | 0 | Deceleration threshold in m/s² for triggering braking events |
| `event.fuel.increase.threshold` | 0 | Minimum fuel level increase to trigger a fuel increase event |
| `event.fuel.drop.threshold` | 0 | Minimum fuel level decrease to trigger a fuel drop event |
| `event.motion.processInvalidPositions` | false | Whether to process positions with invalid GPS fix for motion detection |
| `event.overspeed.minimalDuration` | 0 | Minimum duration (in seconds) of speeding before an event is generated |
| `event.overspeed.preferLowest` | false | Whether to use the lowest applicable speed limit when multiple limits apply |
| `event.overspeed.threshold.multiplier` | 1.0 | Multiplier applied to the speed limit to determine the actual threshold |
| `event.overspeed.limit` | 0 | Default speed limit (if no other limits apply) |

### Trip Configuration

The following parameters affect motion detection and trip recording:

| Parameter | Default | Description |
| --- | --- | --- |
| `device.tripDistance` | 500 | Minimum distance (in meters) for a position to be considered moving |
| `device.minimalTripDistance` | 500 | Minimum total distance (in meters) for a trip to be recorded |
| `device.minimalTripDuration` | 300 | Minimum duration (in seconds) for a trip to be recorded |
| `device.minimalParkingDuration` | 300 | Minimum duration (in seconds) for a stop to be considered parking |
| `device.minimalNoDataDuration` | 3600 | Minimum duration (in seconds) without data to end a trip |
| `device.speedThreshold` | 0 | Speed threshold (in knots) to determine if a device is moving |

## Conclusion

The Event Processing Service is a critical component of the Traccar system, providing real-time event detection based on position data. By understanding the various event handlers and their configuration options, you can customize the system to meet your specific requirements and effectively troubleshoot any issues that arise.