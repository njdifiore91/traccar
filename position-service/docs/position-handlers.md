# Position Processing Handlers

## Overview

The Position Processing Service implements a sequential processing pipeline for GPS position data. This pipeline consists of specialized handlers that validate, enrich, and transform position data as it flows through the system. Each handler performs a specific function in the processing chain, from filtering invalid positions to adding geocoding information and calculating derived attributes.

This document provides a comprehensive reference for the position processing handlers, their configuration options, and the order in which they are executed in the processing pipeline.

## Handler Chain Architecture

The Position Processing Service uses a handler chain pattern where position data flows through a series of specialized handlers in a defined order. Each handler can:

- Process and modify the position data
- Filter out positions that don't meet specific criteria
- Perform asynchronous operations (like geocoding)
- Add additional attributes or context to the position

All handlers extend the `BasePositionHandler` abstract class, which provides common functionality and a consistent interface for the handler chain.

### Handler Execution Flow

```
Raw Position → Handler 1 → Handler 2 → ... → Handler N → Enriched Position
```

Each handler in the chain calls the next handler after completing its processing through a callback mechanism. If a handler determines that a position should be filtered out (e.g., invalid coordinates), it can terminate the chain early.

### Handler Order

Handlers are executed in the following order:

1. **Early Computed Attributes Handler** - Processes computed attributes with negative priority
2. **Filter Handler** - Filters out invalid or redundant positions
3. **Distance Handler** - Calculates distance and other motion metrics
4. **Geofence Handler** - Checks positions against defined geofences
5. **Geocoder Handler** - Performs reverse geocoding to add address information
6. **Late Computed Attributes Handler** - Processes computed attributes with positive priority

This order ensures that:
- Basic validation happens early in the pipeline
- Expensive operations (like geocoding) only occur for valid positions
- Computed attributes can be calculated both before and after other enrichment

## Handler Reference

### BasePositionHandler

**Purpose**: Abstract base class that defines the common interface for all position handlers.

**Key Features**:
- Defines the handler interface with `onPosition` method
- Provides error handling for all handlers
- Uses a callback mechanism to signal when processing is complete

**Implementation**:
```java
public abstract class BasePositionHandler {

    public interface Callback {
        void processed(boolean filtered);
    }

    public abstract void onPosition(Position position, Callback callback);

    public void handlePosition(Position position, Callback callback) {
        try {
            onPosition(position, callback);
        } catch (RuntimeException e) {
            LOGGER.warn("Position handler failed", e);
            callback.processed(false);
        }
    }
}
```

### ComputedAttributesHandler

**Purpose**: Calculates additional attributes based on expressions defined in the database. This handler can run both early (before other handlers) and late (after other handlers) in the pipeline.

**Key Features**:
- Evaluates JEXL expressions to compute attribute values
- Can modify core position fields (latitude, longitude, speed, etc.)
- Can add custom attributes to the position
- Supports both early and late execution in the handler chain

**Configuration Options**:

| Parameter | Description | Default |
|-----------|-------------|--------|
| `processing.computedAttributes.localVariables` | Allow local variables in expressions | false |
| `processing.computedAttributes.loops` | Allow loops in expressions | false |
| `processing.computedAttributes.newInstanceCreation` | Allow creating new objects in expressions | false |
| `processing.computedAttributes.deviceAttributes` | Include device attributes in expression context | true |
| `processing.computedAttributes.lastAttributes` | Include previous position attributes in expression context | true |

**Example Expressions**:

```
# Calculate speed in km/h instead of knots
speed * 1.852

# Set custom attribute based on condition
speed > 90 ? true : false

# Access device attribute
deviceId == 1 && deviceName == 'Vehicle1'

# Use previous position data
speed > lastSpeed * 2
```

### FilterHandler

**Purpose**: Filters out invalid or redundant positions based on configurable criteria.

**Key Features**:
- Validates position data (coordinates, timestamps, etc.)
- Filters out duplicate or redundant positions
- Applies distance-based filtering
- Implements speed validation
- Supports calendar-based filtering

**Configuration Options**:

| Parameter | Description | Default |
|-----------|-------------|--------|
| `filter.invalid` | Filter out invalid positions | true |
| `filter.zero` | Filter out zero coordinates (0.0, 0.0) | true |
| `filter.duplicate` | Filter out duplicate positions (same time) | false |
| `filter.outdated` | Filter out positions marked as outdated | false |
| `filter.future` | Max time difference in seconds for future positions | 0 |
| `filter.past` | Max time difference in seconds for past positions | 0 |
| `filter.accuracy` | Max position accuracy in meters (0 = disabled) | 0 |
| `filter.approximate` | Filter out approximate positions | false |
| `filter.static` | Filter out positions with zero speed | false |
| `filter.distance` | Min distance in meters between positions | 0 |
| `filter.maxSpeed` | Max speed in knots (0 = disabled) | 0 |
| `filter.minPeriod` | Min time period in seconds between positions | 0 |
| `filter.dailyLimit` | Max number of stored positions per device per day | 0 |
| `filter.dailyLimitInterval` | Min time interval in seconds for daily limit | 0 |
| `filter.relative` | Use preceding position from database for checks | false |
| `filter.skipLimit` | Time limit in seconds for skipping filtering | 0 |
| `filter.skipAttributes.enable` | Enable attribute-based skip filtering | false |
| `filter.skipAttributes` | Comma-separated list of attributes to skip filtering | "" |

### GeofenceHandler

**Purpose**: Checks positions against defined geofences and adds geofence information to the position.

**Key Features**:
- Determines which geofences the position is inside
- Adds geofence IDs to the position for event generation
- Supports multiple geofence types (circle, polygon, etc.)

**Configuration Options**:

No specific configuration options. The handler uses geofences defined in the database.

### GeocoderHandler

**Purpose**: Performs reverse geocoding to add human-readable address information to positions.

**Key Features**:
- Converts coordinates to street addresses
- Supports multiple geocoding providers
- Implements caching to reduce external API calls
- Handles asynchronous geocoding operations

**Configuration Options**:

| Parameter | Description | Default |
|-----------|-------------|--------|
| `geocoder.ignore.positions` | Skip geocoding for positions | false |
| `geocoder.process.invalid.positions` | Process invalid positions | false |
| `geocoder.reuse.distance` | Distance in meters to reuse previous geocoding result | 0 |

## Implementing Custom Handlers

You can extend the position processing pipeline by implementing custom handlers. Follow these steps to create a new handler:

1. Create a new class that extends `BasePositionHandler`
2. Implement the `onPosition` method to process position data
3. Call the callback when processing is complete
4. Register your handler in the handler chain

### Example Custom Handler

```java
public class MyCustomHandler extends BasePositionHandler {

    private final Config config;

    @Inject
    public MyCustomHandler(Config config) {
        this.config = config;
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        // Process the position
        position.set("customAttribute", "customValue");
        
        // Continue the handler chain
        callback.processed(false);
        
        // Or filter the position
        // callback.processed(true);
    }
}
```

## Handler Chain Configuration

The handler chain is configured using dependency injection. The order of handlers is defined in the service configuration.

### Example Configuration

```yaml
# Position service configuration
handlers:
  - org.traccar.handler.ComputedAttributesHandler$Early
  - org.traccar.handler.FilterHandler
  - org.traccar.handler.DistanceHandler
  - org.traccar.handler.GeofenceHandler
  - org.traccar.handler.GeocoderHandler
  - org.traccar.handler.ComputedAttributesHandler$Late
  - com.example.MyCustomHandler  # Custom handler
```

## Best Practices

1. **Handler Ordering**: Place filtering handlers early in the chain to avoid unnecessary processing
2. **Performance Considerations**: Expensive operations (like geocoding) should only be performed on valid positions
3. **Error Handling**: Handlers should catch and handle their own exceptions to prevent pipeline disruption
4. **Stateless Design**: Handlers should be stateless where possible to support horizontal scaling
5. **Configuration**: Make handler behavior configurable through the configuration system
6. **Logging**: Use appropriate logging levels for debugging and monitoring

## Conclusion

The position processing pipeline provides a flexible and extensible architecture for processing GPS position data. By understanding the handler chain and the role of each handler, you can effectively customize and extend the position processing capabilities of the system.

For more information on implementing specific handlers or extending the pipeline, refer to the Position Service implementation documentation.