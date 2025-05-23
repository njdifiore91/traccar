# Speed Limit Mock Resources

This directory contains mock resources for testing the speed limit functionality in the Traccar Protocol Service without requiring actual API calls to external services like Overpass API.

## Overview

The speed limit functionality in Traccar allows the system to retrieve speed limit information for specific geographic coordinates. In production, this is typically done using the `OverpassSpeedLimitProvider` which makes HTTP requests to the Overpass API. For testing purposes, these mock resources allow developers to simulate various responses and scenarios without making actual API calls.

## Mock Files

### 1. mock_provider_responses.json

This file maps specific latitude/longitude coordinates to predefined speed limit responses or error conditions.

**Purpose**: Enables deterministic testing by returning consistent results for specific locations.

**Structure**:
```json
{
  "coordinates": [
    {
      "latitude": 34.74767,
      "longitude": -82.48098,
      "speedLimit": 52.1,
      "unit": "kph"
    },
    {
      "latitude": 40.7128,
      "longitude": -74.0060,
      "speedLimit": 25.0,
      "unit": "mph"
    },
    {
      "latitude": 0.0,
      "longitude": 0.0,
      "error": "Not found"
    }
  ]
}
```

**Usage**: When testing with these coordinates, the mock provider will return the corresponding speed limit or error.

### 2. geographic_areas.json

Defines different geographic areas with their associated speed limits for various road types.

**Purpose**: Allows testing of location-based speed limit functionality across different geographic contexts.

**Structure**:
```json
{
  "areas": [
    {
      "name": "Urban",
      "center": {
        "latitude": 34.85,
        "longitude": -82.4
      },
      "radius": 5000,
      "speedLimits": {
        "residential": 30,
        "primary": 50,
        "secondary": 40,
        "highway": 80
      }
    },
    {
      "name": "School Zone",
      "center": {
        "latitude": 34.86,
        "longitude": -82.41
      },
      "radius": 500,
      "speedLimits": {
        "residential": 20,
        "primary": 30,
        "secondary": 30,
        "highway": null
      }
    }
  ]
}
```

**Usage**: Used for testing speed limit determination based on geographic context and road type.

### 3. speed_limit_config.json

Configuration file for the mock speed limit service.

**Purpose**: Allows tests to configure the behavior of the speed limit provider without modifying code.

**Structure**:
```json
{
  "provider": "mock",
  "apiUrl": "http://mock-server/api",
  "accuracy": 100,
  "timeout": 5000,
  "cacheSize": 1000,
  "cacheTtl": 86400,
  "mockBehavior": {
    "delayMs": 50,
    "errorRate": 0.1,
    "defaultLimit": 50
  }
}
```

**Usage**: Configure the mock provider's behavior for different test scenarios.

### 4. overpass_response.json

Mock response from the Overpass API containing speed limit data.

**Purpose**: Simulates the JSON response structure returned by the Overpass API.

**Structure**:
```json
{
  "version": 0.6,
  "generator": "Overpass API",
  "elements": [
    {
      "type": "way",
      "id": 123456789,
      "tags": {
        "highway": "residential",
        "maxspeed": "50"
      }
    },
    {
      "type": "way",
      "id": 987654321,
      "tags": {
        "highway": "primary",
        "maxspeed": "35 mph"
      }
    },
    {
      "type": "way",
      "id": 555555555,
      "tags": {
        "highway": "motorway",
        "maxspeed": "25 knots"
      }
    }
  ]
}
```

**Usage**: Used to test the parsing logic in `OverpassSpeedLimitProvider` for different speed limit formats (numeric, mph, knots).

## Using Mock Resources in Tests

### Example: Creating a Mock Speed Limit Provider

```java
public class MockSpeedLimitProvider implements SpeedLimitProvider {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Map<String, Object> mockResponses = new HashMap<>();
    
    public MockSpeedLimitProvider() throws IOException {
        // Load mock responses from JSON file
        try (InputStream is = getClass().getResourceAsStream("/mocks/speedlimit/mock_provider_responses.json")) {
            JsonNode root = mapper.readTree(is);
            JsonNode coordinates = root.get("coordinates");
            
            for (JsonNode coord : coordinates) {
                String key = coord.get("latitude").asDouble() + "," + coord.get("longitude").asDouble();
                if (coord.has("error")) {
                    mockResponses.put(key, coord.get("error").asText());
                } else {
                    double speedLimit = coord.get("speedLimit").asDouble();
                    String unit = coord.get("unit").asText();
                    
                    // Convert to knots if necessary
                    if ("mph".equals(unit)) {
                        speedLimit = UnitsConverter.knotsFromMph(speedLimit);
                    } else if ("kph".equals(unit)) {
                        speedLimit = UnitsConverter.knotsFromKph(speedLimit);
                    }
                    
                    mockResponses.put(key, speedLimit);
                }
            }
        }
    }
    
    @Override
    public void getSpeedLimit(double latitude, double longitude, SpeedLimitProviderCallback callback) {
        String key = latitude + "," + longitude;
        Object response = mockResponses.getOrDefault(key, "Not found");
        
        // Simulate async behavior
        new Thread(() -> {
            try {
                // Add a small delay to simulate network latency
                Thread.sleep(50);
                
                if (response instanceof String) {
                    callback.onFailure(new SpeedLimitException((String) response));
                } else if (response instanceof Double) {
                    callback.onSuccess((Double) response);
                }
            } catch (InterruptedException e) {
                callback.onFailure(e);
            }
        }).start();
    }
}
```

### Example: Testing Overspeed Event Handler with Mock Provider

```java
@Test
public void testOverspeedEventWithMockSpeedLimit() throws Exception {
    // Setup mock provider
    SpeedLimitProvider mockProvider = new MockSpeedLimitProvider();
    
    // Create handler with mock provider
    OverspeedEventHandler handler = new OverspeedEventHandler(mockProvider);
    
    // Create test position at coordinates with known mock speed limit
    Position position = new Position();
    position.setLatitude(34.74767);
    position.setLongitude(-82.48098);
    position.setSpeed(60); // Speed higher than the mock limit (52.1)
    position.setDeviceId(1);
    
    // Test event generation
    CompletableFuture<Event> eventFuture = new CompletableFuture<>();
    handler.analyzePosition(position, eventFuture::complete);
    
    // Wait for async result
    Event event = eventFuture.get(1, TimeUnit.SECONDS);
    
    // Verify event
    assertNotNull(event);
    assertEquals(Event.TYPE_DEVICE_OVERSPEED, event.getType());
    assertEquals(52.1, event.getDouble("speedLimit"), 0.1);
}
```

## Extending Mock Data

To add new mock scenarios, follow these steps:

### Adding New Coordinate Responses

1. Open `mock_provider_responses.json`
2. Add a new entry to the `coordinates` array:
   ```json
   {
     "latitude": 51.5074,
     "longitude": -0.1278,
     "speedLimit": 30.0,
     "unit": "mph"
   }
   ```
   Or for an error case:
   ```json
   {
     "latitude": 51.5074,
     "longitude": -0.1278,
     "error": "Invalid coordinates"
   }
   ```

### Adding New Geographic Areas

1. Open `geographic_areas.json`
2. Add a new area to the `areas` array:
   ```json
   {
     "name": "Highway Zone",
     "center": {
       "latitude": 35.0,
       "longitude": -82.5
     },
     "radius": 2000,
     "speedLimits": {
       "residential": 50,
       "primary": 80,
       "secondary": 65,
       "highway": 120
     }
   }
   ```

### Creating Custom Overpass Responses

1. Create a new file like `overpass_response_custom.json`
2. Model it after the existing `overpass_response.json` but with your specific test case
3. Load it in your test:
   ```java
   try (InputStream is = getClass().getResourceAsStream("/mocks/speedlimit/overpass_response_custom.json")) {
       // Parse and use the custom response
   }
   ```

## Best Practices

1. **Deterministic Testing**: Always use predefined coordinates with known responses for repeatable tests.
2. **Realistic Data**: Use realistic speed limits and geographic areas that match real-world scenarios.
3. **Error Cases**: Include error scenarios to test error handling in your code.
4. **Unit Conversion**: Test with different speed limit units (kph, mph, knots) to verify conversion logic.
5. **Async Testing**: Remember that the SpeedLimitProvider is asynchronous, so use appropriate async testing patterns.

## Related Files

- `src/main/java/org/traccar/speedlimit/SpeedLimitProvider.java` - Interface defining the speed limit provider functionality
- `src/main/java/org/traccar/speedlimit/OverpassSpeedLimitProvider.java` - Implementation using Overpass API
- `src/test/java/org/traccar/speedlimit/OverpassSpeedLimitProviderTest.java` - Integration test for the Overpass provider