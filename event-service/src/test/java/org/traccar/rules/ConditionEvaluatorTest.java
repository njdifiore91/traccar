package org.traccar.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.traccar.model.Position;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConditionEvaluatorTest {

    private ConditionEvaluator conditionEvaluator;
    private Position position;
    private Map<String, Object> deviceAttributes;

    @BeforeEach
    public void setUp() {
        conditionEvaluator = new ConditionEvaluator();
        position = new Position();
        position.setSpeed(50);
        position.setValid(true);
        position.set("ignition", true);
        position.set("fuel", 75.5);
        position.set("batteryLevel", 80);
        position.set("alarm", "temperature");
        position.set("temperature", 28.5);
        position.set("driver", "John Doe");
        position.set("event", 42);
        position.set("result", "success");
        
        // Set up device attributes
        deviceAttributes = new HashMap<>();
        deviceAttributes.put("maxSpeed", 100);
        deviceAttributes.put("minBatteryLevel", 20);
        deviceAttributes.put("fuelCapacity", 100.0);
        deviceAttributes.put("maintenance", false);
    }

    @Test
    public void testSimpleComparisonConditions() {
        // Equals operator
        assertTrue(conditionEvaluator.evaluate("speed == 50", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed == 60", position, deviceAttributes));
        
        // String equals
        assertTrue(conditionEvaluator.evaluate("alarm == 'temperature'", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("alarm == 'crash'", position, deviceAttributes));
        
        // Greater than
        assertTrue(conditionEvaluator.evaluate("speed > 40", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed > 50", position, deviceAttributes));
        
        // Less than
        assertTrue(conditionEvaluator.evaluate("speed < 60", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed < 50", position, deviceAttributes));
        
        // Greater than or equal
        assertTrue(conditionEvaluator.evaluate("speed >= 50", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("speed >= 40", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed >= 60", position, deviceAttributes));
        
        // Less than or equal
        assertTrue(conditionEvaluator.evaluate("speed <= 50", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("speed <= 60", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed <= 40", position, deviceAttributes));
        
        // Not equals
        assertTrue(conditionEvaluator.evaluate("speed != 60", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed != 50", position, deviceAttributes));
    }

    @Test
    public void testCompoundConditionsWithLogicalOperators() {
        // AND operator
        assertTrue(conditionEvaluator.evaluate("speed > 40 && ignition == true", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed > 60 && ignition == true", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed > 40 && ignition == false", position, deviceAttributes));
        
        // OR operator
        assertTrue(conditionEvaluator.evaluate("speed > 60 || ignition == true", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("speed > 40 || ignition == false", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed > 60 || ignition == false", position, deviceAttributes));
        
        // NOT operator
        assertTrue(conditionEvaluator.evaluate("!(speed > 60)", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("!(speed > 40)", position, deviceAttributes));
        
        // Complex combinations
        assertTrue(conditionEvaluator.evaluate("(speed > 40 && ignition == true) || batteryLevel > 70", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("!(speed > 60) && (ignition == true || batteryLevel < 30)", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("(speed > 60 && ignition == true) || batteryLevel < 70", position, deviceAttributes));
    }

    @Test
    public void testConditionsWithMathematicalExpressions() {
        // Simple arithmetic
        assertTrue(conditionEvaluator.evaluate("speed + 10 > 55", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed + 10 > 65", position, deviceAttributes));
        
        // Multiplication and division
        assertTrue(conditionEvaluator.evaluate("speed * 2 == 100", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("speed / 2 == 25", position, deviceAttributes));
        
        // Complex expressions
        assertTrue(conditionEvaluator.evaluate("(speed * 2 - 10) / 2 > 40", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("(speed * 2 - 10) / 2 > 50", position, deviceAttributes));
        
        // Modulo operation
        assertTrue(conditionEvaluator.evaluate("speed % 10 == 0", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed % 30 == 0", position, deviceAttributes));
        
        // Expressions with multiple attributes
        assertTrue(conditionEvaluator.evaluate("speed + batteryLevel > 100", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("fuel / fuelCapacity * 100 > 70", position, deviceAttributes));
    }

    @Test
    public void testConditionsWithPositionAttributesAndDeviceProperties() {
        // Position attributes
        assertTrue(conditionEvaluator.evaluate("temperature > 25", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("driver == 'John Doe'", position, deviceAttributes));
        
        // Device properties
        assertTrue(conditionEvaluator.evaluate("maxSpeed > speed", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("batteryLevel > minBatteryLevel", position, deviceAttributes));
        
        // Combining position and device attributes
        assertTrue(conditionEvaluator.evaluate("speed < maxSpeed && batteryLevel > minBatteryLevel", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("fuel / fuelCapacity * 100 > 70 && !maintenance", position, deviceAttributes));
        
        // Boolean attributes
        assertTrue(conditionEvaluator.evaluate("ignition && !maintenance", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("!ignition && maintenance", position, deviceAttributes));
        
        // Ternary operator
        assertTrue(conditionEvaluator.evaluate("event == 42 ? true : false", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("speed > 60 ? false : true", position, deviceAttributes));
    }

    @Test
    public void testGeofenceRelatedConditions() throws ParseException {
        // Set up position with location
        position.setLatitude(40.7128);
        position.setLongitude(-74.0060);
        
        // Mock geofence data in attributes
        position.set("geofenceIds", new long[] {1, 2, 3});
        deviceAttributes.put("homeGeofence", 1L);
        deviceAttributes.put("workGeofence", 5L);
        
        // Test geofence containment
        assertTrue(conditionEvaluator.evaluate("geofence:contains(1)", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("geofence:contains(homeGeofence)", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("geofence:contains(workGeofence)", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("geofence:contains(10)", position, deviceAttributes));
        
        // Test geofence with logical operators
        assertTrue(conditionEvaluator.evaluate("geofence:contains(1) && speed < 60", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("geofence:contains(5) || speed > 60", position, deviceAttributes));
        
        // Test multiple geofences
        assertTrue(conditionEvaluator.evaluate("geofence:contains(1) || geofence:contains(2)", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("geofence:contains(1) && geofence:contains(2)", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("geofence:contains(5) && geofence:contains(6)", position, deviceAttributes));
    }

    @Test
    public void testTimeBasedFunctionsAndComparisons() throws ParseException {
        // Set up position with time
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        Date date = dateFormat.parse("2023-06-15 14:30:00");
        position.setTime(date);
        
        // Set up device time attributes
        deviceAttributes.put("lastMaintenance", dateFormat.parse("2023-05-15 10:00:00"));
        deviceAttributes.put("nextService", dateFormat.parse("2023-07-15 10:00:00"));
        
        // Test time functions
        assertTrue(conditionEvaluator.evaluate("time:hours(fixTime) == 14", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("time:minutes(fixTime) == 30", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("time:day(fixTime) == 15", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("time:month(fixTime) == 6", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("time:year(fixTime) == 2023", position, deviceAttributes));
        
        // Test time comparisons
        assertTrue(conditionEvaluator.evaluate("fixTime > lastMaintenance", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("fixTime < nextService", position, deviceAttributes));
        
        // Test time difference calculations
        assertTrue(conditionEvaluator.evaluate("time:diffDays(fixTime, lastMaintenance) > 25", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("time:diffHours(nextService, fixTime) > 500", position, deviceAttributes));
        
        // Test time-based conditions with logical operators
        assertTrue(conditionEvaluator.evaluate("time:hours(fixTime) >= 9 && time:hours(fixTime) <= 17", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("time:day(fixTime) == 15 && speed < 60", position, deviceAttributes));
    }

    @Test
    public void testHandlingOfNullOrMissingAttributes() {
        // Test with null values
        position.set("temperature", null);
        assertFalse(conditionEvaluator.evaluate("temperature > 25", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("temperature == null", position, deviceAttributes));
        
        // Test with missing attributes
        assertFalse(conditionEvaluator.evaluate("nonExistentAttribute == 'value'", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("nonExistentAttribute == null", position, deviceAttributes));
        
        // Test null-safe operations
        assertTrue(conditionEvaluator.evaluate("temperature == null ? true : false", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("nonExistentAttribute == null ? speed > 40 : false", position, deviceAttributes));
        
        // Test with default values
        assertTrue(conditionEvaluator.evaluate("(temperature != null ? temperature : 0) < 25", position, deviceAttributes));
        assertTrue(conditionEvaluator.evaluate("(nonExistentAttribute != null ? nonExistentAttribute : 100) == 100", position, deviceAttributes));
        
        // Test with logical operators and null values
        assertTrue(conditionEvaluator.evaluate("speed > 40 && (temperature == null || temperature > 25)", position, deviceAttributes));
        assertFalse(conditionEvaluator.evaluate("speed < 40 || (temperature != null && temperature < 0)", position, deviceAttributes));
    }
}