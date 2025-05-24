package org.traccar.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the RuleParser class which is responsible for parsing rule definitions
 * from various configuration formats (JSON, YAML) into rule objects that can be evaluated
 * by the Event Service's rule engine.
 */
@ExtendWith(MockitoExtension.class)
public class RuleParserTest {

    @Mock
    private RuleConfigProvider configProvider;

    private RuleParser ruleParser;

    @BeforeEach
    public void setUp() {
        ruleParser = new RuleParser(configProvider);
    }

    /**
     * Tests parsing of a simple rule with a basic condition and a single action.
     */
    @Test
    public void testParseSimpleRule() throws IOException {
        // Arrange
        String jsonConfig = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"speed-limit\","
                + "    \"name\": \"Speed Limit Rule\","
                + "    \"condition\": \"speed > 100\","
                + "    \"priority\": 1,"
                + "    \"enabled\": true,"
                + "    \"actions\": [{"
                + "      \"type\": \"GENERATE_EVENT\","
                + "      \"parameters\": {"
                + "        \"eventType\": \"overspeed\","
                + "        \"attributes\": \"{\\\"speed\\\":100}\""
                + "      }"
                + "    }]"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(jsonConfig.getBytes(StandardCharsets.UTF_8)));

        // Act
        List<Rule> rules = ruleParser.parseRules();

        // Assert
        assertEquals(1, rules.size(), "Should parse one rule");
        
        Rule rule = rules.get(0);
        assertEquals("speed-limit", rule.getId(), "Rule ID should match");
        assertEquals("Speed Limit Rule", rule.getName(), "Rule name should match");
        assertEquals("speed > 100", rule.getCondition(), "Rule condition should match");
        assertEquals(1, rule.getPriority(), "Rule priority should match");
        assertTrue(rule.isEnabled(), "Rule should be enabled");
        
        assertEquals(1, rule.getActions().size(), "Rule should have one action");
        Action action = rule.getActions().get(0);
        assertEquals(ActionType.GENERATE_EVENT, action.getType(), "Action type should match");
        assertEquals("overspeed", action.getParameter("eventType"), "Action parameter should match");
        assertEquals("{\"speed\":100}", action.getParameter("attributes"), "Action parameter should match");
    }

    /**
     * Tests parsing of a rule with a compound condition using logical operators (AND, OR, NOT).
     */
    @Test
    public void testParseRuleWithCompoundCondition() throws IOException {
        // Arrange
        String jsonConfig = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"compound-condition\","
                + "    \"name\": \"Compound Condition Rule\","
                + "    \"condition\": \"speed > 100 && ignition == true && !(geofence:contains(1) || batteryLevel < 20)\","
                + "    \"priority\": 2,"
                + "    \"enabled\": true,"
                + "    \"actions\": [{"
                + "      \"type\": \"SEND_NOTIFICATION\","
                + "      \"parameters\": {"
                + "        \"notificationType\": \"speeding\","
                + "        \"recipients\": \"admin,owner\""
                + "      }"
                + "    }]"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(jsonConfig.getBytes(StandardCharsets.UTF_8)));

        // Act
        List<Rule> rules = ruleParser.parseRules();

        // Assert
        assertEquals(1, rules.size(), "Should parse one rule");
        
        Rule rule = rules.get(0);
        assertEquals("compound-condition", rule.getId(), "Rule ID should match");
        assertEquals("Compound Condition Rule", rule.getName(), "Rule name should match");
        assertEquals("speed > 100 && ignition == true && !(geofence:contains(1) || batteryLevel < 20)", 
                rule.getCondition(), "Rule condition should match");
        assertEquals(2, rule.getPriority(), "Rule priority should match");
        
        assertEquals(1, rule.getActions().size(), "Rule should have one action");
        Action action = rule.getActions().get(0);
        assertEquals(ActionType.SEND_NOTIFICATION, action.getType(), "Action type should match");
        assertEquals("speeding", action.getParameter("notificationType"), "Action parameter should match");
        assertEquals("admin,owner", action.getParameter("recipients"), "Action parameter should match");
    }

    /**
     * Tests parsing of a rule with a complex condition using functions and expressions.
     */
    @Test
    public void testParseRuleWithComplexCondition() throws IOException {
        // Arrange
        String jsonConfig = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"complex-condition\","
                + "    \"name\": \"Complex Condition Rule\","
                + "    \"condition\": \"time:hours(fixTime) >= 9 && time:hours(fixTime) <= 17 && math:abs(speed - maxSpeed) > 10 && fuel / fuelCapacity * 100 < 20\","
                + "    \"priority\": 3,"
                + "    \"enabled\": true,"
                + "    \"actions\": [{"
                + "      \"type\": \"EXECUTE_COMMAND\","
                + "      \"parameters\": {"
                + "        \"commandType\": \"engineStop\","
                + "        \"attributes\": \"{\\\"reason\\\":\\\"speeding\\\"}\""
                + "      }"
                + "    }]"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(jsonConfig.getBytes(StandardCharsets.UTF_8)));

        // Act
        List<Rule> rules = ruleParser.parseRules();

        // Assert
        assertEquals(1, rules.size(), "Should parse one rule");
        
        Rule rule = rules.get(0);
        assertEquals("complex-condition", rule.getId(), "Rule ID should match");
        assertEquals("Complex Condition Rule", rule.getName(), "Rule name should match");
        assertEquals("time:hours(fixTime) >= 9 && time:hours(fixTime) <= 17 && math:abs(speed - maxSpeed) > 10 && fuel / fuelCapacity * 100 < 20", 
                rule.getCondition(), "Rule condition should match");
        assertEquals(3, rule.getPriority(), "Rule priority should match");
        
        assertEquals(1, rule.getActions().size(), "Rule should have one action");
        Action action = rule.getActions().get(0);
        assertEquals(ActionType.EXECUTE_COMMAND, action.getType(), "Action type should match");
        assertEquals("engineStop", action.getParameter("commandType"), "Action parameter should match");
        assertEquals("{\"reason\":\"speeding\"}", action.getParameter("attributes"), "Action parameter should match");
    }

    /**
     * Tests parsing of a rule with multiple actions of different types.
     */
    @Test
    public void testParseRuleWithMultipleActions() throws IOException {
        // Arrange
        String jsonConfig = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"multiple-actions\","
                + "    \"name\": \"Multiple Actions Rule\","
                + "    \"condition\": \"speed > 120\","
                + "    \"priority\": 1,"
                + "    \"enabled\": true,"
                + "    \"actions\": ["
                + "      {"
                + "        \"type\": \"GENERATE_EVENT\","
                + "        \"parameters\": {"
                + "          \"eventType\": \"overspeed\","
                + "          \"attributes\": \"{\\\"speed\\\":120}\""
                + "        },"
                + "        \"priority\": 1"
                + "      },"
                + "      {"
                + "        \"type\": \"SEND_NOTIFICATION\","
                + "        \"parameters\": {"
                + "          \"notificationType\": \"speeding\","
                + "          \"recipients\": \"admin\""
                + "        },"
                + "        \"priority\": 2"
                + "      },"
                + "      {"
                + "        \"type\": \"UPDATE_DEVICE_STATE\","
                + "        \"parameters\": {"
                + "          \"attribute\": \"status\","
                + "          \"value\": \"speeding\""
                + "        },"
                + "        \"priority\": 3"
                + "      }"
                + "    ]"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(jsonConfig.getBytes(StandardCharsets.UTF_8)));

        // Act
        List<Rule> rules = ruleParser.parseRules();

        // Assert
        assertEquals(1, rules.size(), "Should parse one rule");
        
        Rule rule = rules.get(0);
        assertEquals("multiple-actions", rule.getId(), "Rule ID should match");
        assertEquals("Multiple Actions Rule", rule.getName(), "Rule name should match");
        assertEquals("speed > 120", rule.getCondition(), "Rule condition should match");
        
        assertEquals(3, rule.getActions().size(), "Rule should have three actions");
        
        // First action
        Action action1 = rule.getActions().get(0);
        assertEquals(ActionType.GENERATE_EVENT, action1.getType(), "Action type should match");
        assertEquals("overspeed", action1.getParameter("eventType"), "Action parameter should match");
        assertEquals(1, action1.getPriority(), "Action priority should match");
        
        // Second action
        Action action2 = rule.getActions().get(1);
        assertEquals(ActionType.SEND_NOTIFICATION, action2.getType(), "Action type should match");
        assertEquals("speeding", action2.getParameter("notificationType"), "Action parameter should match");
        assertEquals(2, action2.getPriority(), "Action priority should match");
        
        // Third action
        Action action3 = rule.getActions().get(2);
        assertEquals(ActionType.UPDATE_DEVICE_STATE, action3.getType(), "Action type should match");
        assertEquals("status", action3.getParameter("attribute"), "Action parameter should match");
        assertEquals("speeding", action3.getParameter("value"), "Action parameter should match");
        assertEquals(3, action3.getPriority(), "Action priority should match");
    }

    /**
     * Tests parsing of multiple rules from a single configuration.
     */
    @Test
    public void testParseMultipleRules() throws IOException {
        // Arrange
        String jsonConfig = "{"
                + "  \"rules\": ["
                + "    {"
                + "      \"id\": \"rule1\","
                + "      \"name\": \"Rule 1\","
                + "      \"condition\": \"speed > 100\","
                + "      \"priority\": 1,"
                + "      \"enabled\": true,"
                + "      \"actions\": [{"
                + "        \"type\": \"GENERATE_EVENT\","
                + "        \"parameters\": {"
                + "          \"eventType\": \"overspeed\""
                + "        }"
                + "      }]"
                + "    },"
                + "    {"
                + "      \"id\": \"rule2\","
                + "      \"name\": \"Rule 2\","
                + "      \"condition\": \"ignition == false\","
                + "      \"priority\": 2,"
                + "      \"enabled\": true,"
                + "      \"actions\": [{"
                + "        \"type\": \"GENERATE_EVENT\","
                + "        \"parameters\": {"
                + "          \"eventType\": \"ignitionOff\""
                + "        }"
                + "      }]"
                + "    },"
                + "    {"
                + "      \"id\": \"rule3\","
                + "      \"name\": \"Rule 3\","
                + "      \"condition\": \"batteryLevel < 20\","
                + "      \"priority\": 3,"
                + "      \"enabled\": false,"
                + "      \"actions\": [{"
                + "        \"type\": \"GENERATE_EVENT\","
                + "        \"parameters\": {"
                + "          \"eventType\": \"lowBattery\""
                + "        }"
                + "      }]"
                + "    }"
                + "  ]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(jsonConfig.getBytes(StandardCharsets.UTF_8)));

        // Act
        List<Rule> rules = ruleParser.parseRules();

        // Assert
        assertEquals(3, rules.size(), "Should parse three rules");
        
        // Rule 1
        Rule rule1 = rules.get(0);
        assertEquals("rule1", rule1.getId(), "Rule ID should match");
        assertEquals("Rule 1", rule1.getName(), "Rule name should match");
        assertEquals("speed > 100", rule1.getCondition(), "Rule condition should match");
        assertEquals(1, rule1.getPriority(), "Rule priority should match");
        assertTrue(rule1.isEnabled(), "Rule should be enabled");
        
        // Rule 2
        Rule rule2 = rules.get(1);
        assertEquals("rule2", rule2.getId(), "Rule ID should match");
        assertEquals("Rule 2", rule2.getName(), "Rule name should match");
        assertEquals("ignition == false", rule2.getCondition(), "Rule condition should match");
        assertEquals(2, rule2.getPriority(), "Rule priority should match");
        assertTrue(rule2.isEnabled(), "Rule should be enabled");
        
        // Rule 3
        Rule rule3 = rules.get(2);
        assertEquals("rule3", rule3.getId(), "Rule ID should match");
        assertEquals("Rule 3", rule3.getName(), "Rule name should match");
        assertEquals("batteryLevel < 20", rule3.getCondition(), "Rule condition should match");
        assertEquals(3, rule3.getPriority(), "Rule priority should match");
        assertFalse(rule3.isEnabled(), "Rule should be disabled");
    }

    /**
     * Tests error handling for malformed JSON in rule configuration.
     */
    @Test
    public void testMalformedJsonConfiguration() throws IOException {
        // Arrange
        String malformedJson = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"invalid-json\","
                + "    \"name\": \"Invalid JSON Rule\","
                + "    \"condition\": \"speed > 100\","
                + "    \"actions\": [{"
                + "      \"type\": \"GENERATE_EVENT\","
                + "      \"parameters\": {"
                + "        \"eventType\": \"overspeed\""
                + "      }"
                + "    }]"
                + "  }" // Missing closing bracket
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(malformedJson.getBytes(StandardCharsets.UTF_8)));

        // Act & Assert
        Exception exception = assertThrows(RuleParsingException.class, () -> {
            ruleParser.parseRules();
        }, "Should throw RuleParsingException for malformed JSON");
        
        assertTrue(exception.getMessage().contains("Error parsing rule configuration"), 
                "Exception message should indicate parsing error");
    }

    /**
     * Tests error handling for missing required rule properties.
     */
    @Test
    public void testMissingRequiredRuleProperties() throws IOException {
        // Arrange - Missing condition
        String missingCondition = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"missing-condition\","
                + "    \"name\": \"Missing Condition Rule\","
                + "    \"priority\": 1,"
                + "    \"enabled\": true,"
                + "    \"actions\": [{"
                + "      \"type\": \"GENERATE_EVENT\","
                + "      \"parameters\": {"
                + "        \"eventType\": \"overspeed\""
                + "      }"
                + "    }]"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(missingCondition.getBytes(StandardCharsets.UTF_8)));

        // Act & Assert
        Exception exception = assertThrows(RuleValidationException.class, () -> {
            ruleParser.parseRules();
        }, "Should throw RuleValidationException for missing condition");
        
        assertTrue(exception.getMessage().contains("Missing required property: condition"), 
                "Exception message should indicate missing condition");

        // Arrange - Missing actions
        String missingActions = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"missing-actions\","
                + "    \"name\": \"Missing Actions Rule\","
                + "    \"condition\": \"speed > 100\","
                + "    \"priority\": 1,"
                + "    \"enabled\": true"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(missingActions.getBytes(StandardCharsets.UTF_8)));

        // Act & Assert
        exception = assertThrows(RuleValidationException.class, () -> {
            ruleParser.parseRules();
        }, "Should throw RuleValidationException for missing actions");
        
        assertTrue(exception.getMessage().contains("Missing required property: actions"), 
                "Exception message should indicate missing actions");
    }

    /**
     * Tests error handling for invalid action types in rule configuration.
     */
    @Test
    public void testInvalidActionType() throws IOException {
        // Arrange
        String invalidActionType = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"invalid-action\","
                + "    \"name\": \"Invalid Action Rule\","
                + "    \"condition\": \"speed > 100\","
                + "    \"priority\": 1,"
                + "    \"enabled\": true,"
                + "    \"actions\": [{"
                + "      \"type\": \"INVALID_ACTION_TYPE\","
                + "      \"parameters\": {"
                + "        \"param1\": \"value1\""
                + "      }"
                + "    }]"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(invalidActionType.getBytes(StandardCharsets.UTF_8)));

        // Act & Assert
        Exception exception = assertThrows(RuleValidationException.class, () -> {
            ruleParser.parseRules();
        }, "Should throw RuleValidationException for invalid action type");
        
        assertTrue(exception.getMessage().contains("Invalid action type: INVALID_ACTION_TYPE"), 
                "Exception message should indicate invalid action type");
    }

    /**
     * Tests error handling for missing required action parameters.
     */
    @Test
    public void testMissingRequiredActionParameters() throws IOException {
        // Arrange - Missing eventType for GENERATE_EVENT action
        String missingEventType = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"missing-event-type\","
                + "    \"name\": \"Missing Event Type Rule\","
                + "    \"condition\": \"speed > 100\","
                + "    \"priority\": 1,"
                + "    \"enabled\": true,"
                + "    \"actions\": [{"
                + "      \"type\": \"GENERATE_EVENT\","
                + "      \"parameters\": {"
                + "        \"attributes\": \"{\\\"speed\\\":100}\""
                + "      }"
                + "    }]"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(missingEventType.getBytes(StandardCharsets.UTF_8)));

        // Act & Assert
        Exception exception = assertThrows(RuleValidationException.class, () -> {
            ruleParser.parseRules();
        }, "Should throw RuleValidationException for missing eventType");
        
        assertTrue(exception.getMessage().contains("Missing required parameter: eventType"), 
                "Exception message should indicate missing eventType");

        // Arrange - Missing notificationType for SEND_NOTIFICATION action
        String missingNotificationType = "{"
                + "  \"rules\": [{"
                + "    \"id\": \"missing-notification-type\","
                + "    \"name\": \"Missing Notification Type Rule\","
                + "    \"condition\": \"speed > 100\","
                + "    \"priority\": 1,"
                + "    \"enabled\": true,"
                + "    \"actions\": [{"
                + "      \"type\": \"SEND_NOTIFICATION\","
                + "      \"parameters\": {"
                + "        \"recipients\": \"admin\""
                + "      }"
                + "    }]"
                + "  }]"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(missingNotificationType.getBytes(StandardCharsets.UTF_8)));

        // Act & Assert
        exception = assertThrows(RuleValidationException.class, () -> {
            ruleParser.parseRules();
        }, "Should throw RuleValidationException for missing notificationType");
        
        assertTrue(exception.getMessage().contains("Missing required parameter: notificationType"), 
                "Exception message should indicate missing notificationType");
    }

    /**
     * Tests parsing of rules from YAML format.
     */
    @Test
    public void testParseRulesFromYaml() throws IOException {
        // Arrange
        String yamlConfig = "---\n"
                + "rules:\n"
                + "- id: yaml-rule\n"
                + "  name: YAML Rule\n"
                + "  condition: speed > 100\n"
                + "  priority: 1\n"
                + "  enabled: true\n"
                + "  actions:\n"
                + "  - type: GENERATE_EVENT\n"
                + "    parameters:\n"
                + "      eventType: overspeed\n"
                + "      attributes: '{\"speed\":100}'\n";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(yamlConfig.getBytes(StandardCharsets.UTF_8)));

        // Act
        List<Rule> rules = ruleParser.parseRules();

        // Assert
        assertEquals(1, rules.size(), "Should parse one rule");
        
        Rule rule = rules.get(0);
        assertEquals("yaml-rule", rule.getId(), "Rule ID should match");
        assertEquals("YAML Rule", rule.getName(), "Rule name should match");
        assertEquals("speed > 100", rule.getCondition(), "Rule condition should match");
        assertEquals(1, rule.getPriority(), "Rule priority should match");
        assertTrue(rule.isEnabled(), "Rule should be enabled");
        
        assertEquals(1, rule.getActions().size(), "Rule should have one action");
        Action action = rule.getActions().get(0);
        assertEquals(ActionType.GENERATE_EVENT, action.getType(), "Action type should match");
        assertEquals("overspeed", action.getParameter("eventType"), "Action parameter should match");
        assertEquals("{\"speed\":100}", action.getParameter("attributes"), "Action parameter should match");
    }

    /**
     * Tests handling of empty rule configuration.
     */
    @Test
    public void testEmptyRuleConfiguration() throws IOException {
        // Arrange
        String emptyConfig = "{"
                + "  \"rules\": []"
                + "}";

        when(configProvider.getRuleConfiguration()).thenReturn(
                new ByteArrayInputStream(emptyConfig.getBytes(StandardCharsets.UTF_8)));

        // Act
        List<Rule> rules = ruleParser.parseRules();

        // Assert
        assertTrue(rules.isEmpty(), "Should return empty list for empty rules configuration");
    }

    /**
     * Tests handling of null rule configuration.
     */
    @Test
    public void testNullRuleConfiguration() throws IOException {
        // Arrange
        when(configProvider.getRuleConfiguration()).thenReturn(null);

        // Act & Assert
        Exception exception = assertThrows(RuleParsingException.class, () -> {
            ruleParser.parseRules();
        }, "Should throw RuleParsingException for null configuration");
        
        assertTrue(exception.getMessage().contains("Rule configuration not found"), 
                "Exception message should indicate missing configuration");
    }

    /**
     * Tests handling of IO exceptions during rule parsing.
     */
    @Test
    public void testIOExceptionHandling() throws IOException {
        // Arrange
        when(configProvider.getRuleConfiguration()).thenThrow(new IOException("Simulated IO error"));

        // Act & Assert
        Exception exception = assertThrows(RuleParsingException.class, () -> {
            ruleParser.parseRules();
        }, "Should throw RuleParsingException for IO errors");
        
        assertTrue(exception.getMessage().contains("Error reading rule configuration"), 
                "Exception message should indicate IO error");
    }
}