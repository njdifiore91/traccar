package org.traccar.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.messaging.MessageProducer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Integration tests for the complete rule evaluation pipeline of the Event Service's rule engine.
 * Tests the end-to-end process from parsing rules to evaluating conditions to executing actions.
 */
@ExtendWith(MockitoExtension.class)
public class RuleEngineTest {

    @Mock
    private RuleParser ruleParser;

    @Mock
    private ConditionEvaluator conditionEvaluator;

    @Mock
    private ActionExecutor actionExecutor;

    @Mock
    private MessageProducer messageProducer;

    private RuleEngine ruleEngine;

    @BeforeEach
    public void setUp() {
        ruleEngine = new RuleEngine(ruleParser, conditionEvaluator, actionExecutor, messageProducer);
    }

    /**
     * Tests the basic rule evaluation pipeline with a single rule and position update.
     * Verifies that the rule engine correctly processes the position data and triggers
     * the appropriate action when the condition is met.
     */
    @Test
    public void testBasicRuleEvaluation() {
        // Create a test rule
        Rule speedRule = new Rule();
        speedRule.setId(1L);
        speedRule.setName("Speed Limit Rule");
        speedRule.setCondition("speed > 100");
        speedRule.setPriority(1);
        speedRule.setEnabled(true);
        
        // Create a test action
        Action notifyAction = new Action();
        notifyAction.setType("notification");
        notifyAction.setParameter("type", "overspeed");
        speedRule.setActions(Arrays.asList(notifyAction));
        
        // Mock rule parser to return our test rule
        when(ruleParser.parseRules()).thenReturn(Arrays.asList(speedRule));
        
        // Create a position that exceeds the speed limit
        Position position = new Position();
        position.setDeviceId(1);
        position.setTime(new Date());
        position.setSpeed(120.0); // Speed in km/h
        
        // Mock condition evaluator to return true for our condition
        when(conditionEvaluator.evaluate(eq("speed > 100"), any(Position.class))).thenReturn(true);
        
        // Mock action executor to return a successful future
        when(actionExecutor.execute(any(Action.class), any(Position.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        
        // Execute rule evaluation
        List<Event> events = new ArrayList<>();
        ruleEngine.evaluateRules(position, events::add);
        
        // Verify that the rule was evaluated and action was executed
        verify(conditionEvaluator).evaluate(eq("speed > 100"), eq(position));
        verify(actionExecutor).execute(eq(notifyAction), eq(position));
        
        // Verify that an event was generated
        assertEquals(1, events.size());
        Event event = events.get(0);
        assertEquals(Event.TYPE_OVERSPEED, event.getType());
        assertEquals(position.getDeviceId(), event.getDeviceId());
    }

    /**
     * Tests rule prioritization to ensure that rules are evaluated in the correct order
     * based on their priority values.
     */
    @Test
    public void testRulePrioritization() {
        // Create test rules with different priorities
        Rule highPriorityRule = new Rule();
        highPriorityRule.setId(1L);
        highPriorityRule.setName("High Priority Rule");
        highPriorityRule.setCondition("speed > 0");
        highPriorityRule.setPriority(1); // Higher priority (lower number)
        highPriorityRule.setEnabled(true);
        
        Action highPriorityAction = new Action();
        highPriorityAction.setType("notification");
        highPriorityAction.setParameter("type", "high_priority");
        highPriorityRule.setActions(Arrays.asList(highPriorityAction));
        
        Rule lowPriorityRule = new Rule();
        lowPriorityRule.setId(2L);
        lowPriorityRule.setName("Low Priority Rule");
        lowPriorityRule.setCondition("speed > 0");
        lowPriorityRule.setPriority(2); // Lower priority (higher number)
        lowPriorityRule.setEnabled(true);
        
        Action lowPriorityAction = new Action();
        lowPriorityAction.setType("notification");
        lowPriorityAction.setParameter("type", "low_priority");
        lowPriorityRule.setActions(Arrays.asList(lowPriorityAction));
        
        // Mock rule parser to return our test rules in reverse priority order
        when(ruleParser.parseRules()).thenReturn(Arrays.asList(lowPriorityRule, highPriorityRule));
        
        // Create a position that satisfies both rules
        Position position = new Position();
        position.setDeviceId(1);
        position.setTime(new Date());
        position.setSpeed(50.0);
        
        // Mock condition evaluator to return true for both conditions
        when(conditionEvaluator.evaluate(eq("speed > 0"), any(Position.class))).thenReturn(true);
        
        // Mock action executor to return a successful future
        when(actionExecutor.execute(any(Action.class), any(Position.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        
        // Execute rule evaluation
        List<Event> events = new ArrayList<>();
        ruleEngine.evaluateRules(position, events::add);
        
        // Verify that actions were executed in priority order (high priority first)
        // We can use inOrder to verify the sequence of calls
        var inOrder = inOrder(actionExecutor);
        inOrder.verify(actionExecutor).execute(eq(highPriorityAction), eq(position));
        inOrder.verify(actionExecutor).execute(eq(lowPriorityAction), eq(position));
    }

    /**
     * Tests stateful rule evaluation across multiple position updates to ensure that
     * the rule engine correctly maintains state for rules that depend on previous positions.
     */
    @Test
    public void testStatefulRuleEvaluation() {
        // Create a stateful rule that triggers when a device stops moving
        Rule motionRule = new Rule();
        motionRule.setId(1L);
        motionRule.setName("Motion State Rule");
        motionRule.setCondition("motion == false && previousMotion == true");
        motionRule.setPriority(1);
        motionRule.setEnabled(true);
        motionRule.setStateful(true);
        
        Action notifyStopAction = new Action();
        notifyStopAction.setType("notification");
        notifyStopAction.setParameter("type", "device_stopped");
        motionRule.setActions(Arrays.asList(notifyStopAction));
        
        // Mock rule parser to return our stateful rule
        when(ruleParser.parseRules()).thenReturn(Arrays.asList(motionRule));
        
        // Create two positions: first with motion, second without motion
        Position position1 = new Position();
        position1.setDeviceId(1);
        position1.setTime(new Date(System.currentTimeMillis() - 60000)); // 1 minute ago
        position1.set(Position.KEY_MOTION, true);
        
        Position position2 = new Position();
        position2.setDeviceId(1);
        position2.setTime(new Date());
        position2.set(Position.KEY_MOTION, false);
        
        // Mock condition evaluator for the first position (should not trigger)
        when(conditionEvaluator.evaluate(eq("motion == false && previousMotion == true"), eq(position1)))
                .thenReturn(false);
        
        // Mock condition evaluator for the second position (should trigger)
        when(conditionEvaluator.evaluate(eq("motion == false && previousMotion == true"), eq(position2)))
                .thenReturn(true);
        
        // Mock action executor to return a successful future
        when(actionExecutor.execute(any(Action.class), any(Position.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        
        // Process the first position (should not trigger the rule)
        List<Event> events1 = new ArrayList<>();
        ruleEngine.evaluateRules(position1, events1::add);
        
        // Verify that no events were generated for the first position
        assertTrue(events1.isEmpty());
        verify(actionExecutor, never()).execute(any(Action.class), eq(position1));
        
        // Process the second position (should trigger the rule)
        List<Event> events2 = new ArrayList<>();
        ruleEngine.evaluateRules(position2, events2::add);
        
        // Verify that an event was generated for the second position
        assertEquals(1, events2.size());
        Event event = events2.get(0);
        assertEquals(Event.TYPE_DEVICE_STOPPED, event.getType());
        assertEquals(position2.getDeviceId(), event.getDeviceId());
        verify(actionExecutor).execute(eq(notifyStopAction), eq(position2));
    }

    /**
     * Tests rule evaluation performance with a large rule set to ensure that the rule engine
     * can efficiently process many rules against position data.
     */
    @Test
    public void testLargeRuleSetPerformance() {
        // Create a large set of rules (100 rules)
        List<Rule> rules = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Rule rule = new Rule();
            rule.setId((long) i);
            rule.setName("Rule " + i);
            rule.setCondition("speed > " + i);
            rule.setPriority(i);
            rule.setEnabled(true);
            
            Action action = new Action();
            action.setType("notification");
            action.setParameter("type", "rule_" + i);
            rule.setActions(Arrays.asList(action));
            
            rules.add(rule);
        }
        
        // Mock rule parser to return our large rule set
        when(ruleParser.parseRules()).thenReturn(rules);
        
        // Create a position that will trigger some rules
        Position position = new Position();
        position.setDeviceId(1);
        position.setTime(new Date());
        position.setSpeed(50.0); // Will trigger rules with speed > 0 to speed > 49
        
        // Mock condition evaluator to return true for rules with threshold < 50
        when(conditionEvaluator.evaluate(contains("speed > "), any(Position.class)))
                .thenAnswer(invocation -> {
                    String condition = invocation.getArgument(0);
                    int threshold = Integer.parseInt(condition.substring("speed > ".length()));
                    return threshold < 50;
                });
        
        // Mock action executor to return a successful future
        when(actionExecutor.execute(any(Action.class), any(Position.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        
        // Execute rule evaluation and measure time
        long startTime = System.currentTimeMillis();
        List<Event> events = new ArrayList<>();
        ruleEngine.evaluateRules(position, events::add);
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify that the expected number of rules were triggered (50 rules)
        assertEquals(50, events.size());
        
        // Verify that the evaluation completed within a reasonable time (e.g., < 1 second)
        // This is a simple performance check; in a real test, you might use a more sophisticated approach
        assertTrue(duration < 1000, "Rule evaluation took too long: " + duration + "ms");
    }

    /**
     * Tests rule conflict resolution to ensure that when multiple rules generate conflicting
     * actions, the rule engine correctly resolves the conflicts based on rule priorities.
     */
    @Test
    public void testRuleConflictResolution() {
        // Create two conflicting rules with different priorities
        Rule highPriorityRule = new Rule();
        highPriorityRule.setId(1L);
        highPriorityRule.setName("High Priority Geofence Rule");
        highPriorityRule.setCondition("geofenceId == 1");
        highPriorityRule.setPriority(1); // Higher priority
        highPriorityRule.setEnabled(true);
        
        Action highPriorityAction = new Action();
        highPriorityAction.setType("command");
        highPriorityAction.setParameter("type", "engineStop");
        highPriorityRule.setActions(Arrays.asList(highPriorityAction));
        
        Rule lowPriorityRule = new Rule();
        lowPriorityRule.setId(2L);
        lowPriorityRule.setName("Low Priority Geofence Rule");
        lowPriorityRule.setCondition("geofenceId == 1");
        lowPriorityRule.setPriority(2); // Lower priority
        lowPriorityRule.setEnabled(true);
        
        Action lowPriorityAction = new Action();
        lowPriorityAction.setType("command");
        lowPriorityAction.setParameter("type", "engineStart");
        lowPriorityRule.setActions(Arrays.asList(lowPriorityAction));
        
        // Mock rule parser to return our conflicting rules
        when(ruleParser.parseRules()).thenReturn(Arrays.asList(lowPriorityRule, highPriorityRule));
        
        // Create a position that triggers both rules
        Position position = new Position();
        position.setDeviceId(1);
        position.setTime(new Date());
        position.set("geofenceId", 1);
        
        // Mock condition evaluator to return true for both rules
        when(conditionEvaluator.evaluate(eq("geofenceId == 1"), any(Position.class))).thenReturn(true);
        
        // Mock action executor to return a successful future
        when(actionExecutor.execute(any(Action.class), any(Position.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        
        // Configure rule engine to resolve conflicts (only execute the highest priority action)
        ruleEngine.setConflictResolutionStrategy(RuleEngine.ConflictResolutionStrategy.HIGHEST_PRIORITY_ONLY);
        
        // Execute rule evaluation
        List<Event> events = new ArrayList<>();
        ruleEngine.evaluateRules(position, events::add);
        
        // Verify that only the high priority action was executed
        verify(actionExecutor).execute(eq(highPriorityAction), eq(position));
        verify(actionExecutor, never()).execute(eq(lowPriorityAction), eq(position));
        
        // Verify that only one event was generated
        assertEquals(1, events.size());
    }

    /**
     * Tests the integration between rule parser, condition evaluator, and action executor
     * to ensure that all components work together correctly in the rule evaluation pipeline.
     */
    @Test
    public void testComponentIntegration() {
        // Create a test rule with a complex condition and multiple actions
        Rule complexRule = new Rule();
        complexRule.setId(1L);
        complexRule.setName("Complex Integration Rule");
        complexRule.setCondition("speed > 100 && ignition == true && batteryLevel < 30");
        complexRule.setPriority(1);
        complexRule.setEnabled(true);
        
        Action notifyAction = new Action();
        notifyAction.setType("notification");
        notifyAction.setParameter("type", "custom_alert");
        notifyAction.setParameter("message", "Vehicle speeding with low battery");
        
        Action commandAction = new Action();
        commandAction.setType("command");
        commandAction.setParameter("type", "custom");
        commandAction.setParameter("data", "BATTERY_SAVE_MODE");
        
        complexRule.setActions(Arrays.asList(notifyAction, commandAction));
        
        // Mock rule parser to return our complex rule
        when(ruleParser.parseRules()).thenReturn(Arrays.asList(complexRule));
        
        // Create a position that triggers the rule
        Position position = new Position();
        position.setDeviceId(1);
        position.setTime(new Date());
        position.setSpeed(120.0);
        position.set(Position.KEY_IGNITION, true);
        position.set("batteryLevel", 20);
        
        // Mock condition evaluator to return true for our complex condition
        when(conditionEvaluator.evaluate(eq("speed > 100 && ignition == true && batteryLevel < 30"), any(Position.class)))
                .thenReturn(true);
        
        // Mock action executor to return a successful future for both actions
        when(actionExecutor.execute(any(Action.class), any(Position.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        
        // Execute rule evaluation
        List<Event> events = new ArrayList<>();
        ruleEngine.evaluateRules(position, events::add);
        
        // Verify that the condition was evaluated
        verify(conditionEvaluator).evaluate(eq("speed > 100 && ignition == true && batteryLevel < 30"), eq(position));
        
        // Verify that both actions were executed
        verify(actionExecutor).execute(eq(notifyAction), eq(position));
        verify(actionExecutor).execute(eq(commandAction), eq(position));
        
        // Verify that an event was generated
        assertEquals(1, events.size());
        Event event = events.get(0);
        assertEquals("custom_alert", event.getType());
        assertEquals(position.getDeviceId(), event.getDeviceId());
        assertEquals("Vehicle speeding with low battery", event.getString("message"));
    }

    /**
     * Tests rule engine configuration options to ensure that the rule engine behaves correctly
     * with different configuration settings.
     */
    @Test
    public void testRuleEngineConfiguration() {
        // Create a test rule
        Rule testRule = new Rule();
        testRule.setId(1L);
        testRule.setName("Test Rule");
        testRule.setCondition("speed > 100");
        testRule.setPriority(1);
        testRule.setEnabled(true);
        
        Action testAction = new Action();
        testAction.setType("notification");
        testAction.setParameter("type", "test_alert");
        testRule.setActions(Arrays.asList(testAction));
        
        // Mock rule parser to return our test rule
        when(ruleParser.parseRules()).thenReturn(Arrays.asList(testRule));
        
        // Create a position that triggers the rule
        Position position = new Position();
        position.setDeviceId(1);
        position.setTime(new Date());
        position.setSpeed(120.0);
        
        // Mock condition evaluator to return true
        when(conditionEvaluator.evaluate(eq("speed > 100"), any(Position.class))).thenReturn(true);
        
        // Mock action executor to return a successful future
        when(actionExecutor.execute(any(Action.class), any(Position.class)))
                .thenReturn(CompletableFuture.completedFuture(true));
        
        // Test with rule engine disabled
        ruleEngine.setEnabled(false);
        List<Event> eventsDisabled = new ArrayList<>();
        ruleEngine.evaluateRules(position, eventsDisabled::add);
        
        // Verify that no rules were evaluated when engine is disabled
        assertTrue(eventsDisabled.isEmpty());
        verify(conditionEvaluator, never()).evaluate(any(), any());
        
        // Test with rule engine enabled
        ruleEngine.setEnabled(true);
        List<Event> eventsEnabled = new ArrayList<>();
        ruleEngine.evaluateRules(position, eventsEnabled::add);
        
        // Verify that rules were evaluated when engine is enabled
        assertEquals(1, eventsEnabled.size());
        verify(conditionEvaluator).evaluate(eq("speed > 100"), eq(position));
        
        // Test with different conflict resolution strategies
        ruleEngine.setConflictResolutionStrategy(RuleEngine.ConflictResolutionStrategy.EXECUTE_ALL);
        // This would need additional test logic to verify all conflicting actions are executed
        
        // Test with different rule evaluation modes
        ruleEngine.setEvaluationMode(RuleEngine.EvaluationMode.PARALLEL);
        // This would need additional test logic to verify parallel execution
    }
}