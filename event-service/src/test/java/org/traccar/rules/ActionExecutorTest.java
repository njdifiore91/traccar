package org.traccar.rules;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.messaging.MessagePublisher;
import org.traccar.notification.NotificationService;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the ActionExecutor class which is responsible for executing
 * actions when rule conditions are met in the Event Service's rule engine.
 */
@ExtendWith(MockitoExtension.class)
public class ActionExecutorTest {

    @Mock
    private MessagePublisher messagePublisher;

    @Mock
    private NotificationService notificationService;

    @Mock
    private CommandExecutionService commandExecutionService;

    @Mock
    private DeviceStateService deviceStateService;

    @InjectMocks
    private ActionExecutor actionExecutor;

    private Position position;
    private Map<String, Object> context;

    @BeforeEach
    public void setUp() {
        position = new Position();
        position.setDeviceId(1L);
        position.setLatitude(52.5163);
        position.setLongitude(13.3779);
        position.setSpeed(60.0);
        position.set(Position.KEY_IGNITION, true);

        context = new HashMap<>();
        context.put("deviceId", 1L);
        context.put("ruleId", "speed-limit-rule");
    }

    @Test
    public void testExecuteEventGenerationAction() {
        // Arrange
        Action action = new Action();
        action.setType(ActionType.GENERATE_EVENT);
        action.setParameter("eventType", Event.TYPE_OVERSPEED);
        action.setParameter("attributes", "{\"speed\":60.0}");

        when(messagePublisher.publish(eq("events"), any(Event.class))).thenReturn(CompletableFuture.completedFuture(null));

        // Act
        boolean result = actionExecutor.execute(action, position, context);

        // Assert
        assertTrue(result, "Action execution should succeed");
        verify(messagePublisher).publish(eq("events"), any(Event.class));
    }

    @Test
    public void testExecuteNotificationAction() {
        // Arrange
        Action action = new Action();
        action.setType(ActionType.SEND_NOTIFICATION);
        action.setParameter("notificationType", "overspeed");
        action.setParameter("recipients", "admin,owner");
        action.setParameter("template", "speed-alert");

        when(notificationService.sendNotification(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));

        // Act
        boolean result = actionExecutor.execute(action, position, context);

        // Assert
        assertTrue(result, "Action execution should succeed");
        verify(notificationService).sendNotification(
                eq("overspeed"),
                eq("admin,owner"),
                eq("speed-alert"),
                any(Map.class));
    }

    @Test
    public void testExecuteCommandAction() {
        // Arrange
        Action action = new Action();
        action.setType(ActionType.EXECUTE_COMMAND);
        action.setParameter("commandType", "engineStop");
        action.setParameter("attributes", "{\"reason\":\"speeding\"}");

        when(commandExecutionService.executeCommand(anyLong(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));

        // Act
        boolean result = actionExecutor.execute(action, position, context);

        // Assert
        assertTrue(result, "Action execution should succeed");
        verify(commandExecutionService).executeCommand(
                eq(1L),
                eq("engineStop"),
                any(Map.class));
    }

    @Test
    public void testExecuteUpdateDeviceStateAction() {
        // Arrange
        Action action = new Action();
        action.setType(ActionType.UPDATE_DEVICE_STATE);
        action.setParameter("attribute", "status");
        action.setParameter("value", "blocked");

        when(deviceStateService.updateDeviceState(anyLong(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));

        // Act
        boolean result = actionExecutor.execute(action, position, context);

        // Assert
        assertTrue(result, "Action execution should succeed");
        verify(deviceStateService).updateDeviceState(
                eq(1L),
                eq("status"),
                eq("blocked"));
    }

    @Test
    public void testExecuteActionWithDynamicParameters() {
        // Arrange
        Action action = new Action();
        action.setType(ActionType.SEND_NOTIFICATION);
        action.setParameter("notificationType", "custom");
        action.setParameter("recipients", "admin");
        action.setParameter("template", "custom-template");
        action.setParameter("dynamicParams", "true");
        action.setParameter("speedParam", "${position.speed}");
        action.setParameter("locationParam", "${position.latitude},${position.longitude}");

        when(notificationService.sendNotification(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));

        // Act
        boolean result = actionExecutor.execute(action, position, context);

        // Assert
        assertTrue(result, "Action execution should succeed");
        verify(notificationService).sendNotification(
                eq("custom"),
                eq("admin"),
                eq("custom-template"),
                argThat(params -> {
                    return params.containsKey("speedParam") && 
                           params.get("speedParam").equals(60.0) &&
                           params.containsKey("locationParam") &&
                           params.get("locationParam").equals("52.5163,13.3779");
                }));
    }

    @Test
    public void testErrorHandlingDuringActionExecution() {
        // Arrange
        Action action1 = new Action();
        action1.setType(ActionType.SEND_NOTIFICATION);
        action1.setParameter("notificationType", "error");

        Action action2 = new Action();
        action2.setType(ActionType.GENERATE_EVENT);
        action2.setParameter("eventType", Event.TYPE_DEVICE_OFFLINE);

        when(notificationService.sendNotification(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("Notification service unavailable")));

        when(messagePublisher.publish(eq("events"), any(Event.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Act
        boolean result1 = actionExecutor.execute(action1, position, context);
        boolean result2 = actionExecutor.execute(action2, position, context);

        // Assert
        assertFalse(result1, "Action execution should fail when service throws exception");
        assertTrue(result2, "Second action should execute successfully despite first action's failure");

        verify(notificationService).sendNotification(eq("error"), any(), any(), any());
        verify(messagePublisher).publish(eq("events"), any(Event.class));
    }

    @Test
    public void testActionPrioritizationAndSequencing() {
        // Arrange
        Action highPriorityAction = new Action();
        highPriorityAction.setType(ActionType.EXECUTE_COMMAND);
        highPriorityAction.setParameter("commandType", "engineStop");
        highPriorityAction.setPriority(1);

        Action mediumPriorityAction = new Action();
        mediumPriorityAction.setType(ActionType.SEND_NOTIFICATION);
        mediumPriorityAction.setParameter("notificationType", "alert");
        mediumPriorityAction.setPriority(5);

        Action lowPriorityAction = new Action();
        lowPriorityAction.setType(ActionType.GENERATE_EVENT);
        lowPriorityAction.setParameter("eventType", Event.TYPE_ALARM);
        lowPriorityAction.setPriority(10);

        // Mock all service calls to succeed
        when(commandExecutionService.executeCommand(anyLong(), anyString(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(notificationService.sendNotification(any(), any(), any(), any()))
                .thenReturn(CompletableFuture.completedFuture(true));
        when(messagePublisher.publish(eq("events"), any(Event.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Create a list of actions in random order
        ActionList actionList = new ActionList();
        actionList.addAction(mediumPriorityAction);
        actionList.addAction(lowPriorityAction);
        actionList.addAction(highPriorityAction);

        // Act
        boolean result = actionExecutor.executeAll(actionList, position, context);

        // Assert
        assertTrue(result, "All actions should execute successfully");

        // Verify execution order based on priority
        // We need to use inOrder to verify the sequence of calls
        var inOrder = inOrder(commandExecutionService, notificationService, messagePublisher);
        inOrder.verify(commandExecutionService).executeCommand(anyLong(), anyString(), any()); // High priority first
        inOrder.verify(notificationService).sendNotification(any(), any(), any(), any()); // Medium priority second
        inOrder.verify(messagePublisher).publish(eq("events"), any(Event.class)); // Low priority last
    }

    @Test
    public void testInvalidActionType() {
        // Arrange
        Action action = new Action();
        action.setType(null); // Invalid action type

        // Act
        boolean result = actionExecutor.execute(action, position, context);

        // Assert
        assertFalse(result, "Action execution should fail with invalid action type");
        verifyNoInteractions(messagePublisher, notificationService, commandExecutionService, deviceStateService);
    }

    @Test
    public void testMissingRequiredParameters() {
        // Arrange
        Action action = new Action();
        action.setType(ActionType.GENERATE_EVENT);
        // Missing required parameter 'eventType'

        // Act
        boolean result = actionExecutor.execute(action, position, context);

        // Assert
        assertFalse(result, "Action execution should fail with missing required parameters");
        verifyNoInteractions(messagePublisher);
    }
}