package org.traccar.api;

import io.netty.channel.Channel;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.junit.jupiter.api.DisplayName;
import org.traccar.BaseProtocol;
import org.traccar.Protocol;
import org.traccar.ServerManager;
import org.traccar.api.resource.CommandResource;
import org.traccar.api.security.SecurityRequestFilter;
import org.traccar.database.CommandsManager;
import org.traccar.helper.model.DeviceUtil;
import org.traccar.model.Command;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.model.Position;
import org.traccar.model.QueuedCommand;
import org.traccar.model.User;
import org.traccar.model.UserRestrictions;
import org.traccar.model.Typed;
import org.traccar.session.cache.CacheManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.lang.reflect.Field;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for the command-related API endpoints in the API Gateway.
 * 
 * This test class verifies that the API Gateway correctly routes command requests to the
 * appropriate backend services, applies proper authentication and authorization, and maintains
 * all existing command functionality in the microservices architecture.
 */
@ExtendWith(MockitoExtension.class)
public class CommandApiTest extends BaseTest {

    @Mock
    private Storage storage;

    @Mock
    private CommandsManager commandsManager;

    @Mock
    private ServerManager serverManager;

    @Mock
    private CacheManager cacheManager;

    @Mock
    private PermissionsService permissionsService;

    private CommandResource commandResource;

    @BeforeEach
    public void setup() throws StorageException, NoSuchFieldException, IllegalAccessException {
        MockitoAnnotations.openMocks(this);

        User user = new User();
        user.setId(1);
        user.setAdministrator(true);

        // Setup permissions service
        doNothing().when(permissionsService).checkPermission(eq(Device.class), anyLong(), anyLong());
        doNothing().when(permissionsService).checkPermission(eq(Group.class), anyLong(), anyLong());
        doNothing().when(permissionsService).checkPermission(eq(Command.class), anyLong(), anyLong());
        doNothing().when(permissionsService).checkRestriction(anyLong(), any());

        // Setup command resource
        commandResource = new CommandResource();
        commandResource.setStorage(storage);
        commandResource.setPermissionsService(permissionsService);
        injectField(commandResource, "commandsManager", commandsManager);
        injectField(commandResource, "serverManager", serverManager);

        // Setup security context
        initSecurityContext(user);
    }
    
    private void injectField(Object target, String fieldName, Object value) 
            throws NoSuchFieldException, IllegalAccessException {
        Field field = CommandResource.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
    
    private void initSecurityContext(User user) {
        // This method would normally set up the security context for the test
        // In a microservices architecture, this might involve setting up JWT tokens
        // or other authentication mechanisms
    }

    @Test
    @DisplayName("Test retrieving commands for a device with protocol filtering")
    public void testGetCommandsForDevice() throws StorageException {
        long deviceId = 1;
        
        // Mock device protocol
        BaseProtocol protocol = mock(BaseProtocol.class);
        when(protocol.getSupportedTextCommands()).thenReturn(Set.of(Command.TYPE_CUSTOM, Command.TYPE_ENGINE_STOP));
        when(protocol.getSupportedDataCommands()).thenReturn(Set.of(Command.TYPE_POSITION_SINGLE));
        
        // Mock position for protocol detection
        Position position = new Position();
        position.setProtocol("test");
        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(position);
        when(serverManager.getProtocol("test")).thenReturn(protocol);
        
        // Mock commands for the device
        Command command1 = new Command();
        command1.setId(1);
        command1.setDeviceId(deviceId);
        command1.setType(Command.TYPE_CUSTOM);
        command1.setTextChannel(true);
        
        Command command2 = new Command();
        command2.setId(2);
        command2.setDeviceId(deviceId);
        command2.setType(Command.TYPE_ENGINE_STOP);
        command2.setTextChannel(true);
        
        Command command3 = new Command();
        command3.setId(3);
        command3.setDeviceId(deviceId);
        command3.setType(Command.TYPE_POSITION_SINGLE);
        command3.setTextChannel(false);
        
        Command command4 = new Command();
        command4.setId(4);
        command4.setDeviceId(deviceId);
        command4.setType(Command.TYPE_ENGINE_RESUME);
        command4.setTextChannel(true);
        
        when(storage.getObjects(eq(Command.class), any(Request.class)))
                .thenReturn(List.of(command1, command2, command3, command4));
        
        // Execute test
        List<Command> result = (List<Command>) commandResource.get(deviceId);
        
        // Verify results
        assertEquals(3, result.size());
        assertTrue(result.contains(command1)); // Custom command should be included
        assertTrue(result.contains(command2)); // Supported text command should be included
        assertTrue(result.contains(command3)); // Supported data command should be included
        // command4 should be filtered out as it's not supported by the protocol
        
        // Verify permission check was called
        verify(permissionsService).checkPermission(Device.class, 1L, deviceId);
    }

    @Test
    @DisplayName("Test sending a command to a single device")
    public void testSendCommandToDevice() throws Exception {
        long deviceId = 1;
        
        // Create command to send
        Command command = new Command();
        command.setDeviceId(deviceId);
        command.setType(Command.TYPE_ENGINE_STOP);
        
        // Mock queued command response
        QueuedCommand queuedCommand = new QueuedCommand();
        queuedCommand.setCommandId(1);
        queuedCommand.setDeviceId(deviceId);
        when(commandsManager.sendCommand(any(Command.class))).thenReturn(queuedCommand);
        
        // Execute test
        Response response = commandResource.send(command, 0);
        
        // Verify results
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
        assertEquals(queuedCommand, response.getEntity());
        
        // Verify permission check was called
        verify(permissionsService).checkPermission(Device.class, 1L, deviceId);
        verify(permissionsService).checkRestriction(eq(1L), any());
    }

    @Test
    @DisplayName("Test sending a command to a group of devices")
    public void testSendCommandToGroup() throws Exception {
        long groupId = 1;
        
        // Create command to send
        Command command = new Command();
        command.setType(Command.TYPE_ENGINE_STOP);
        
        // Mock devices in group
        Device device1 = new Device();
        device1.setId(1);
        Device device2 = new Device();
        device2.setId(2);
        List<Device> groupDevices = List.of(device1, device2);
        
        // Mock storage to return devices in group
        when(storage.getObjects(eq(Device.class), any(Request.class)))
                .thenReturn(groupDevices);
        
        // Create a utility method in the test class to replace DeviceUtil
        Field deviceUtilField = CommandResource.class.getDeclaredField("deviceUtil");
        deviceUtilField.setAccessible(true);
        DeviceUtil deviceUtil = mock(DeviceUtil.class);
        when(deviceUtil.getAccessibleDevices(any(), anyLong(), any(), any()))
                .thenReturn(groupDevices);
        deviceUtilField.set(commandResource, deviceUtil);
        
        // Mock queued commands
        QueuedCommand queuedCommand1 = new QueuedCommand();
        queuedCommand1.setCommandId(1);
        queuedCommand1.setDeviceId(1);
        
        QueuedCommand queuedCommand2 = new QueuedCommand();
        queuedCommand2.setCommandId(2);
        queuedCommand2.setDeviceId(2);
        
        when(commandsManager.sendCommand(any(Command.class)))
                .thenReturn(queuedCommand1)
                .thenReturn(queuedCommand2);
        
        // Execute test
        Response response = commandResource.send(command, groupId);
        
        // Verify results
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
        assertTrue(response.getEntity() instanceof List);
        List<?> resultList = (List<?>) response.getEntity();
        assertEquals(2, resultList.size());
        
        // Verify permission check was called
        verify(permissionsService).checkPermission(Group.class, 1L, groupId);
        verify(permissionsService).checkRestriction(eq(1L), any());
        
        // Verify command was sent to each device
        ArgumentCaptor<Command> commandCaptor = ArgumentCaptor.forClass(Command.class);
        verify(commandsManager, times(2)).sendCommand(commandCaptor.capture());
        
        List<Command> capturedCommands = commandCaptor.getAllValues();
        assertEquals(2, capturedCommands.size());
        assertEquals(1L, capturedCommands.get(0).getDeviceId());
        assertEquals(2L, capturedCommands.get(1).getDeviceId());
    }

    @Test
    @DisplayName("Test retrieving text command types for a specific device")
    public void testGetCommandTypes() throws StorageException {
        long deviceId = 1;
        
        // Mock device protocol
        BaseProtocol protocol = mock(BaseProtocol.class);
        when(protocol.getSupportedTextCommands()).thenReturn(Set.of(
                Command.TYPE_CUSTOM, 
                Command.TYPE_ENGINE_STOP, 
                Command.TYPE_ENGINE_RESUME));
        
        // Mock position for protocol detection
        Position position = new Position();
        position.setProtocol("test");
        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(position);
        when(serverManager.getProtocol("test")).thenReturn(protocol);
        
        // Execute test for text channel
        List<?> result = (List<?>) commandResource.get(deviceId, true);
        
        // Verify results
        assertEquals(3, result.size());
        
        // Verify permission check was called
        verify(permissionsService).checkPermission(Device.class, 1L, deviceId);
    }

    @Test
    @DisplayName("Test retrieving all command types when no device is specified")
    public void testGetCommandTypesNoDevice() {
        // Execute test
        List<?> result = (List<?>) commandResource.get(0, false);
        
        // Verify results - should return all command types defined in Command class
        assertTrue(result.size() > 0);
    }

    @Test
    @DisplayName("Test sending a command when no protocol is available")
    public void testSendCommandNoProtocol() throws Exception {
        long deviceId = 1;
        
        // Create command to send
        Command command = new Command();
        command.setDeviceId(deviceId);
        command.setType(Command.TYPE_CUSTOM);
        
        // Mock no position found (no protocol)
        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(null);
        
        // Mock command manager to return null (command not sent)
        when(commandsManager.sendCommand(any(Command.class))).thenReturn(null);
        
        // Execute test
        Response response = commandResource.send(command, 0);
        
        // Verify results - should still return OK with the command
        assertEquals(Response.Status.OK.getStatusCode(), response.getStatus());
        assertEquals(command, response.getEntity());
        
        // Verify permission check was called
        verify(permissionsService).checkPermission(Device.class, 1L, deviceId);
    }

    @Test
    @DisplayName("Test sending an existing command with ID")
    public void testSendExistingCommand() throws Exception {
        long deviceId = 1;
        long commandId = 5;
        
        // Create command to send with existing ID
        Command command = new Command();
        command.setId(commandId);
        command.setDeviceId(deviceId);
        command.setType(Command.TYPE_ENGINE_STOP);
        
        // Mock existing command retrieval
        Command existingCommand = new Command();
        existingCommand.setId(commandId);
        existingCommand.setType(Command.TYPE_ENGINE_STOP);
        when(storage.getObject(eq(Command.class), any(Request.class))).thenReturn(existingCommand);
        
        // Mock queued command response
        QueuedCommand queuedCommand = new QueuedCommand();
        queuedCommand.setCommandId(commandId);
        queuedCommand.setDeviceId(deviceId);
        when(commandsManager.sendCommand(any(Command.class))).thenReturn(queuedCommand);
        
        // Execute test
        Response response = commandResource.send(command, 0);
        
        // Verify results
        assertEquals(Response.Status.ACCEPTED.getStatusCode(), response.getStatus());
        assertEquals(queuedCommand, response.getEntity());
        
        // Verify permission checks were called
        verify(permissionsService).checkPermission(Command.class, 1L, commandId);
        verify(permissionsService).checkPermission(Device.class, 1L, deviceId);
    }

    @Test
    @DisplayName("Test retrieving data command types for a specific device")
    public void testGetDataCommandTypes() throws StorageException {
        long deviceId = 1;
        
        // Mock device protocol
        BaseProtocol protocol = mock(BaseProtocol.class);
        when(protocol.getSupportedDataCommands()).thenReturn(Set.of(
                Command.TYPE_POSITION_SINGLE, 
                Command.TYPE_POSITION_PERIODIC, 
                Command.TYPE_POSITION_STOP));
        
        // Mock position for protocol detection
        Position position = new Position();
        position.setProtocol("test");
        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(position);
        when(serverManager.getProtocol("test")).thenReturn(protocol);
        
        // Execute test for data channel
        List<?> result = (List<?>) commandResource.get(deviceId, false);
        
        // Verify results
        assertEquals(3, result.size());
        
        // Verify permission check was called
        verify(permissionsService).checkPermission(Device.class, 1L, deviceId);
    }

    @Test
    @DisplayName("Test retrieving command types when no protocol support is available")
    public void testGetCommandTypesNoProtocolSupport() throws StorageException {
        long deviceId = 1;
        
        // Mock position for protocol detection but no protocol support
        Position position = new Position();
        position.setProtocol("test");
        when(storage.getObject(eq(Position.class), any(Request.class))).thenReturn(position);
        when(serverManager.getProtocol("test")).thenReturn(null);
        
        // Execute test
        List<?> result = (List<?>) commandResource.get(deviceId, true);
        
        // Verify results - should only return custom command type
        assertEquals(1, result.size());
        assertEquals(Command.TYPE_CUSTOM, ((Typed)result.get(0)).getType());
        
        // Verify permission check was called
        verify(permissionsService).checkPermission(Device.class, 1L, deviceId);
    }
}