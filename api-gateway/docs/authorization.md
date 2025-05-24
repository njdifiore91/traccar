# Authorization System

## Overview

The Traccar API Gateway implements a comprehensive authorization system that enforces access control before routing requests to backend services. This document describes the authorization mechanisms, role-based access control, permission management, and resource authorization rules implemented in the system.

## Role-Based Access Control

The system implements a hierarchical role-based access control (RBAC) model with three primary roles:

### Admin Role

Administrators have full access to all system resources and functionality:

- Can manage all users, devices, and system settings
- Can create, modify, and delete any resource
- Can assign permissions to other users
- Can access system configuration and server settings
- Not subject to resource limits or restrictions

### Manager Role

Managers have limited administrative capabilities:

- Can create and manage users (but not administrators)
- Can manage devices, geofences, and other resources for their users
- Subject to user limit restrictions (configured per manager)
- Cannot modify system-wide settings
- Cannot access resources outside their management scope

### User Role

Regular users have basic access to their assigned resources:

- Can view and interact with devices assigned to them
- Can create geofences, notifications, and reports for their devices
- Subject to device limit restrictions (configured per user)
- Cannot manage other users
- Cannot access resources not explicitly shared with them

## Permission Management

The authorization system manages permissions at multiple levels:

### User Permissions

User permissions control what actions a user can perform:

- **administrator**: Boolean flag indicating administrator privileges
- **userLimit**: Number of users a manager can create (0 for regular users)
- **deviceLimit**: Maximum number of devices a user can have
- **readonly**: Prevents the user from making any changes
- **deviceReadonly**: Prevents the user from modifying device information
- **limitCommands**: Restricts the ability to send commands to devices
- **disableReports**: Prevents access to the reporting functionality

### Resource Permissions

Resource permissions control access to specific entities:

- **Device permissions**: Control which devices a user can access
- **Group permissions**: Control which device groups a user can access
- **Geofence permissions**: Control which geofences a user can access
- **Notification permissions**: Control which notifications a user can access
- **Calendar permissions**: Control which calendars a user can access
- **Command permissions**: Control which saved commands a user can access
- **Attribute permissions**: Control which custom attributes a user can access

### Permission Inheritance

Permissions can be inherited through relationships:

- Users with access to a group automatically have access to all devices in that group
- Managers automatically have access to all resources created by users they manage
- Administrators have implicit access to all resources

## Resource Authorization Rules

The system enforces specific authorization rules for different resource types:

### Device Authorization

- Users can only access devices explicitly shared with them
- Device access can be granted directly or via group membership
- Device limit checks are enforced when creating new devices
- Device readonly restrictions prevent modifications

### User Management Authorization

- Only administrators can create or modify administrator accounts
- Managers can only manage users they have created
- Users cannot modify their own administrative settings
- Fixed email restrictions prevent email changes for certain accounts

### Command Authorization

- Command execution requires both device access and command permission
- Command limitations can be applied at user or server level
- Command history is logged for audit purposes

### Report Authorization

- Report generation requires access to all devices included in the report
- Report access can be restricted at the user level
- Report sharing follows the same permission model as other resources

## Policy Enforcement Points

Authorization is enforced at multiple points in the system:

### API Gateway Authorization

The API Gateway serves as the primary enforcement point for external requests:

- Validates authentication tokens and establishes user identity
- Performs preliminary authorization checks based on user role
- Routes requests to appropriate backend services with user context
- Rejects unauthorized requests with appropriate HTTP status codes

### Service-Level Authorization

Each microservice performs additional authorization checks:

- Validates that the authenticated user has permission for the requested resource
- Enforces resource-specific access rules
- Applies operation-specific permissions (read, write, share, etc.)
- Filters query results to include only authorized resources

### Media Access Authorization

Media resources (images, videos) have specialized authorization:

- Media access requires permission to the associated device
- Media requests are validated through the MediaFilter component
- Device identifiers in media paths are resolved to verify permissions

## Authorization Implementation

The authorization system is implemented through several key components:

### PermissionsService

The `PermissionsService` class provides methods for checking various permissions:

```java
// Check if user has administrator privileges
public void checkAdmin(long userId) throws StorageException, SecurityException

// Check if user has manager privileges
public void checkManager(long userId) throws StorageException, SecurityException

// Check if user has permission to a specific resource
public <T extends BaseModel> void checkPermission(
        Class<T> clazz, long userId, long objectId) throws StorageException, SecurityException

// Check if user has permission to edit a resource
public void checkEdit(
        long userId, BaseModel object, boolean addition, boolean skipReadonly)
        throws StorageException, SecurityException
```

### Security Filters

Security filters intercept requests to enforce authorization:

- `SecurityRequestFilter`: Validates user authentication and establishes security context
- `MediaFilter`: Specialized filter for media resource access
- `ResourceFilter`: Ensures users only access authorized resources

## Audit Logging

All authorization decisions are logged for audit purposes:

- Failed authorization attempts are logged with user ID and requested resource
- Administrative actions are logged with before/after values
- User permission changes are recorded with the user who made the change
- Login/logout events and session management are tracked
- Logs include timestamp, user ID, action type, and affected resources

## Service-to-Service Authorization

Internal service-to-service communication uses secure authorization mechanisms:

- Mutual TLS (mTLS) for service authentication
- JWT tokens with service identity for alternative authentication
- Service mesh authorization policies for fine-grained control
- Kubernetes RBAC for pod-to-pod communication security

## Best Practices for Developers

When developing features that interact with the authorization system:

1. Always use the `PermissionsService` to check permissions before accessing resources
2. Never bypass authorization checks, even for seemingly harmless operations
3. Use the most specific permission check method available for your use case
4. Remember that administrators bypass most restrictions but still generate audit logs
5. Consider permission inheritance when designing new resource types
6. Test authorization with different user roles to ensure proper enforcement

## Example: Checking Device Permission

```java
@Inject
private PermissionsService permissionsService;

public void accessDevice(long userId, long deviceId) {
    try {
        // This will throw SecurityException if the user doesn't have permission
        permissionsService.checkPermission(Device.class, userId, deviceId);
        
        // If we get here, the user has permission to the device
        // Proceed with the operation
    } catch (SecurityException e) {
        // Handle unauthorized access
        throw new ForbiddenException("No access to the device");
    } catch (StorageException e) {
        // Handle database errors
        throw new InternalServerErrorException(e);
    }
}
```

## Example: Checking Edit Permission

```java
@Inject
private PermissionsService permissionsService;

public void updateDevice(long userId, Device device) {
    try {
        // Check if the user can edit this device
        // Parameters: userId, object, isAddition, skipReadonlyCheck
        permissionsService.checkEdit(userId, device, false, false);
        
        // If we get here, the user can edit the device
        // Proceed with the update operation
    } catch (SecurityException e) {
        // Handle unauthorized access
        throw new ForbiddenException("Cannot edit the device");
    } catch (StorageException e) {
        // Handle database errors
        throw new InternalServerErrorException(e);
    }
}
```

## Conclusion

The authorization system is a critical component of the Traccar platform's security architecture. By implementing role-based access control, resource-level permissions, and comprehensive audit logging, the system ensures that users can only access and modify resources appropriate to their role and assigned permissions.

Developers should always use the provided authorization services and never implement custom permission checks that might bypass the centralized authorization system.