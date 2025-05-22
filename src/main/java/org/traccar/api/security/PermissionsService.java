/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.api.security;

import com.google.inject.servlet.RequestScoped;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.model.BaseModel;
import org.traccar.model.Calendar;
import org.traccar.model.Command;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.model.GroupedModel;
import org.traccar.model.ManagedUser;
import org.traccar.model.Notification;
import org.traccar.model.Schedulable;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.model.UserRestrictions;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;
import org.traccar.session.cache.CacheManager;
import org.traccar.api.security.jwt.JwtService;
import org.traccar.api.security.mTLS.CertificateValidator;

import jakarta.inject.Inject;
import java.util.Objects;
import java.util.Optional;

/**
 * Service responsible for handling permissions and authorization checks.
 * Enhanced with distributed tracing, metrics collection, and support for JWT and mTLS authentication.
 */
@RequestScoped
public class PermissionsService {

    private final Storage storage;
    private final CacheManager cacheManager;
    private final JwtService jwtService;
    private final CertificateValidator certificateValidator;
    private final Tracer tracer;
    private final Meter meter;
    
    private Server server;
    private User user;
    
    // Metrics for authorization decisions
    private final LongCounter permissionChecksCounter;
    private final LongCounter permissionDeniedCounter;

    /**
     * Creates a new instance of the PermissionsService.
     *
     * @param storage The storage service for database operations
     * @param cacheManager The cache manager for distributed session management
     * @param jwtService The JWT service for token validation
     * @param certificateValidator The certificate validator for mTLS authentication
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meter The OpenTelemetry meter for metrics collection
     */
    @Inject
    public PermissionsService(
            Storage storage, 
            CacheManager cacheManager, 
            JwtService jwtService, 
            CertificateValidator certificateValidator,
            Tracer tracer,
            Meter meter) {
        this.storage = storage;
        this.cacheManager = cacheManager;
        this.jwtService = jwtService;
        this.certificateValidator = certificateValidator;
        this.tracer = tracer;
        this.meter = meter;
        
        // Initialize metrics
        this.permissionChecksCounter = meter.counterBuilder("permission.checks")
                .setDescription("Number of permission checks performed")
                .build();
        
        this.permissionDeniedCounter = meter.counterBuilder("permission.denied")
                .setDescription("Number of permission checks that were denied")
                .build();
    }

    /**
     * Gets the server configuration.
     *
     * @return The server configuration
     * @throws StorageException If a storage error occurs
     */
    public Server getServer() throws StorageException {
        if (server == null) {
            server = storage.getObject(
                    Server.class, new Request(new Columns.All()));
        }
        return server;
    }

    /**
     * Gets a user by ID, checking both distributed cache and database.
     * Also supports retrieving user context from JWT claims.
     *
     * @param userId The user ID
     * @return The user object
     * @throws StorageException If a storage error occurs
     */
    public User getUser(long userId) throws StorageException {
        Span span = tracer.spanBuilder("PermissionsService.getUser")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .startSpan();
        
        try {
            if (user == null && userId > 0) {
                // First check Redis cache
                Optional<User> cachedUser = cacheManager.getUser(userId);
                if (cachedUser.isPresent()) {
                    user = cachedUser.get();
                    span.setAttribute("cache.hit", true);
                } else {
                    span.setAttribute("cache.hit", false);
                    if (userId == ServiceAccountUser.ID) {
                        user = new ServiceAccountUser();
                    } else {
                        user = storage.getObject(
                                User.class, new Request(new Columns.All(), new Condition.Equals("id", userId)));
                        // Cache the user for future requests
                        if (user != null) {
                            cacheManager.putUser(user);
                        }
                    }
                }
            }
            return user;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Gets a user from JWT claims.
     *
     * @param token The JWT token
     * @return The user object
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the token is invalid
     */
    public User getUserFromToken(String token) throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.getUserFromToken")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try {
            // Validate the token and extract user ID
            Long userId = jwtService.validateTokenAndGetUserId(token);
            if (userId == null) {
                span.setAttribute("token.valid", false);
                throw new SecurityException("Invalid token");
            }
            
            span.setAttribute("token.valid", true);
            span.setAttribute("userId", userId);
            
            return getUser(userId);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Gets a service account from mTLS certificate.
     *
     * @param certificateData The certificate data
     * @return The service account user
     * @throws SecurityException If the certificate is invalid
     */
    public User getUserFromCertificate(String certificateData) throws SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.getUserFromCertificate")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try {
            // Validate the certificate
            String serviceId = certificateValidator.validateCertificate(certificateData);
            if (serviceId == null) {
                span.setAttribute("certificate.valid", false);
                throw new SecurityException("Invalid certificate");
            }
            
            span.setAttribute("certificate.valid", true);
            span.setAttribute("serviceId", serviceId);
            
            // Return a service account user
            return new ServiceAccountUser(serviceId);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a user is not an administrator.
     *
     * @param userId The user ID
     * @return True if the user is not an administrator, false otherwise
     * @throws StorageException If a storage error occurs
     */
    public boolean notAdmin(long userId) throws StorageException {
        Span span = tracer.spanBuilder("PermissionsService.notAdmin")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .startSpan();
        
        try {
            boolean result = !getUser(userId).getAdministrator();
            span.setAttribute("result", result);
            return result;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a user has administrator privileges.
     *
     * @param userId The user ID
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the user is not an administrator
     */
    public void checkAdmin(long userId) throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.checkAdmin")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .startSpan();
        
        try {
            permissionChecksCounter.add(1, Attributes.of(AttributeKey.stringKey("check_type"), "admin"));
            
            if (!getUser(userId).getAdministrator()) {
                permissionDeniedCounter.add(1, Attributes.of(AttributeKey.stringKey("check_type"), "admin"));
                span.setAttribute("authorized", false);
                throw new SecurityException("Administrator access required");
            }
            span.setAttribute("authorized", true);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a user has manager privileges.
     *
     * @param userId The user ID
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the user is not a manager
     */
    public void checkManager(long userId) throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.checkManager")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .startSpan();
        
        try {
            permissionChecksCounter.add(1, Attributes.of(AttributeKey.stringKey("check_type"), "manager"));
            
            if (!getUser(userId).getAdministrator() && getUser(userId).getUserLimit() == 0) {
                permissionDeniedCounter.add(1, Attributes.of(AttributeKey.stringKey("check_type"), "manager"));
                span.setAttribute("authorized", false);
                throw new SecurityException("Manager access required");
            }
            span.setAttribute("authorized", true);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Callback interface for checking restrictions.
     */
    public interface CheckRestrictionCallback {
        boolean denied(UserRestrictions userRestrictions);
    }

    /**
     * Checks if a user has restrictions.
     *
     * @param userId The user ID
     * @param callback The callback to check restrictions
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the operation is restricted
     */
    public void checkRestriction(
            long userId, CheckRestrictionCallback callback) throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.checkRestriction")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .startSpan();
        
        try {
            permissionChecksCounter.add(1, Attributes.of(AttributeKey.stringKey("check_type"), "restriction"));
            
            if (!getUser(userId).getAdministrator()
                    && (callback.denied(getServer()) || callback.denied(getUser(userId)))) {
                permissionDeniedCounter.add(1, Attributes.of(AttributeKey.stringKey("check_type"), "restriction"));
                span.setAttribute("authorized", false);
                throw new SecurityException("Operation restricted");
            }
            span.setAttribute("authorized", true);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a user can edit an object of a specific class.
     *
     * @param userId The user ID
     * @param clazz The class of the object
     * @param addition Whether the operation is an addition
     * @param skipReadonly Whether to skip readonly checks
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the user does not have edit permissions
     */
    public void checkEdit(
            long userId, Class<?> clazz, boolean addition, boolean skipReadonly)
            throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.checkEdit")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .setAttribute("class", clazz.getSimpleName())
                .setAttribute("addition", addition)
                .setAttribute("skipReadonly", skipReadonly)
                .startSpan();
        
        try {
            permissionChecksCounter.add(1, Attributes.of(
                AttributeKey.stringKey("check_type"), "edit",
                AttributeKey.stringKey("class"), clazz.getSimpleName()));
            
            if (!getUser(userId).getAdministrator()) {
                boolean denied = false;
                if (!skipReadonly && (getServer().getReadonly() || getUser(userId).getReadonly())) {
                    denied = true;
                } else if (clazz.equals(Device.class)) {
                    denied = getServer().getDeviceReadonly() || getUser(userId).getDeviceReadonly()
                            || addition && getUser(userId).getDeviceLimit() == 0;
                    if (!denied && addition && getUser(userId).getDeviceLimit() > 0) {
                        int deviceCount = storage.getObjects(Device.class, new Request(
                                new Columns.Include("id"),
                                new Condition.Permission(User.class, userId, Device.class))).size();
                        denied = deviceCount >= getUser(userId).getDeviceLimit();
                    }
                } else if (clazz.equals(Command.class)) {
                    denied = getServer().getLimitCommands() || getUser(userId).getLimitCommands();
                }
                if (denied) {
                    permissionDeniedCounter.add(1, Attributes.of(
                        AttributeKey.stringKey("check_type"), "edit",
                        AttributeKey.stringKey("class"), clazz.getSimpleName()));
                    span.setAttribute("authorized", false);
                    throw new SecurityException("Write access denied");
                }
            }
            span.setAttribute("authorized", true);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a user can edit a specific object.
     *
     * @param userId The user ID
     * @param object The object to edit
     * @param addition Whether the operation is an addition
     * @param skipReadonly Whether to skip readonly checks
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the user does not have edit permissions
     */
    public void checkEdit(
            long userId, BaseModel object, boolean addition, boolean skipReadonly)
            throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.checkEdit")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .setAttribute("objectId", object.getId())
                .setAttribute("objectClass", object.getClass().getSimpleName())
                .setAttribute("addition", addition)
                .setAttribute("skipReadonly", skipReadonly)
                .startSpan();
        
        try {
            permissionChecksCounter.add(1, Attributes.of(
                AttributeKey.stringKey("check_type"), "edit_object",
                AttributeKey.stringKey("class"), object.getClass().getSimpleName()));
            
            if (!getUser(userId).getAdministrator()) {
                checkEdit(userId, object.getClass(), addition, skipReadonly);
                if (object instanceof GroupedModel after) {
                    if (after.getGroupId() > 0) {
                        GroupedModel before = null;
                        if (!addition) {
                            before = storage.getObject(after.getClass(), new Request(
                                    new Columns.Include("groupId"), new Condition.Equals("id", after.getId())));
                        }
                        if (before == null || before.getGroupId() != after.getGroupId()) {
                            checkPermission(Group.class, userId, after.getGroupId());
                        }
                    }
                }
                if (object instanceof Schedulable after) {
                    if (after.getCalendarId() > 0) {
                        Schedulable before = null;
                        if (!addition) {
                            before = storage.getObject(after.getClass(), new Request(
                                    new Columns.Include("calendarId"), new Condition.Equals("id", object.getId())));
                        }
                        if (before == null || before.getCalendarId() != after.getCalendarId()) {
                            checkPermission(Calendar.class, userId, after.getCalendarId());
                        }
                    }
                }
                if (object instanceof Notification after) {
                    if (after.getCommandId() > 0) {
                        Notification before = null;
                        if (!addition) {
                            before = storage.getObject(after.getClass(), new Request(
                                    new Columns.Include("commandId"), new Condition.Equals("id", object.getId())));
                        }
                        if (before == null || before.getCommandId() != after.getCommandId()) {
                            checkPermission(Command.class, userId, after.getCommandId());
                        }
                    }
                }
            }
            span.setAttribute("authorized", true);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            permissionDeniedCounter.add(1, Attributes.of(
                AttributeKey.stringKey("check_type"), "edit_object",
                AttributeKey.stringKey("class"), object.getClass().getSimpleName()));
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a user can access another user.
     *
     * @param userId The user ID
     * @param managedUserId The managed user ID
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the user does not have access to the managed user
     */
    public void checkUser(long userId, long managedUserId) throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.checkUser")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .setAttribute("managedUserId", managedUserId)
                .startSpan();
        
        try {
            permissionChecksCounter.add(1, Attributes.of(
                AttributeKey.stringKey("check_type"), "user_access",
                AttributeKey.stringKey("managedUserId"), String.valueOf(managedUserId)));
            
            if (userId != managedUserId && !getUser(userId).getAdministrator()) {
                if (!getUser(userId).getManager()
                        || storage.getPermissions(User.class, userId, ManagedUser.class, managedUserId).isEmpty()) {
                    permissionDeniedCounter.add(1, Attributes.of(
                        AttributeKey.stringKey("check_type"), "user_access",
                        AttributeKey.stringKey("managedUserId"), String.valueOf(managedUserId)));
                    span.setAttribute("authorized", false);
                    throw new SecurityException("User access denied");
                }
            }
            span.setAttribute("authorized", true);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a user can update another user.
     *
     * @param userId The user ID
     * @param before The user before the update
     * @param after The user after the update
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the user does not have permission to update the user
     */
    public void checkUserUpdate(long userId, User before, User after) throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.checkUserUpdate")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .setAttribute("targetUserId", after.getId())
                .startSpan();
        
        try {
            permissionChecksCounter.add(1, Attributes.of(
                AttributeKey.stringKey("check_type"), "user_update",
                AttributeKey.stringKey("targetUserId"), String.valueOf(after.getId())));
            
            if (before.getAdministrator() != after.getAdministrator()
                    || before.getDeviceLimit() != after.getDeviceLimit()
                    || before.getUserLimit() != after.getUserLimit()) {
                checkAdmin(userId);
            }
            User user = userId > 0 ? getUser(userId) : null;
            if (user != null && user.getExpirationTime() != null
                    && !Objects.equals(before.getExpirationTime(), after.getExpirationTime())
                    && (after.getExpirationTime() == null
                    || user.getExpirationTime().compareTo(after.getExpirationTime()) < 0)) {
                checkAdmin(userId);
            }
            if (before.getReadonly() != after.getReadonly()
                    || before.getDeviceReadonly() != after.getDeviceReadonly()
                    || before.getDisabled() != after.getDisabled()
                    || before.getLimitCommands() != after.getLimitCommands()
                    || before.getDisableReports() != after.getDisableReports()
                    || before.getFixedEmail() != after.getFixedEmail()) {
                if (userId == after.getId()) {
                    checkAdmin(userId);
                } else if (after.getId() > 0) {
                    checkUser(userId, after.getId());
                } else {
                    checkManager(userId);
                }
            }
            if (before.getFixedEmail() && !before.getEmail().equals(after.getEmail())) {
                checkAdmin(userId);
            }
            span.setAttribute("authorized", true);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            permissionDeniedCounter.add(1, Attributes.of(
                AttributeKey.stringKey("check_type"), "user_update",
                AttributeKey.stringKey("targetUserId"), String.valueOf(after.getId())));
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a user has permission to access an object.
     *
     * @param clazz The class of the object
     * @param userId The user ID
     * @param objectId The object ID
     * @param <T> The type of the object
     * @throws StorageException If a storage error occurs
     * @throws SecurityException If the user does not have permission to access the object
     */
    public <T extends BaseModel> void checkPermission(
            Class<T> clazz, long userId, long objectId) throws StorageException, SecurityException {
        Span span = tracer.spanBuilder("PermissionsService.checkPermission")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("userId", userId)
                .setAttribute("objectId", objectId)
                .setAttribute("objectClass", clazz.getSimpleName())
                .startSpan();
        
        try {
            permissionChecksCounter.add(1, Attributes.of(
                AttributeKey.stringKey("check_type"), "permission",
                AttributeKey.stringKey("class"), clazz.getSimpleName(),
                AttributeKey.stringKey("objectId"), String.valueOf(objectId)));
            
            if (!getUser(userId).getAdministrator() && !(clazz.equals(User.class) && userId == objectId)) {
                var object = storage.getObject(clazz, new Request(
                        new Columns.Include("id"),
                        new Condition.And(
                                new Condition.Equals("id", objectId),
                                new Condition.Permission(
                                        User.class, userId, clazz.equals(User.class) ? ManagedUser.class : clazz))));
                if (object == null) {
                    permissionDeniedCounter.add(1, Attributes.of(
                        AttributeKey.stringKey("check_type"), "permission",
                        AttributeKey.stringKey("class"), clazz.getSimpleName(),
                        AttributeKey.stringKey("objectId"), String.valueOf(objectId)));
                    span.setAttribute("authorized", false);
                    throw new SecurityException(clazz.getSimpleName() + " access denied");
                }
            }
            span.setAttribute("authorized", true);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }

}