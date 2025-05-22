/*
 * Copyright 2015 - 2023 Anton Tananaev (anton@traccar.org)
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

import java.io.Serializable;
import java.security.Principal;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents the security principal for authenticated users and services.
 * Enhanced to support JWT claims, distributed session management, and service identity.
 */
public class UserPrincipal implements Principal, Serializable {

    private static final long serialVersionUID = 1L;

    private final long userId;
    private final Date expiration;
    private final String sessionId;
    private final Map<String, Object> claims;
    private final Set<String> roles;
    private final Set<String> scopes;
    private final boolean isServiceIdentity;
    private final String serviceName;
    private final String correlationId;
    private Date lastActivity;

    /**
     * Constructor for user-based principal.
     * 
     * @param userId User identifier
     * @param expiration Token expiration date
     * @param sessionId Unique session identifier
     * @param claims JWT claims associated with this principal
     * @param roles Set of roles assigned to this principal
     * @param scopes Set of permission scopes for this principal
     */
    public UserPrincipal(long userId, Date expiration, String sessionId, Map<String, Object> claims, 
                        Set<String> roles, Set<String> scopes) {
        this.userId = userId;
        this.expiration = expiration;
        this.sessionId = sessionId;
        this.claims = claims != null ? claims : new HashMap<>();
        this.roles = roles != null ? roles : new HashSet<>();
        this.scopes = scopes != null ? scopes : new HashSet<>();
        this.isServiceIdentity = false;
        this.serviceName = null;
        this.correlationId = null;
        this.lastActivity = new Date();
    }

    /**
     * Constructor for service-based principal.
     * 
     * @param serviceName Name of the service
     * @param expiration Token expiration date
     * @param scopes Set of permission scopes for this service
     * @param correlationId Request correlation ID for distributed tracing
     */
    public UserPrincipal(String serviceName, Date expiration, Set<String> scopes, String correlationId) {
        this.userId = 0; // Service identities don't have user IDs
        this.expiration = expiration;
        this.sessionId = null; // Services don't have sessions
        this.claims = new HashMap<>();
        this.roles = new HashSet<>();
        this.scopes = scopes != null ? scopes : new HashSet<>();
        this.isServiceIdentity = true;
        this.serviceName = serviceName;
        this.correlationId = correlationId;
        this.lastActivity = new Date();
    }

    /**
     * Legacy constructor for backward compatibility.
     * 
     * @param userId User identifier
     * @param expiration Token expiration date
     */
    public UserPrincipal(long userId, Date expiration) {
        this(userId, expiration, null, null, null, null);
    }

    /**
     * Get the user ID associated with this principal.
     * 
     * @return User ID or 0 for service identities
     */
    public Long getUserId() {
        return userId;
    }

    /**
     * Get the expiration date for this principal.
     * 
     * @return Expiration date
     */
    public Date getExpiration() {
        return expiration;
    }

    /**
     * Get the session ID for this principal.
     * 
     * @return Session ID or null for service identities
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * Get the JWT claims associated with this principal.
     * 
     * @return Map of JWT claims
     */
    public Map<String, Object> getClaims() {
        return claims;
    }

    /**
     * Get a specific claim value.
     * 
     * @param claimName Name of the claim
     * @return Claim value or null if not present
     */
    public Object getClaim(String claimName) {
        return claims.get(claimName);
    }

    /**
     * Get the roles assigned to this principal.
     * 
     * @return Set of role names
     */
    public Set<String> getRoles() {
        return roles;
    }

    /**
     * Check if the principal has a specific role.
     * 
     * @param role Role name to check
     * @return true if the principal has the role
     */
    public boolean hasRole(String role) {
        return roles.contains(role);
    }

    /**
     * Get the permission scopes for this principal.
     * 
     * @return Set of permission scopes
     */
    public Set<String> getScopes() {
        return scopes;
    }

    /**
     * Check if the principal has a specific permission scope.
     * 
     * @param scope Scope to check
     * @return true if the principal has the scope
     */
    public boolean hasScope(String scope) {
        return scopes.contains(scope);
    }

    /**
     * Check if this principal represents a service identity.
     * 
     * @return true if this is a service identity
     */
    public boolean isServiceIdentity() {
        return isServiceIdentity;
    }

    /**
     * Get the service name for service identities.
     * 
     * @return Service name or null for user identities
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Get the correlation ID for distributed tracing.
     * 
     * @return Correlation ID or null if not set
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Get the timestamp of the last activity for this principal.
     * Used for session management and heartbeat.
     * 
     * @return Last activity timestamp
     */
    public Date getLastActivity() {
        return lastActivity;
    }

    /**
     * Update the last activity timestamp to the current time.
     * Used for session heartbeat.
     */
    public void updateLastActivity() {
        this.lastActivity = new Date();
    }

    /**
     * Check if this principal has expired.
     * 
     * @return true if the principal has expired
     */
    public boolean isExpired() {
        return expiration != null && expiration.before(new Date());
    }

    @Override
    public String getName() {
        if (isServiceIdentity) {
            return serviceName;
        }
        return (String) claims.getOrDefault("name", String.valueOf(userId));
    }
}