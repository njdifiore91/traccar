/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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

import jakarta.ws.rs.core.SecurityContext;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Security context implementation that supports JWT-based authentication and role-based access control.
 * This class is used by the API Gateway to authenticate and authorize requests.
 */
public class UserSecurityContext implements SecurityContext {

    private final UserPrincipal principal;
    private final Set<String> roles;
    private final boolean secure;
    private final String authenticationScheme;
    private final boolean serviceIdentity;

    /**
     * Creates a new security context for a user authentication.
     *
     * @param principal The user principal
     * @param roles Set of roles assigned to the user
     * @param secure Whether the request is secure (HTTPS)
     */
    public UserSecurityContext(UserPrincipal principal, Set<String> roles, boolean secure) {
        this.principal = principal;
        this.roles = roles != null ? new HashSet<>(roles) : Collections.emptySet();
        this.secure = secure;
        this.authenticationScheme = BEARER_AUTH;
        this.serviceIdentity = false;
    }

    /**
     * Creates a new security context for a service-to-service authentication.
     *
     * @param principal The service principal
     * @param roles Set of roles assigned to the service
     * @param secure Whether the request is secure (HTTPS)
     * @param serviceIdentity Whether this is a service identity
     */
    public UserSecurityContext(UserPrincipal principal, Set<String> roles, boolean secure, boolean serviceIdentity) {
        this.principal = principal;
        this.roles = roles != null ? new HashSet<>(roles) : Collections.emptySet();
        this.secure = secure;
        this.authenticationScheme = BEARER_AUTH;
        this.serviceIdentity = serviceIdentity;
    }

    @Override
    public Principal getUserPrincipal() {
        return principal;
    }

    @Override
    public boolean isUserInRole(String role) {
        return roles.contains(role);
    }

    @Override
    public boolean isSecure() {
        return secure;
    }

    @Override
    public String getAuthenticationScheme() {
        return authenticationScheme;
    }

    /**
     * Checks if this security context represents a service identity.
     * Service identities are used for service-to-service communication.
     *
     * @return true if this is a service identity, false otherwise
     */
    public boolean isServiceIdentity() {
        return serviceIdentity;
    }

    /**
     * Gets the set of roles assigned to the user or service.
     *
     * @return The set of roles
     */
    public Set<String> getRoles() {
        return Collections.unmodifiableSet(roles);
    }
}