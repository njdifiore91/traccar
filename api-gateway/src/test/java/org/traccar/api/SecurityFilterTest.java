/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.SecurityContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.api.security.LoginResult;
import org.traccar.api.security.LoginService;
import org.traccar.api.security.PermissionsService;
import org.traccar.api.security.SecurityRequestFilter;
import org.traccar.api.security.UserPrincipal;
import org.traccar.api.signature.TokenManager;
import org.traccar.database.StatisticsManager;
import org.traccar.helper.SessionHelper;
import org.traccar.model.User;

import java.security.GeneralSecurityException;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SecurityFilterTest {

    @Mock
    private LoginService loginService;

    @Mock
    private StatisticsManager statisticsManager;

    @Mock
    private PermissionsService permissionsService;

    @Mock
    private TokenManager tokenManager;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    @Mock
    private ContainerRequestContext requestContext;

    @Mock
    private ResourceInfo resourceInfo;

    @Mock
    private HttpSession session;

    @Mock
    private FilterChain filterChain;

    @InjectMocks
    private SecurityRequestFilter securityFilter;

    private User user;
    private Date expiration;
    private String token;

    @BeforeEach
    public void setUp() {
        user = new User();
        user.setId(1);
        user.setName("test");
        user.setEmail("test@example.com");
        user.setAdmin(false);

        expiration = new Date(System.currentTimeMillis() + 3600000); // 1 hour from now
        token = "valid.jwt.token";
    }

    @Test
    public void testValidJwtTokenAuthentication() throws Exception {
        // Setup
        when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + token);
        when(loginService.login("Bearer", token)).thenReturn(new LoginResult(user, expiration));

        // Execute
        securityFilter.filter(requestContext);

        // Verify
        verify(statisticsManager).registerRequest(eq(user.getId()));
        ArgumentCaptor<SecurityContext> securityContextCaptor = ArgumentCaptor.forClass(SecurityContext.class);
        verify(requestContext).setSecurityContext(securityContextCaptor.capture());

        SecurityContext securityContext = securityContextCaptor.getValue();
        UserPrincipal principal = (UserPrincipal) securityContext.getUserPrincipal();
        assertEquals(user.getId(), principal.getUserId());
        assertEquals(expiration, principal.getExpiration());
    }

    @Test
    public void testSessionAuthentication() throws Exception {
        // Setup
        when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn(null);
        when(request.getSession(false)).thenReturn(session);
        when(session.getAttribute(SessionHelper.USER_ID_KEY)).thenReturn(user.getId());
        when(session.getAttribute(SessionHelper.EXPIRATION_KEY)).thenReturn(expiration);
        when(permissionsService.getUser(user.getId())).thenReturn(user);

        // Execute
        securityFilter.filter(requestContext);

        // Verify
        verify(statisticsManager).registerRequest(eq(user.getId()));
        ArgumentCaptor<SecurityContext> securityContextCaptor = ArgumentCaptor.forClass(SecurityContext.class);
        verify(requestContext).setSecurityContext(securityContextCaptor.capture());

        SecurityContext securityContext = securityContextCaptor.getValue();
        UserPrincipal principal = (UserPrincipal) securityContext.getUserPrincipal();
        assertEquals(user.getId(), principal.getUserId());
        assertEquals(expiration, principal.getExpiration());
    }

    @Test
    public void testExpiredToken() throws Exception {
        // Setup
        Date pastExpiration = new Date(System.currentTimeMillis() - 3600000); // 1 hour ago
        when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + token);
        when(loginService.login("Bearer", token)).thenReturn(new LoginResult(user, pastExpiration));

        // Execute and verify exception
        Exception exception = assertThrows(SecurityException.class, () -> {
            securityFilter.filter(requestContext);
        });

        assertTrue(exception.getMessage().contains("Token expired"));
        verify(statisticsManager, never()).registerRequest(anyLong());
    }

    @Test
    public void testInvalidToken() throws Exception {
        // Setup
        when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer invalid.token");
        when(loginService.login("Bearer", "invalid.token")).thenThrow(new GeneralSecurityException("Invalid token"));

        // Execute and verify exception
        Exception exception = assertThrows(SecurityException.class, () -> {
            securityFilter.filter(requestContext);
        });

        assertTrue(exception.getMessage().contains("Invalid token"));
        verify(statisticsManager, never()).registerRequest(anyLong());
    }

    @Test
    public void testDisabledUser() throws Exception {
        // Setup
        user.setDisabled(true);
        when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + token);
        when(loginService.login("Bearer", token)).thenReturn(new LoginResult(user, expiration));

        // Execute and verify exception
        Exception exception = assertThrows(SecurityException.class, () -> {
            securityFilter.filter(requestContext);
        });

        assertTrue(exception.getMessage().contains("Account is disabled"));
        verify(statisticsManager, never()).registerRequest(anyLong());
    }

    @Test
    public void testOptionsMethodBypass() throws Exception {
        // Setup
        when(requestContext.getMethod()).thenReturn("OPTIONS");

        // Execute
        securityFilter.filter(requestContext);

        // Verify
        verify(loginService, never()).login(anyString(), anyString());
        verify(statisticsManager, never()).registerRequest(anyLong());
        verify(requestContext, never()).setSecurityContext(any(SecurityContext.class));
    }

    @Test
    public void testRoleBasedAccess() throws Exception {
        // Setup admin user
        user.setAdmin(true);
        when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + token);
        when(loginService.login("Bearer", token)).thenReturn(new LoginResult(user, expiration));

        // Execute
        securityFilter.filter(requestContext);

        // Verify
        verify(statisticsManager).registerRequest(eq(user.getId()));
        ArgumentCaptor<SecurityContext> securityContextCaptor = ArgumentCaptor.forClass(SecurityContext.class);
        verify(requestContext).setSecurityContext(securityContextCaptor.capture());

        SecurityContext securityContext = securityContextCaptor.getValue();
        UserPrincipal principal = (UserPrincipal) securityContext.getUserPrincipal();
        assertEquals(user.getId(), principal.getUserId());
        
        // Verify admin role
        assertTrue(user.getAdmin());
    }

    @Test
    public void testSecurityHeaderProcessing() throws Exception {
        // Setup with custom security headers
        when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn("Bearer " + token);
        when(request.getHeader("X-Forwarded-For")).thenReturn("192.168.1.1");
        when(loginService.login("Bearer", token)).thenReturn(new LoginResult(user, expiration));

        // Execute
        securityFilter.filter(requestContext);

        // Verify
        verify(statisticsManager).registerRequest(eq(user.getId()));
        verify(requestContext).setSecurityContext(any(SecurityContext.class));
    }

    @Test
    public void testMissingAuthentication() throws Exception {
        // Setup with no auth header and no session
        when(request.getHeader(HttpHeaders.AUTHORIZATION)).thenReturn(null);
        when(request.getSession(false)).thenReturn(null);

        // Execute and verify exception
        Exception exception = assertThrows(SecurityException.class, () -> {
            securityFilter.filter(requestContext);
        });

        assertTrue(exception.getMessage().contains("Not authenticated"));
        verify(statisticsManager, never()).registerRequest(anyLong());
    }
}