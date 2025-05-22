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

import com.google.inject.Injector;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.JwtException;
import io.jsonwebtoken.Jwts;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.data.redis.core.RedisTemplate;
import org.traccar.api.signature.TokenManager;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.database.StatisticsManager;
import org.traccar.helper.SessionHelper;
import org.traccar.model.User;
import org.traccar.storage.StorageException;

import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.ResourceInfo;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.SecurityContext;
import java.io.IOException;
import java.lang.reflect.Method;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Security filter that handles authentication and authorization for API requests.
 * Supports multiple authentication methods including JWT, Basic Auth, Session, and Service-to-Service.
 * Integrates with distributed Redis session store and implements circuit breaker pattern.
 */
public class SecurityRequestFilter implements ContainerRequestFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SecurityRequestFilter.class);

    @Context
    private HttpServletRequest request;

    @Context
    private ResourceInfo resourceInfo;

    @Inject
    private LoginService loginService;

    @Inject
    private StatisticsManager statisticsManager;

    @Inject
    private Injector injector;
    
    @Inject
    private TokenManager tokenManager;
    
    @Inject
    private RedisTemplate<String, Object> redisTemplate;
    
    @Inject
    private MeterRegistry meterRegistry;
    
    @Inject
    private Tracer tracer;
    
    @Inject
    private Provider<Config> configProvider;
    
    private final CircuitBreaker authCircuitBreaker;
    
    // Metrics
    private final Counter authAttemptCounter;
    private final Counter authSuccessCounter;
    private final Counter authFailureCounter;
    private final Timer authProcessingTimer;
    
    private final String redisSessionPrefix;
    private final long sessionTimeoutSeconds;
    private final boolean requireSecureConnections;
    private final boolean enableServiceAuth;

    /**
     * Constructs a new SecurityRequestFilter with the required dependencies.
     */
    @Inject
    public SecurityRequestFilter(MeterRegistry meterRegistry, Config config) {
        // Configure circuit breaker for authentication
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        authCircuitBreaker = circuitBreakerRegistry.circuitBreaker("authentication");
        
        // Initialize metrics
        authAttemptCounter = Counter.builder("auth.attempts")
                .description("Number of authentication attempts")
                .register(meterRegistry);
        
        authSuccessCounter = Counter.builder("auth.success")
                .description("Number of successful authentication attempts")
                .register(meterRegistry);
        
        authFailureCounter = Counter.builder("auth.failure")
                .description("Number of failed authentication attempts")
                .register(meterRegistry);
        
        authProcessingTimer = Timer.builder("auth.processing.time")
                .description("Time taken to process authentication")
                .register(meterRegistry);
        
        // Load configuration
        redisSessionPrefix = config.getString("redis.sessionPrefix", "traccar:session:");
        sessionTimeoutSeconds = config.getLong("web.sessionTimeout", 3600L);
        requireSecureConnections = config.getBoolean("web.requireSecureConnections", false);
        enableServiceAuth = config.getBoolean("security.enableServiceAuth", true);
    }

    @Override
    public void filter(ContainerRequestContext requestContext) {
        // Skip OPTIONS requests (for CORS)
        if (requestContext.getMethod().equals("OPTIONS")) {
            return;
        }
        
        // Generate correlation ID for distributed tracing
        String correlationId = requestContext.getHeaderString("X-Correlation-ID");
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = UUID.randomUUID().toString();
            requestContext.getHeaders().putSingle("X-Correlation-ID", correlationId);
        }
        
        // Set up distributed tracing
        Span span = tracer.spanBuilder("authenticate_request")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("correlation.id", correlationId)
                .setAttribute("http.method", requestContext.getMethod())
                .setAttribute("http.path", requestContext.getUriInfo().getPath())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Add correlation ID to logging context
            MDC.put("correlationId", correlationId);
            
            // Record authentication attempt
            authAttemptCounter.increment();
            
            // Process authentication with timing
            authProcessingTimer.record(() -> {
                processAuthentication(requestContext, correlationId, span);
            });
        } catch (Exception e) {
            span.recordException(e);
            throw e;
        } finally {
            span.end();
            MDC.remove("correlationId");
        }
    }
    
    /**
     * Process authentication for the request using various authentication methods.
     * 
     * @param requestContext The container request context
     * @param correlationId The correlation ID for distributed tracing
     * @param span The current tracing span
     */
    private void processAuthentication(ContainerRequestContext requestContext, String correlationId, Span span) {
        SecurityContext securityContext = null;
        
        try {
            // Check if secure connection is required
            if (requireSecureConnections && !request.isSecure()) {
                LOGGER.warn("Insecure connection rejected [correlationId={}]", correlationId);
                span.setAttribute("auth.secure_required", true);
                span.setAttribute("auth.result", "rejected_insecure");
                throw new SecurityException("Secure connection required");
            }
            
            // Try JWT token authentication from Authorization header
            String authHeader = requestContext.getHeaderString("Authorization");
            if (authHeader != null) {
                span.setAttribute("auth.method", "header");
                securityContext = authenticateWithHeader(authHeader, correlationId, span);
            }
            
            // Try JWT token from cookie
            if (securityContext == null && request.getCookies() != null) {
                span.setAttribute("auth.method", "cookie");
                securityContext = authenticateWithCookie(request.getCookies(), correlationId, span);
            }
            
            // Try session-based authentication
            if (securityContext == null && request.getSession() != null) {
                span.setAttribute("auth.method", "session");
                securityContext = authenticateWithSession(correlationId, span);
            }
            
            // Try service-to-service authentication
            if (securityContext == null && enableServiceAuth) {
                String serviceToken = requestContext.getHeaderString("X-Service-Token");
                String serviceName = requestContext.getHeaderString("X-Service-Name");
                if (serviceToken != null && serviceName != null) {
                    span.setAttribute("auth.method", "service");
                    securityContext = authenticateService(serviceToken, serviceName, correlationId, span);
                }
            }
            
            // Set security context if authentication was successful
            if (securityContext != null) {
                requestContext.setSecurityContext(securityContext);
                span.setAttribute("auth.result", "success");
                authSuccessCounter.increment();
            } else {
                // Check if the resource allows unauthenticated access
                Method method = resourceInfo.getResourceMethod();
                if (!method.isAnnotationPresent(PermitAll.class)) {
                    span.setAttribute("auth.result", "unauthorized");
                    authFailureCounter.increment();
                    
                    Response.ResponseBuilder responseBuilder = Response.status(Response.Status.UNAUTHORIZED);
                    String accept = request.getHeader("Accept");
                    if (accept != null && accept.contains("text/html")) {
                        responseBuilder.header("WWW-Authenticate", "Basic realm=\"api\"");
                    }
                    throw new WebApplicationException(responseBuilder.build());
                }
                
                span.setAttribute("auth.result", "permit_all");
            }
        } catch (SecurityException | StorageException e) {
            span.recordException(e);
            span.setAttribute("auth.result", "error");
            authFailureCounter.increment();
            LOGGER.warn("Authentication error [correlationId={}]", correlationId, e);
        }
    }
    
    /**
     * Authenticate using the Authorization header.
     * Supports Bearer (JWT) and Basic authentication schemes.
     * 
     * @param authHeader The Authorization header value
     * @param correlationId The correlation ID for distributed tracing
     * @param span The current tracing span
     * @return The security context if authentication is successful, null otherwise
     * @throws StorageException If there's an error accessing the storage
     */
    private SecurityContext authenticateWithHeader(String authHeader, String correlationId, Span span) 
            throws StorageException {
        try {
            String[] auth = authHeader.split(" ", 2);
            if (auth.length != 2) {
                span.setAttribute("auth.header.valid", false);
                return null;
            }
            
            String scheme = auth[0].toLowerCase();
            String credentials = auth[1];
            
            span.setAttribute("auth.scheme", scheme);
            
            // Use circuit breaker to protect against authentication service failures
            return authCircuitBreaker.executeSupplier(() -> {
                try {
                    LoginResult loginResult = loginService.login(scheme, credentials);
                    if (loginResult != null) {
                        User user = loginResult.getUser();
                        if (user != null) {
                            statisticsManager.registerRequest(user.getId());
                            
                            // Extract claims and scopes from JWT token if available
                            Set<String> roles = new HashSet<>();
                            Set<String> scopes = new HashSet<>();
                            Map<String, Object> claims = null;
                            
                            if (loginResult.getToken() != null) {
                                try {
                                    Claims jwtClaims = Jwts.parserBuilder()
                                            .setSigningKey(tokenManager.getPublicKey())
                                            .build()
                                            .parseClaimsJws(loginResult.getToken())
                                            .getBody();
                                    
                                    claims = jwtClaims;
                                    
                                    // Extract roles
                                    if (jwtClaims.containsKey("role")) {
                                        roles.add(jwtClaims.get("role", String.class));
                                    }
                                    
                                    // Extract scopes
                                    if (jwtClaims.containsKey("scope")) {
                                        String scopeStr = jwtClaims.get("scope", String.class);
                                        if (scopeStr != null) {
                                            scopes.addAll(Arrays.asList(scopeStr.split(" ")));
                                        }
                                    }
                                } catch (JwtException e) {
                                    LOGGER.warn("JWT parsing error [correlationId={}]", correlationId, e);
                                }
                            }
                            
                            // Create user principal with session ID, claims, roles, and scopes
                            String sessionId = UUID.randomUUID().toString();
                            UserPrincipal principal = new UserPrincipal(
                                    user.getId(), 
                                    loginResult.getExpiration(),
                                    sessionId,
                                    claims,
                                    roles,
                                    scopes);
                            
                            // Store session in Redis
                            String sessionKey = redisSessionPrefix + sessionId;
                            redisTemplate.opsForHash().put(sessionKey, "userId", user.getId());
                            redisTemplate.opsForHash().put(sessionKey, "expiration", loginResult.getExpiration());
                            redisTemplate.opsForHash().put(sessionKey, "correlationId", correlationId);
                            redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
                            
                            span.setAttribute("auth.user.id", user.getId());
                            span.setAttribute("auth.session.id", sessionId);
                            
                            return new UserSecurityContext(principal, roles, request.isSecure());
                        }
                    }
                    return null;
                } catch (StorageException | GeneralSecurityException | IOException e) {
                    LOGGER.warn("Login error [correlationId={}]", correlationId, e);
                    span.recordException(e);
                    throw new WebApplicationException(e);
                }
            });
        } catch (Exception e) {
            LOGGER.warn("Authentication circuit breaker triggered [correlationId={}]", correlationId, e);
            span.recordException(e);
            span.setAttribute("auth.circuit_breaker", "open");
            return null;
        }
    }
    
    /**
     * Authenticate using cookies.
     * Looks for JWT token in cookies.
     * 
     * @param cookies The request cookies
     * @param correlationId The correlation ID for distributed tracing
     * @param span The current tracing span
     * @return The security context if authentication is successful, null otherwise
     * @throws StorageException If there's an error accessing the storage
     */
    private SecurityContext authenticateWithCookie(Cookie[] cookies, String correlationId, Span span) 
            throws StorageException {
        Optional<Cookie> tokenCookie = Arrays.stream(cookies)
                .filter(cookie -> "token".equals(cookie.getName()))
                .findFirst();
        
        if (tokenCookie.isPresent()) {
            String token = tokenCookie.get().getValue();
            span.setAttribute("auth.cookie.present", true);
            
            try {
                // Validate JWT token
                TokenManager.TokenData tokenData = tokenManager.verifyToken(token);
                if (tokenData != null) {
                    User user = injector.getInstance(PermissionsService.class).getUser(tokenData.getUserId());
                    if (user != null) {
                        user.checkDisabled();
                        statisticsManager.registerRequest(user.getId());
                        
                        // Extract claims and scopes from JWT token
                        Set<String> roles = new HashSet<>();
                        Set<String> scopes = new HashSet<>();
                        Map<String, Object> claims = null;
                        
                        try {
                            Claims jwtClaims = Jwts.parserBuilder()
                                    .setSigningKey(tokenManager.getPublicKey())
                                    .build()
                                    .parseClaimsJws(token)
                                    .getBody();
                            
                            claims = jwtClaims;
                            
                            // Extract roles
                            if (jwtClaims.containsKey("role")) {
                                roles.add(jwtClaims.get("role", String.class));
                            }
                            
                            // Extract scopes
                            if (jwtClaims.containsKey("scope")) {
                                String scopeStr = jwtClaims.get("scope", String.class);
                                if (scopeStr != null) {
                                    scopes.addAll(Arrays.asList(scopeStr.split(" ")));
                                }
                            }
                        } catch (JwtException e) {
                            LOGGER.warn("JWT parsing error [correlationId={}]", correlationId, e);
                        }
                        
                        // Create user principal with session ID, claims, roles, and scopes
                        String sessionId = UUID.randomUUID().toString();
                        UserPrincipal principal = new UserPrincipal(
                                user.getId(), 
                                tokenData.getExpiration(),
                                sessionId,
                                claims,
                                roles,
                                scopes);
                        
                        // Store session in Redis
                        String sessionKey = redisSessionPrefix + sessionId;
                        redisTemplate.opsForHash().put(sessionKey, "userId", user.getId());
                        redisTemplate.opsForHash().put(sessionKey, "expiration", tokenData.getExpiration());
                        redisTemplate.opsForHash().put(sessionKey, "correlationId", correlationId);
                        redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
                        
                        span.setAttribute("auth.user.id", user.getId());
                        span.setAttribute("auth.session.id", sessionId);
                        
                        return new UserSecurityContext(principal, roles, request.isSecure());
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Cookie authentication error [correlationId={}]", correlationId, e);
                span.recordException(e);
            }
        }
        
        return null;
    }
    
    /**
     * Authenticate using the session.
     * This method is for backward compatibility with the existing session-based authentication.
     * 
     * @param correlationId The correlation ID for distributed tracing
     * @param span The current tracing span
     * @return The security context if authentication is successful, null otherwise
     * @throws StorageException If there's an error accessing the storage
     */
    private SecurityContext authenticateWithSession(String correlationId, Span span) throws StorageException {
        Long userId = (Long) request.getSession().getAttribute(SessionHelper.USER_ID_KEY);
        Date expiration = (Date) request.getSession().getAttribute(SessionHelper.EXPIRATION_KEY);
        
        if (userId != null) {
            // Check if session exists in Redis
            String sessionKey = redisSessionPrefix + userId;
            Boolean sessionExists = redisTemplate.hasKey(sessionKey);
            
            if (sessionExists == null || !sessionExists) {
                // Create session in Redis for backward compatibility
                redisTemplate.opsForHash().put(sessionKey, "userId", userId);
                redisTemplate.opsForHash().put(sessionKey, "expiration", expiration);
                redisTemplate.opsForHash().put(sessionKey, "correlationId", correlationId);
                redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
            } else {
                // Refresh session expiration
                redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
            }
            
            User user = injector.getInstance(PermissionsService.class).getUser(userId);
            if (user != null) {
                user.checkDisabled();
                statisticsManager.registerRequest(userId);
                
                // Create user principal with minimal information
                UserPrincipal principal = new UserPrincipal(userId, expiration);
                
                span.setAttribute("auth.user.id", userId);
                span.setAttribute("auth.session.exists", sessionExists != null && sessionExists);
                
                return new UserSecurityContext(principal, null, request.isSecure());
            }
        }
        
        return null;
    }
    
    /**
     * Authenticate a service-to-service request.
     * 
     * @param serviceToken The service token
     * @param serviceName The name of the calling service
     * @param correlationId The correlation ID for distributed tracing
     * @param span The current tracing span
     * @return The security context if authentication is successful, null otherwise
     */
    private SecurityContext authenticateService(String serviceToken, String serviceName, String correlationId, Span span) {
        if (!enableServiceAuth) {
            LOGGER.warn("Service authentication attempted but disabled [correlationId={}]", correlationId);
            span.setAttribute("auth.service.enabled", false);
            return null;
        }
        
        try {
            // Validate service token
            boolean validToken = tokenManager.verifyServiceToken(serviceToken, serviceName);
            if (!validToken) {
                LOGGER.warn("Invalid service token for service {} [correlationId={}]", serviceName, correlationId);
                span.setAttribute("auth.service.valid", false);
                return null;
            }
            
            // Extract service permissions from token
            Set<String> scopes = tokenManager.getServiceScopes(serviceToken);
            Date expiration = tokenManager.getServiceTokenExpiration(serviceToken);
            
            // Create service principal
            UserPrincipal principal = new UserPrincipal(serviceName, expiration, scopes, correlationId);
            
            span.setAttribute("auth.service.name", serviceName);
            span.setAttribute("auth.service.valid", true);
            
            return new UserSecurityContext(principal, null, request.isSecure(), true);
        } catch (Exception e) {
            LOGGER.warn("Service authentication error for service {} [correlationId={}]", serviceName, correlationId, e);
            span.recordException(e);
            span.setAttribute("auth.service.valid", false);
            return null;
        }
    }
}
