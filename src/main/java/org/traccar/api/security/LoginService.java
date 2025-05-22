/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.warrenstrange.googleauth.GoogleAuthenticator;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisTemplate;
import org.traccar.api.signature.TokenManager;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.database.LdapProvider;
import org.traccar.helper.DataConverter;
import org.traccar.helper.model.UserUtil;
import org.traccar.model.User;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.annotation.Nullable;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Service responsible for user authentication and session management.
 * Supports multiple authentication methods and integrates with distributed Redis session store.
 * Implements circuit breaker pattern for external identity provider calls.
 */
@Singleton
public class LoginService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoginService.class);
    
    private final Config config;
    private final Storage storage;
    private final TokenManager tokenManager;
    private final LdapProvider ldapProvider;
    private final RedisTemplate<String, Object> redisTemplate;
    private final CircuitBreaker identityProviderCircuitBreaker;
    private final MeterRegistry meterRegistry;
    private final KeyManagementService keyManagementService;

    private final String serviceAccountToken;
    private final boolean forceLdap;
    private final boolean forceOpenId;
    private final long sessionTimeoutSeconds;
    private final String redisSessionPrefix;

    // Metrics
    private final Counter authSuccessCounter;
    private final Counter authFailureCounter;
    private final Timer authTimer;

    /**
     * Constructs a new LoginService with the required dependencies.
     *
     * @param config Configuration provider
     * @param storage Storage for user data
     * @param tokenManager Token manager for JWT validation
     * @param ldapProvider LDAP authentication provider (optional)
     * @param redisTemplate Redis template for distributed session storage
     * @param meterRegistry Metrics registry for instrumentation
     * @param keyManagementService Service for JWT key management
     */
    @Inject
    public LoginService(
            Config config, 
            Storage storage, 
            TokenManager tokenManager, 
            @Nullable LdapProvider ldapProvider,
            RedisTemplate<String, Object> redisTemplate,
            MeterRegistry meterRegistry,
            KeyManagementService keyManagementService) {
        this.config = config;
        this.storage = storage;
        this.tokenManager = tokenManager;
        this.ldapProvider = ldapProvider;
        this.redisTemplate = redisTemplate;
        this.meterRegistry = meterRegistry;
        this.keyManagementService = keyManagementService;
        
        // Load configuration
        serviceAccountToken = config.getString(Keys.WEB_SERVICE_ACCOUNT_TOKEN);
        forceLdap = config.getBoolean(Keys.LDAP_FORCE);
        forceOpenId = config.getBoolean(Keys.OPENID_FORCE);
        sessionTimeoutSeconds = config.getLong("web.sessionTimeout", 3600L);
        redisSessionPrefix = config.getString("redis.sessionPrefix", "traccar:session:");
        
        // Configure circuit breaker for identity provider calls
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        identityProviderCircuitBreaker = circuitBreakerRegistry.circuitBreaker("identityProvider");
        
        // Initialize metrics
        authSuccessCounter = Counter.builder("auth.success")
                .description("Number of successful authentication attempts")
                .register(meterRegistry);
        
        authFailureCounter = Counter.builder("auth.failure")
                .description("Number of failed authentication attempts")
                .register(meterRegistry);
        
        authTimer = Timer.builder("auth.time")
                .description("Authentication request timing")
                .register(meterRegistry);
    }

    /**
     * Authenticate a user based on the provided authorization scheme and credentials.
     *
     * @param scheme The authorization scheme (e.g., "bearer", "basic")
     * @param credentials The credentials for authentication
     * @return LoginResult containing the authenticated user and session information
     * @throws StorageException If there's an error accessing the storage
     * @throws GeneralSecurityException If there's a security-related error
     * @throws IOException If there's an I/O error
     */
    public LoginResult login(
            String scheme, String credentials) throws StorageException, GeneralSecurityException, IOException {
        return authTimer.record(() -> {
            try {
                LoginResult result;
                String correlationId = UUID.randomUUID().toString();
                LOGGER.debug("Authentication attempt [correlationId={}] using scheme: {}", correlationId, scheme);
                
                switch (scheme.toLowerCase()) {
                    case "bearer":
                        result = login(credentials);
                        break;
                    case "basic":
                        byte[] decodedBytes = DataConverter.parseBase64(credentials);
                        String[] auth = new String(decodedBytes, StandardCharsets.US_ASCII).split(":", 2);
                        result = login(auth[0], auth[1], null);
                        break;
                    case "oauth2":
                        result = loginWithOAuth2(credentials);
                        break;
                    default:
                        LOGGER.warn("Unsupported authorization scheme: {} [correlationId={}]", scheme, correlationId);
                        authFailureCounter.increment();
                        throw new SecurityException("Unsupported authorization scheme");
                }
                
                if (result != null) {
                    authSuccessCounter.increment();
                    LOGGER.info("Authentication successful [correlationId={}] for user: {}", 
                            correlationId, result.getUser() != null ? result.getUser().getEmail() : "service-account");
                } else {
                    authFailureCounter.increment();
                    LOGGER.warn("Authentication failed [correlationId={}]", correlationId);
                }
                
                return result;
            } catch (Exception e) {
                authFailureCounter.increment();
                throw e;
            }
        });
    }

    /**
     * Authenticate a user with a JWT token.
     *
     * @param token The JWT token
     * @return LoginResult containing the authenticated user and session information
     * @throws StorageException If there's an error accessing the storage
     * @throws GeneralSecurityException If there's a security-related error
     * @throws IOException If there's an I/O error
     */
    public LoginResult login(String token) throws StorageException, GeneralSecurityException, IOException {
        if (serviceAccountToken != null && serviceAccountToken.equals(token)) {
            ServiceAccountUser serviceAccount = new ServiceAccountUser();
            String jwtToken = generateJwtToken(serviceAccount, true);
            return new LoginResult(serviceAccount, jwtToken);
        }
        
        TokenManager.TokenData tokenData = tokenManager.verifyToken(token);
        User user = storage.getObject(User.class, new Request(
                new Columns.All(), new Condition.Equals("id", tokenData.getUserId())));
        
        if (user != null) {
            checkUserEnabled(user);
            // Check if session exists in Redis
            String sessionKey = redisSessionPrefix + user.getId();
            Boolean sessionExists = redisTemplate.hasKey(sessionKey);
            
            if (sessionExists != null && sessionExists) {
                // Refresh session expiration
                redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
            } else {
                // Create new session in Redis
                Map<String, Object> sessionData = new HashMap<>();
                sessionData.put("userId", user.getId());
                sessionData.put("email", user.getEmail());
                sessionData.put("createdAt", Instant.now().toString());
                redisTemplate.opsForHash().putAll(sessionKey, sessionData);
                redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
            }
        }
        
        return new LoginResult(user, tokenData.getExpiration());
    }

    /**
     * Authenticate a user with email/login and password, with optional 2FA code.
     *
     * @param email The user's email or login
     * @param password The user's password
     * @param code The 2FA code (if enabled)
     * @return LoginResult containing the authenticated user and session information, or null if authentication fails
     * @throws StorageException If there's an error accessing the storage
     */
    public LoginResult login(String email, String password, Integer code) throws StorageException {
        if (forceOpenId) {
            return null;
        }

        email = email.trim();
        User user = storage.getObject(User.class, new Request(
                new Columns.All(),
                new Condition.Or(
                        new Condition.Equals("email", email),
                        new Condition.Equals("login", email))));
        
        if (user != null) {
            if (ldapProvider != null && user.getLogin() != null) {
                // Use circuit breaker for LDAP authentication
                boolean ldapAuthenticated = executeWithCircuitBreaker(
                        () -> ldapProvider.login(user.getLogin(), password),
                        false);
                
                if (ldapAuthenticated || (!forceLdap && user.isPasswordValid(password))) {
                    checkUserCode(user, code);
                    checkUserEnabled(user);
                    
                    // Create session in Redis
                    String sessionKey = redisSessionPrefix + user.getId();
                    Map<String, Object> sessionData = new HashMap<>();
                    sessionData.put("userId", user.getId());
                    sessionData.put("email", user.getEmail());
                    sessionData.put("createdAt", Instant.now().toString());
                    redisTemplate.opsForHash().putAll(sessionKey, sessionData);
                    redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
                    
                    // Generate JWT token with claims and scopes
                    String jwtToken = generateJwtToken(user, false);
                    return new LoginResult(user, jwtToken);
                }
            }
        } else {
            if (ldapProvider != null) {
                // Use circuit breaker for LDAP authentication and user retrieval
                boolean ldapAuthenticated = executeWithCircuitBreaker(
                        () -> ldapProvider.login(email, password),
                        false);
                
                if (ldapAuthenticated) {
                    User ldapUser = executeWithCircuitBreaker(
                            () -> ldapProvider.getUser(email),
                            null);
                    
                    if (ldapUser != null) {
                        ldapUser.setId(storage.addObject(ldapUser, new Request(new Columns.Exclude("id"))));
                        checkUserEnabled(ldapUser);
                        
                        // Create session in Redis
                        String sessionKey = redisSessionPrefix + ldapUser.getId();
                        Map<String, Object> sessionData = new HashMap<>();
                        sessionData.put("userId", ldapUser.getId());
                        sessionData.put("email", ldapUser.getEmail());
                        sessionData.put("createdAt", Instant.now().toString());
                        redisTemplate.opsForHash().putAll(sessionKey, sessionData);
                        redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
                        
                        // Generate JWT token with claims and scopes
                        String jwtToken = generateJwtToken(ldapUser, false);
                        return new LoginResult(ldapUser, jwtToken);
                    }
                }
            }
        }
        return null;
    }

    /**
     * Authenticate or create a user with email and name, typically used for SSO.
     *
     * @param email The user's email
     * @param name The user's name
     * @param administrator Whether the user should have administrator privileges
     * @return LoginResult containing the authenticated user and session information
     * @throws StorageException If there's an error accessing the storage
     */
    public LoginResult login(String email, String name, boolean administrator) throws StorageException {
        User user = storage.getObject(User.class, new Request(
            new Columns.All(),
            new Condition.Equals("email", email)));

        if (user == null) {
            user = new User();
            UserUtil.setUserDefaults(user, config);
            user.setName(name);
            user.setEmail(email);
            user.setFixedEmail(true);
            user.setAdministrator(administrator);
            user.setId(storage.addObject(user, new Request(new Columns.Exclude("id"))));
        }
        checkUserEnabled(user);
        
        // Create session in Redis
        String sessionKey = redisSessionPrefix + user.getId();
        Map<String, Object> sessionData = new HashMap<>();
        sessionData.put("userId", user.getId());
        sessionData.put("email", user.getEmail());
        sessionData.put("createdAt", Instant.now().toString());
        redisTemplate.opsForHash().putAll(sessionKey, sessionData);
        redisTemplate.expire(sessionKey, sessionTimeoutSeconds, TimeUnit.SECONDS);
        
        // Generate JWT token with claims and scopes
        String jwtToken = generateJwtToken(user, false);
        return new LoginResult(user, jwtToken);
    }
    
    /**
     * Authenticate a user with an OAuth2 token.
     *
     * @param token The OAuth2 token
     * @return LoginResult containing the authenticated user and session information
     * @throws StorageException If there's an error accessing the storage
     */
    public LoginResult loginWithOAuth2(String token) throws StorageException {
        // Implementation would depend on the OAuth2 provider integration
        // This is a placeholder for the OAuth2 authentication flow
        LOGGER.info("OAuth2 authentication attempt with token: {}", token.substring(0, 10) + "...");
        
        // In a real implementation, this would validate the token with the OAuth2 provider
        // and extract user information from the token or from the provider's API
        
        return null; // Placeholder return
    }
    
    /**
     * Invalidate a user's session.
     *
     * @param userId The ID of the user whose session should be invalidated
     * @return true if the session was found and invalidated, false otherwise
     */
    public boolean logout(long userId) {
        String sessionKey = redisSessionPrefix + userId;
        Boolean sessionExists = redisTemplate.hasKey(sessionKey);
        
        if (sessionExists != null && sessionExists) {
            redisTemplate.delete(sessionKey);
            LOGGER.info("Session invalidated for user ID: {}", userId);
            return true;
        }
        
        return false;
    }

    /**
     * Check if a user is enabled and can authenticate.
     *
     * @param user The user to check
     * @throws SecurityException If the user is null or disabled
     */
    private void checkUserEnabled(User user) throws SecurityException {
        if (user == null) {
            throw new SecurityException("Unknown account");
        }
        user.checkDisabled();
    }

    /**
     * Check if a user's 2FA code is valid.
     *
     * @param user The user to check
     * @param code The 2FA code provided
     * @throws SecurityException If 2FA is enabled but no code is provided, or if the code is invalid
     */
    private void checkUserCode(User user, Integer code) throws SecurityException {
        String key = user.getTotpKey();
        if (key != null && !key.isEmpty()) {
            if (code == null) {
                throw new CodeRequiredException();
            }
            GoogleAuthenticator authenticator = new GoogleAuthenticator();
            if (!authenticator.authorize(key, code)) {
                throw new SecurityException("User authorization failed");
            }
        }
    }
    
    /**
     * Generate a JWT token for a user with appropriate claims and scopes.
     *
     * @param user The user for whom to generate the token
     * @param isServiceAccount Whether this is a service account token
     * @return The generated JWT token
     */
    private String generateJwtToken(User user, boolean isServiceAccount) {
        // Get signing key from key management service
        Key signingKey = keyManagementService.getSigningKey();
        
        // Set token expiration time
        long expirationSeconds = config.getLong("jwt.expirationSeconds", 3600L);
        Date now = new Date();
        Date expiration = new Date(now.getTime() + TimeUnit.SECONDS.toMillis(expirationSeconds));
        
        // Build claims
        Claims claims = Jwts.claims();
        claims.setSubject(String.valueOf(user.getId()));
        claims.setIssuedAt(now);
        claims.setExpiration(expiration);
        claims.setIssuer(config.getString("jwt.issuer", "traccar"));
        
        // Add user-specific claims
        claims.put("email", user.getEmail());
        claims.put("name", user.getName());
        
        // Add roles and permissions as scopes
        if (user.getAdministrator()) {
            claims.put("role", "admin");
            claims.put("scope", "admin:read admin:write user:read user:write device:read device:write");
        } else if (user.getUserLimit() != 0) {
            claims.put("role", "manager");
            claims.put("scope", "user:read user:write device:read device:write");
        } else {
            claims.put("role", "user");
            claims.put("scope", "device:read");
        }
        
        // Add service account specific claims if applicable
        if (isServiceAccount) {
            claims.put("type", "service_account");
            claims.put("scope", "service:full");
        }
        
        // Generate and sign the token
        return Jwts.builder()
                .setClaims(claims)
                .signWith(signingKey, SignatureAlgorithm.HS256)
                .compact();
    }
    
    /**
     * Execute a function with circuit breaker protection.
     *
     * @param supplier The function to execute
     * @param fallbackValue The fallback value to return if the circuit breaker is open
     * @param <T> The return type of the function
     * @return The result of the function or the fallback value
     */
    private <T> T executeWithCircuitBreaker(Supplier<T> supplier, T fallbackValue) {
        try {
            return identityProviderCircuitBreaker.executeSupplier(supplier);
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker prevented identity provider call or call failed", e);
            return fallbackValue;
        }
    }
    
    /**
     * Interface for key management service that provides signing keys for JWT tokens.
     */
    public interface KeyManagementService {
        /**
         * Get the current signing key for JWT tokens.
         *
         * @return The signing key
         */
        Key getSigningKey();
    }
    
    /**
     * Default implementation of KeyManagementService that uses a local key.
     * In a production environment, this would be replaced with a service that
     * integrates with a secure key management system.
     */
    @Singleton
    public static class DefaultKeyManagementService implements KeyManagementService {
        private final Key signingKey;
        
        @Inject
        public DefaultKeyManagementService(Config config) {
            String secretKey = config.getString("jwt.secret", 
                    // Default to a secure random key if not configured
                    Keys.secretKeyFor(SignatureAlgorithm.HS256).toString());
            signingKey = Keys.hmacShaKeyFor(secretKey.getBytes(StandardCharsets.UTF_8));
        }
        
        @Override
        public Key getSigningKey() {
            return signingKey;
        }
    }
}