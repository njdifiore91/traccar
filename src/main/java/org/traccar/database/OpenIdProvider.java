/*
 * Copyright 2023 Daniel Raper (me@danr.uk)
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
package org.traccar.database;

import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.api.security.LoginService;
import org.traccar.helper.LogAction;
import org.traccar.model.User;
import org.traccar.storage.StorageException;
import org.traccar.helper.SessionHelper;
import org.traccar.helper.WebHelper;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.io.Closeable;
import java.io.IOException;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.servlet.http.HttpServletRequest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;

import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.AuthorizationCode;
import com.nimbusds.oauth2.sdk.ResponseType;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.AuthorizationGrant;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.AuthorizationCodeGrant;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.AuthorizationResponse;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.id.State;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponse;
import com.nimbusds.openid.connect.sdk.OIDCTokenResponseParser;
import com.nimbusds.openid.connect.sdk.UserInfoResponse;
import com.nimbusds.openid.connect.sdk.UserInfoRequest;
import com.nimbusds.openid.connect.sdk.AuthenticationRequest;
import com.nimbusds.openid.connect.sdk.claims.UserInfo;

// OpenTelemetry imports
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

// Circuit breaker imports
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;

@Singleton
public class OpenIdProvider implements Closeable {
    private final Boolean force;
    private final ClientID clientId;
    private final ClientAuthentication clientAuth;
    private final URI callbackUrl;
    private final URI authUrl;
    private final URI tokenUrl;
    private final URI userInfoUrl;
    private final URI baseUrl;
    private final String adminGroup;
    private final String allowGroup;
    private final String groupsClaimName;
    private final int maxRetryAttempts;
    private final long initialBackoffMillis;
    private final long maxBackoffMillis;
    private final double backoffMultiplier;
    private final double jitterFactor;
    
    private final LoginService loginService;
    private final LogAction actionLogger;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final Random random;
    
    // Circuit breaker for OpenID operations
    private final CircuitBreaker tokenCircuitBreaker;
    private final CircuitBreaker userInfoCircuitBreaker;
    
    // Retry handlers for OpenID operations
    private final Retry tokenRetry;
    private final Retry userInfoRetry;
    
    // OpenTelemetry components
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter authAttemptCounter;
    private final LongCounter authSuccessCounter;
    private final LongCounter authFailureCounter;
    private final LongCounter circuitBreakerOpenCounter;

    @Inject
    public OpenIdProvider(
            Config config, LoginService loginService, LogAction actionLogger,
            HttpClient httpClient, ObjectMapper objectMapper)
        throws InterruptedException, IOException, URISyntaxException {

        this.loginService = loginService;
        this.actionLogger = actionLogger;
        this.httpClient = httpClient;
        this.objectMapper = objectMapper;
        this.random = new Random();

        // Basic OpenID configuration
        force = config.getBoolean(Keys.OPENID_FORCE);
        clientId = new ClientID(config.getString(Keys.OPENID_CLIENT_ID));
        clientAuth = new ClientSecretBasic(clientId, new Secret(config.getString(Keys.OPENID_CLIENT_SECRET)));

        baseUrl = new URI(WebHelper.retrieveWebUrl(config));
        callbackUrl = new URI(WebHelper.retrieveWebUrl(config) + "/api/session/openid/callback");
        
        // Retry configuration
        maxRetryAttempts = config.getInteger(Keys.OPENID_MAX_RETRY_ATTEMPTS, 3);
        initialBackoffMillis = config.getLong(Keys.OPENID_INITIAL_BACKOFF_MILLIS, 1000);
        maxBackoffMillis = config.getLong(Keys.OPENID_MAX_BACKOFF_MILLIS, 30000);
        backoffMultiplier = config.getDouble(Keys.OPENID_BACKOFF_MULTIPLIER, 2.0);
        jitterFactor = config.getDouble(Keys.OPENID_JITTER_FACTOR, 0.2);
        
        // Initialize OpenTelemetry
        tracer = GlobalOpenTelemetry.getTracer("org.traccar.database.OpenIdProvider");
        meter = GlobalOpenTelemetry.getMeter("org.traccar.database.OpenIdProvider");
        
        // Create metrics
        authAttemptCounter = meter.counterBuilder("openid.auth.attempts")
                .setDescription("Number of OpenID authentication attempts")
                .build();
        
        authSuccessCounter = meter.counterBuilder("openid.auth.success")
                .setDescription("Number of successful OpenID authentications")
                .build();
        
        authFailureCounter = meter.counterBuilder("openid.auth.failures")
                .setDescription("Number of failed OpenID authentications")
                .build();
        
        circuitBreakerOpenCounter = meter.counterBuilder("openid.circuit_breaker.open")
                .setDescription("Number of times the OpenID circuit breaker opened")
                .build();
        
        // Configure circuit breakers
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(10000))
                .permittedNumberOfCallsInHalfOpenState(2)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        tokenCircuitBreaker = circuitBreakerRegistry.circuitBreaker("openid-token");
        userInfoCircuitBreaker = circuitBreakerRegistry.circuitBreaker("openid-userinfo");
        
        // Add circuit breaker state transition listeners
        tokenCircuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                        circuitBreakerOpenCounter.add(1);
                    }
                });
        
        userInfoCircuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    if (event.getStateTransition() == CircuitBreaker.StateTransition.CLOSED_TO_OPEN) {
                        circuitBreakerOpenCounter.add(1);
                    }
                });
        
        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(maxRetryAttempts)
                .waitDuration(Duration.ofMillis(initialBackoffMillis))
                .retryExceptions(IOException.class, ParseException.class)
                .retryOnResult(response -> response == null)
                .intervalFunction(interval -> {
                    // Calculate exponential backoff with jitter
                    long exponentialBackoff = Math.min(
                            maxBackoffMillis,
                            (long) (initialBackoffMillis * Math.pow(backoffMultiplier, interval - 1)));
                    
                    // Add jitter to prevent thundering herd
                    long jitter = (long) (exponentialBackoff * jitterFactor * (random.nextDouble() * 2 - 1));
                    return exponentialBackoff + jitter;
                })
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        tokenRetry = retryRegistry.retry("openid-token-retry");
        userInfoRetry = retryRegistry.retry("openid-userinfo-retry");
        
        // Discover OpenID endpoints
        Span span = tracer.spanBuilder("openid.discover")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("openid.issuer", config.hasKey(Keys.OPENID_ISSUER_URL) ? 
                    config.getString(Keys.OPENID_ISSUER_URL) : "manual_configuration");
            
            if (config.hasKey(Keys.OPENID_ISSUER_URL)) {
                HttpRequest httpRequest = HttpRequest.newBuilder(
                    URI.create(config.getString(Keys.OPENID_ISSUER_URL) + "/.well-known/openid-configuration"))
                    .header("Accept", "application/json")
                    .timeout(Duration.ofSeconds(10))
                    .build();

                String httpResponse = httpClient.send(httpRequest, BodyHandlers.ofString()).body();

                Map<String, Object> discoveryMap = objectMapper.readValue(httpResponse, new TypeReference<>() {
                });

                authUrl = new URI((String) discoveryMap.get("authorization_endpoint"));
                tokenUrl = new URI((String) discoveryMap.get("token_endpoint"));
                userInfoUrl = new URI((String) discoveryMap.get("userinfo_endpoint"));
                
                span.setAttribute("openid.discovery.success", true);
                span.setAttribute("openid.auth_url", authUrl.toString());
                span.setAttribute("openid.token_url", tokenUrl.toString());
                span.setAttribute("openid.userinfo_url", userInfoUrl.toString());
            } else {
                authUrl = new URI(config.getString(Keys.OPENID_AUTH_URL));
                tokenUrl = new URI(config.getString(Keys.OPENID_TOKEN_URL));
                userInfoUrl = new URI(config.getString(Keys.OPENID_USERINFO_URL));
                
                span.setAttribute("openid.manual_config", true);
                span.setAttribute("openid.auth_url", authUrl.toString());
                span.setAttribute("openid.token_url", tokenUrl.toString());
                span.setAttribute("openid.userinfo_url", userInfoUrl.toString());
            }
            
            adminGroup = config.getString(Keys.OPENID_ADMIN_GROUP);
            allowGroup = config.getString(Keys.OPENID_ALLOW_GROUP);
            groupsClaimName = config.getString(Keys.OPENID_GROUPS_CLAIM_NAME);
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    @PostConstruct
    public void init() {
        // Lifecycle hook for container environments
        Span span = tracer.spanBuilder("openid.init").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Perform any initialization if needed
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
    
    @PreDestroy
    public void destroy() {
        // Lifecycle hook for container environments
        Span span = tracer.spanBuilder("openid.destroy").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Perform any cleanup if needed
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
    
    @Override
    public void close() {
        // Resource cleanup
        destroy();
    }

    public URI createAuthUri() {
        Span span = tracer.spanBuilder("openid.create_auth_uri")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Scope openIdScope = new Scope("openid", "profile", "email");

            if (adminGroup != null) {
                openIdScope.add(groupsClaimName);
                span.setAttribute("openid.scope.groups_claim", groupsClaimName);
            }
            
            span.setAttribute("openid.scope", openIdScope.toString());
            span.setAttribute("openid.auth_url", authUrl.toString());
            span.setAttribute("openid.callback_url", callbackUrl.toString());

            AuthenticationRequest.Builder request = new AuthenticationRequest.Builder(
                    new ResponseType("code"),
                    openIdScope,
                    clientId,
                    callbackUrl);

            URI result = request.endpointURI(authUrl)
                    .state(new State())
                    .build()
                    .toURI();
            
            span.setAttribute("openid.auth_uri", result.toString());
            span.setStatus(StatusCode.OK);
            return result;
        } finally {
            span.end();
        }
    }

    private OIDCTokenResponse getToken(AuthorizationCode code)
            throws IOException, ParseException, GeneralSecurityException {
        Span span = tracer.spanBuilder("openid.get_token")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("openid.token_url", tokenUrl.toString());
            span.setAttribute("openid.callback_url", callbackUrl.toString());
            
            // Use circuit breaker and retry pattern with the token endpoint
            return tokenCircuitBreaker.executeSupplier(() -> {
                return Retry.decorateSupplier(tokenRetry, () -> {
                    try {
                        AuthorizationGrant codeGrant = new AuthorizationCodeGrant(code, callbackUrl);
                        TokenRequest tokenRequest = new TokenRequest(tokenUrl, clientAuth, codeGrant);
                        
                        // Record metrics for token request
                        long startTime = System.currentTimeMillis();
                        HTTPResponse tokenResponse = tokenRequest.toHTTPRequest().send();
                        long endTime = System.currentTimeMillis();
                        
                        // Record latency as a metric attribute
                        Attributes attributes = Attributes.of(
                                AttributeKey.stringKey("operation"), "token_request",
                                AttributeKey.longKey("duration_ms"), (endTime - startTime));
                        meter.gaugeBuilder("openid.operation.latency")
                                .setDescription("Latency of OpenID operations")
                                .setUnit("ms")
                                .buildWithCallback(measurement -> 
                                        measurement.record((endTime - startTime), attributes));
                        
                        TokenResponse token = OIDCTokenResponseParser.parse(tokenResponse);
                        if (!token.indicatesSuccess()) {
                            String errorMessage = "Unable to authenticate with the OpenID Connect provider.";
                            span.setAttribute("error", true);
                            span.setAttribute("error.message", errorMessage);
                            span.setStatus(StatusCode.ERROR, errorMessage);
                            authFailureCounter.add(1);
                            throw new GeneralSecurityException(errorMessage);
                        }
                        
                        span.setAttribute("success", true);
                        return (OIDCTokenResponse) token.toSuccessResponse();
                    } catch (Exception e) {
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        throw new RuntimeException(e);
                    }
                }).get();
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            if (e instanceof IOException) {
                throw (IOException) e;
            } else if (e instanceof ParseException) {
                throw (ParseException) e;
            } else if (e instanceof GeneralSecurityException) {
                throw (GeneralSecurityException) e;
            } else {
                throw new GeneralSecurityException("Error getting token: " + e.getMessage(), e);
            }
        } finally {
            span.end();
        }
    }

    private UserInfo getUserInfo(BearerAccessToken token) throws IOException, ParseException, GeneralSecurityException {
        Span span = tracer.spanBuilder("openid.get_user_info")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("openid.userinfo_url", userInfoUrl.toString());
            
            // Use circuit breaker and retry pattern with the userinfo endpoint
            return userInfoCircuitBreaker.executeSupplier(() -> {
                return Retry.decorateSupplier(userInfoRetry, () -> {
                    try {
                        // Record metrics for userinfo request
                        long startTime = System.currentTimeMillis();
                        HTTPResponse httpResponse = new UserInfoRequest(userInfoUrl, token)
                                .toHTTPRequest()
                                .send();
                        long endTime = System.currentTimeMillis();
                        
                        // Record latency as a metric attribute
                        Attributes attributes = Attributes.of(
                                AttributeKey.stringKey("operation"), "userinfo_request",
                                AttributeKey.longKey("duration_ms"), (endTime - startTime));
                        meter.gaugeBuilder("openid.operation.latency")
                                .setDescription("Latency of OpenID operations")
                                .setUnit("ms")
                                .buildWithCallback(measurement -> 
                                        measurement.record((endTime - startTime), attributes));
                        
                        UserInfoResponse userInfoResponse = UserInfoResponse.parse(httpResponse);

                        if (!userInfoResponse.indicatesSuccess()) {
                            String errorMessage = "Failed to access OpenID Connect user info endpoint. Please contact your administrator.";
                            span.setAttribute("error", true);
                            span.setAttribute("error.message", errorMessage);
                            span.setStatus(StatusCode.ERROR, errorMessage);
                            authFailureCounter.add(1);
                            throw new GeneralSecurityException(errorMessage);
                        }
                        
                        UserInfo userInfo = userInfoResponse.toSuccessResponse().getUserInfo();
                        span.setAttribute("openid.user.email", userInfo.getEmailAddress());
                        span.setAttribute("openid.user.name", userInfo.getName());
                        span.setAttribute("success", true);
                        return userInfo;
                    } catch (Exception e) {
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        throw new RuntimeException(e);
                    }
                }).get();
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            if (e instanceof IOException) {
                throw (IOException) e;
            } else if (e instanceof ParseException) {
                throw (ParseException) e;
            } else if (e instanceof GeneralSecurityException) {
                throw (GeneralSecurityException) e;
            } else {
                throw new GeneralSecurityException("Error getting user info: " + e.getMessage(), e);
            }
        } finally {
            span.end();
        }
    }

    public URI handleCallback(URI requestUri, HttpServletRequest request)
            throws StorageException, ParseException, IOException, GeneralSecurityException {
        
        Span parentSpan = tracer.spanBuilder("openid.handle_callback")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        try (Scope parentScope = parentSpan.makeCurrent()) {
            // Record authentication attempt
            authAttemptCounter.add(1);
            parentSpan.setAttribute("openid.request_uri", requestUri.toString());
            
            AuthorizationResponse response = AuthorizationResponse.parse(requestUri);

            if (!response.indicatesSuccess()) {
                String errorMessage = response.toErrorResponse().getErrorObject().getDescription();
                parentSpan.setAttribute("error", true);
                parentSpan.setAttribute("error.message", errorMessage);
                parentSpan.setStatus(StatusCode.ERROR, errorMessage);
                authFailureCounter.add(1);
                throw new GeneralSecurityException(errorMessage);
            }

            AuthorizationCode authCode = response.toSuccessResponse().getAuthorizationCode();

            if (authCode == null) {
                String errorMessage = "Malformed OpenID callback.";
                parentSpan.setAttribute("error", true);
                parentSpan.setAttribute("error.message", errorMessage);
                parentSpan.setStatus(StatusCode.ERROR, errorMessage);
                authFailureCounter.add(1);
                throw new GeneralSecurityException(errorMessage);
            }
            
            parentSpan.setAttribute("openid.auth_code_received", true);
            
            // Get token asynchronously with timeout
            CompletableFuture<OIDCTokenResponse> tokenFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return getToken(authCode);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            
            OIDCTokenResponse tokens;
            try {
                tokens = tokenFuture.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                String errorMessage = "Failed to get token: " + e.getMessage();
                parentSpan.setAttribute("error", true);
                parentSpan.setAttribute("error.message", errorMessage);
                parentSpan.setStatus(StatusCode.ERROR, errorMessage);
                authFailureCounter.add(1);
                throw new GeneralSecurityException(errorMessage, e);
            }
            
            parentSpan.setAttribute("openid.token_received", true);
            BearerAccessToken bearerToken = tokens.getOIDCTokens().getBearerAccessToken();
            
            // Get user info asynchronously with timeout
            CompletableFuture<UserInfo> userInfoFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    return getUserInfo(bearerToken);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            
            UserInfo userInfo;
            try {
                userInfo = userInfoFuture.get(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                String errorMessage = "Failed to get user info: " + e.getMessage();
                parentSpan.setAttribute("error", true);
                parentSpan.setAttribute("error.message", errorMessage);
                parentSpan.setStatus(StatusCode.ERROR, errorMessage);
                authFailureCounter.add(1);
                throw new GeneralSecurityException(errorMessage, e);
            }
            
            parentSpan.setAttribute("openid.userinfo_received", true);
            parentSpan.setAttribute("openid.user.email", userInfo.getEmailAddress());
            parentSpan.setAttribute("openid.user.name", userInfo.getName());

            List<String> userGroups = userInfo.getStringListClaim(groupsClaimName);
            boolean administrator = adminGroup != null && userGroups.contains(adminGroup);
            
            parentSpan.setAttribute("openid.user.is_admin", administrator);
            parentSpan.setAttribute("openid.user.groups", String.join(",", userGroups));

            if (!(administrator || allowGroup == null || userGroups.contains(allowGroup))) {
                String errorMessage = "Your OpenID Groups do not permit access to Traccar.";
                parentSpan.setAttribute("error", true);
                parentSpan.setAttribute("error.message", errorMessage);
                parentSpan.setAttribute("openid.access_denied", true);
                parentSpan.setStatus(StatusCode.ERROR, errorMessage);
                authFailureCounter.add(1);
                throw new GeneralSecurityException(errorMessage);
            }
            
            // Create a child span for login operation
            Span loginSpan = tracer.spanBuilder("openid.login_user")
                    .setParent(Context.current().with(parentSpan))
                    .startSpan();
            
            try (Scope loginScope = loginSpan.makeCurrent()) {
                loginSpan.setAttribute("openid.user.email", userInfo.getEmailAddress());
                loginSpan.setAttribute("openid.user.name", userInfo.getName());
                loginSpan.setAttribute("openid.user.is_admin", administrator);
                
                User user = loginService.login(
                        userInfo.getEmailAddress(), userInfo.getName(), administrator).getUser();
                
                loginSpan.setAttribute("openid.user.id", user.getId());
                loginSpan.setStatus(StatusCode.OK);
                
                // Create a child span for session creation
                Span sessionSpan = tracer.spanBuilder("openid.create_session")
                        .setParent(Context.current().with(loginSpan))
                        .startSpan();
                
                try (Scope sessionScope = sessionSpan.makeCurrent()) {
                    sessionSpan.setAttribute("openid.user.id", user.getId());
                    SessionHelper.userLogin(actionLogger, request, user, null);
                    sessionSpan.setStatus(StatusCode.OK);
                } finally {
                    sessionSpan.end();
                }
            } finally {
                loginSpan.end();
            }
            
            // Record successful authentication
            authSuccessCounter.add(1);
            parentSpan.setAttribute("success", true);
            parentSpan.setStatus(StatusCode.OK);
            
            return baseUrl.resolve("?openid=success");
        } finally {
            parentSpan.end();
        }
    }

    public boolean getForce() {
        return force;
    }
    
    /**
     * Checks if the OpenID provider is healthy and available.
     * This method is used by health checks in containerized environments.
     * 
     * @return true if the provider is healthy, false otherwise
     */
    public boolean isHealthy() {
        boolean tokenCircuitBreakerClosed = tokenCircuitBreaker.getState() != CircuitBreaker.State.OPEN;
        boolean userInfoCircuitBreakerClosed = userInfoCircuitBreaker.getState() != CircuitBreaker.State.OPEN;
        
        // Record health status as a metric
        meter.gaugeBuilder("openid.health")
                .setDescription("Health status of OpenID provider (1=healthy, 0=unhealthy)")
                .buildWithCallback(measurement -> 
                        measurement.record(tokenCircuitBreakerClosed && userInfoCircuitBreakerClosed ? 1 : 0));
        
        return tokenCircuitBreakerClosed && userInfoCircuitBreakerClosed;
    }
    
    /**
     * Gets the current state of the token circuit breaker.
     * 
     * @return the state of the token circuit breaker
     */
    public CircuitBreaker.State getTokenCircuitBreakerState() {
        return tokenCircuitBreaker.getState();
    }
    
    /**
     * Gets the current state of the user info circuit breaker.
     * 
     * @return the state of the user info circuit breaker
     */
    public CircuitBreaker.State getUserInfoCircuitBreakerState() {
        return userInfoCircuitBreaker.getState();
    }
}