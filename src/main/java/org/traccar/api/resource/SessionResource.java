/*
 * Copyright 2015 - 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.api.resource;

import org.traccar.api.BaseResource;
import org.traccar.api.security.CodeRequiredException;
import org.traccar.api.security.LoginResult;
import org.traccar.api.security.LoginService;
import org.traccar.api.signature.TokenManager;
import org.traccar.database.OpenIdProvider;
import org.traccar.helper.LogAction;
import org.traccar.helper.SessionHelper;
import org.traccar.model.User;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

// Service discovery imports
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.health.ServiceHealth;

// Circuit breaker imports
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

// OpenTelemetry imports
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

// Metrics imports
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.annotation.Timed;

import com.nimbusds.oauth2.sdk.ParseException;
import jakarta.annotation.Nullable;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.net.URI;

@Path("session")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
public class SessionResource extends BaseResource {

    private static final String CIRCUIT_BREAKER_NAME = "sessionResourceBreaker";
    private static final String OPENID_SERVICE_NAME = "openid-provider";
    
    @Inject
    private LoginService loginService;

    @Inject
    @Nullable
    private OpenIdProvider openIdProvider;

    @Inject
    private TokenManager tokenManager;

    @Inject
    private LogAction actionLogger;
    
    @Inject
    private MeterRegistry meterRegistry;
    
    @Context
    private HttpServletRequest request;
    
    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;
    private final Consul consulClient;
    
    public SessionResource() {
        // Initialize OpenTelemetry tracer
        tracer = GlobalOpenTelemetry.getTracer("org.traccar.api.resource.SessionResource");
        
        // Configure and create circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(10000))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Initialize Consul client for service discovery
        consulClient = Consul.builder().build();
    }

    /**
     * Get user session information
     */
    @PermitAll
    @GET
    @Timed(value = "session.get", description = "Time taken to get session information")
    public User get(@QueryParam("token") String token) throws StorageException, IOException, GeneralSecurityException {
        Span span = tracer.spanBuilder("SessionResource.get")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("token.present", token != null);
            
            if (token != null) {
                LoginResult loginResult = loginService.login(token);
                if (loginResult != null) {
                    User user = loginResult.getUser();
                    // Use Redis-backed session store
                    SessionHelper.userLogin(actionLogger, request, user, loginResult.getExpiration());
                    
                    span.setAttribute("user.id", user.getId());
                    span.setStatus(StatusCode.OK);
                    
                    timer.stop(meterRegistry.timer("session.get.token.success"));
                    return user;
                }
            }

            Long userId = (Long) request.getSession().getAttribute(SessionHelper.USER_ID_KEY);
            if (userId != null) {
                User user = permissionsService.getUser(userId);
                if (user != null) {
                    span.setAttribute("user.id", user.getId());
                    span.setStatus(StatusCode.OK);
                    
                    timer.stop(meterRegistry.timer("session.get.session.success"));
                    return user;
                }
            }

            span.setStatus(StatusCode.ERROR, "User not found");
            timer.stop(meterRegistry.timer("session.get.failure"));
            throw new WebApplicationException(Response.status(Response.Status.NOT_FOUND).build());
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            timer.stop(meterRegistry.timer("session.get.error"));
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Get user by ID
     */
    @Path("{id}")
    @GET
    @Timed(value = "session.getById", description = "Time taken to get session by ID")
    public User get(@PathParam("id") long userId) throws StorageException {
        Span span = tracer.spanBuilder("SessionResource.getById")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("user.id.requested", userId);
            
            permissionsService.checkUser(getUserId(), userId);
            User user = storage.getObject(User.class, new Request(
                    new Columns.All(), new Condition.Equals("id", userId)));
            
            // Use Redis-backed session store
            SessionHelper.userLogin(actionLogger, request, user, null);
            
            span.setStatus(StatusCode.OK);
            timer.stop(meterRegistry.timer("session.getById.success"));
            return user;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            timer.stop(meterRegistry.timer("session.getById.error"));
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Login user with credentials
     */
    @PermitAll
    @POST
    @Timed(value = "session.login", description = "Time taken to login")
    public User add(
            @FormParam("email") String email,
            @FormParam("password") String password,
            @FormParam("code") Integer code) throws StorageException {
        
        Span span = tracer.spanBuilder("SessionResource.login")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("email", email);
            span.setAttribute("code.present", code != null);
            
            LoginResult loginResult;
            try {
                loginResult = loginService.login(email, password, code);
            } catch (CodeRequiredException e) {
                span.setStatus(StatusCode.ERROR, "TOTP code required");
                timer.stop(meterRegistry.timer("session.login.code_required"));
                
                Response response = Response
                        .status(Response.Status.UNAUTHORIZED)
                        .header("WWW-Authenticate", "TOTP")
                        .build();
                throw new WebApplicationException(response);
            }
            
            if (loginResult != null) {
                User user = loginResult.getUser();
                // Use Redis-backed session store
                SessionHelper.userLogin(actionLogger, request, user, null);
                
                span.setAttribute("user.id", user.getId());
                span.setStatus(StatusCode.OK);
                timer.stop(meterRegistry.timer("session.login.success"));
                return user;
            } else {
                actionLogger.failedLogin(request);
                span.setStatus(StatusCode.ERROR, "Login failed");
                timer.stop(meterRegistry.timer("session.login.failed"));
                throw new WebApplicationException(Response.status(Response.Status.UNAUTHORIZED).build());
            }
        } catch (Exception e) {
            if (!(e instanceof WebApplicationException)) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                timer.stop(meterRegistry.timer("session.login.error"));
            }
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Logout user
     */
    @DELETE
    @Timed(value = "session.logout", description = "Time taken to logout")
    public Response remove() {
        Span span = tracer.spanBuilder("SessionResource.logout")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            actionLogger.logout(request, getUserId());
            request.getSession().removeAttribute(SessionHelper.USER_ID_KEY);
            
            span.setStatus(StatusCode.OK);
            timer.stop(meterRegistry.timer("session.logout.success"));
            return Response.noContent().build();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            timer.stop(meterRegistry.timer("session.logout.error"));
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Request authentication token
     */
    @Path("token")
    @POST
    @Timed(value = "session.token", description = "Time taken to generate token")
    public String requestToken(
            @FormParam("expiration") Date expiration) throws StorageException, GeneralSecurityException, IOException {
        
        Span span = tracer.spanBuilder("SessionResource.requestToken")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            Date currentExpiration = (Date) request.getSession().getAttribute(SessionHelper.EXPIRATION_KEY);
            if (currentExpiration != null && currentExpiration.before(expiration)) {
                expiration = currentExpiration;
            }
            
            // Use circuit breaker for token generation
            String token = circuitBreaker.executeSupplier(() -> {
                try {
                    return tokenManager.generateToken(getUserId(), expiration);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to generate token", e);
                }
            });
            
            span.setStatus(StatusCode.OK);
            timer.stop(meterRegistry.timer("session.token.success"));
            return token;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            timer.stop(meterRegistry.timer("session.token.error"));
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * OpenID authentication
     */
    @PermitAll
    @Path("openid/auth")
    @GET
    @Timed(value = "session.openid.auth", description = "Time taken for OpenID auth")
    public Response openIdAuth() {
        Span span = tracer.spanBuilder("SessionResource.openIdAuth")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            // Use service discovery to locate OpenID provider if needed
            OpenIdProvider provider = getOpenIdProvider();
            
            // Use circuit breaker for OpenID auth URI creation
            URI authUri = circuitBreaker.executeSupplier(() -> provider.createAuthUri());
            
            span.setStatus(StatusCode.OK);
            timer.stop(meterRegistry.timer("session.openid.auth.success"));
            return Response.seeOther(authUri).build();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            timer.stop(meterRegistry.timer("session.openid.auth.error"));
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * OpenID callback
     */
    @PermitAll
    @Path("openid/callback")
    @GET
    @Timed(value = "session.openid.callback", description = "Time taken for OpenID callback")
    public Response requestToken() throws IOException, StorageException, ParseException, GeneralSecurityException {
        Span span = tracer.spanBuilder("SessionResource.openIdCallback")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        Timer.Sample timer = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            StringBuilder requestUrl = new StringBuilder(request.getRequestURL().toString());
            String queryString = request.getQueryString();
            String requestUri = requestUrl.append('?').append(queryString).toString();
            
            // Use service discovery to locate OpenID provider if needed
            OpenIdProvider provider = getOpenIdProvider();
            
            // Use circuit breaker for OpenID callback handling
            URI callbackUri = circuitBreaker.executeSupplier(() -> {
                try {
                    return provider.handleCallback(URI.create(requestUri), request);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to handle OpenID callback", e);
                }
            });
            
            span.setStatus(StatusCode.OK);
            timer.stop(meterRegistry.timer("session.openid.callback.success"));
            return Response.seeOther(callbackUri).build();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            timer.stop(meterRegistry.timer("session.openid.callback.error"));
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Get OpenID provider using service discovery if needed
     */
    private OpenIdProvider getOpenIdProvider() {
        if (openIdProvider != null) {
            return openIdProvider;
        }
        
        // Use service discovery to locate OpenID provider
        try {
            HealthClient healthClient = consulClient.healthClient();
            List<ServiceHealth> instances = healthClient.getHealthyServiceInstances(OPENID_SERVICE_NAME).getResponse();
            
            if (!instances.isEmpty()) {
                ServiceHealth serviceHealth = instances.get(0);
                String serviceAddress = serviceHealth.getService().getAddress();
                int servicePort = serviceHealth.getService().getPort();
                
                // Here you would create a dynamic OpenID provider based on discovered service
                // This is a placeholder - actual implementation would depend on your OpenID provider setup
                return openIdProvider; // Return default for now
            }
        } catch (Exception e) {
            // Log error and fall back to default provider
        }
        
        return openIdProvider;
    }
}