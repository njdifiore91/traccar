/*
 * Copyright 2018 - 2023 Anton Tananaev (anton@traccar.org)
 * Copyright 2018 Andrey Kunitsyn (andrey@traccar.org)
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

import com.google.inject.Provider;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.api.security.PermissionsService;
import org.traccar.api.signature.TokenManager;
import org.traccar.database.StatisticsManager;
import org.traccar.helper.SessionHelper;
import org.traccar.model.Device;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Filter for handling media requests with JWT authentication, distributed tracing,
 * circuit breaker pattern, and metrics collection.
 */
@Singleton
public class MediaFilter implements Filter {

    private static final String MEDIA_CIRCUIT_BREAKER = "mediaCircuitBreaker";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String BEARER_PREFIX = "Bearer ";

    private final Storage storage;
    private final StatisticsManager statisticsManager;
    private final Provider<PermissionsService> permissionsServiceProvider;
    private final TokenManager tokenManager;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final CircuitBreaker circuitBreaker;

    private final Counter mediaRequestsTotal;
    private final Counter mediaRequestsSuccess;
    private final Counter mediaRequestsFailure;
    private final Timer mediaRequestDuration;

    @Inject
    public MediaFilter(
            Storage storage,
            StatisticsManager statisticsManager,
            Provider<PermissionsService> permissionsServiceProvider,
            TokenManager tokenManager,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.storage = storage;
        this.statisticsManager = statisticsManager;
        this.permissionsServiceProvider = permissionsServiceProvider;
        this.tokenManager = tokenManager;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .recordExceptions(StorageException.class, IOException.class, TimeoutException.class)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(MEDIA_CIRCUIT_BREAKER);

        // Initialize metrics
        this.mediaRequestsTotal = Counter.builder("media_requests_total")
                .description("Total number of media requests")
                .register(meterRegistry);
        
        this.mediaRequestsSuccess = Counter.builder("media_requests_success")
                .description("Number of successful media requests")
                .register(meterRegistry);
        
        this.mediaRequestsFailure = Counter.builder("media_requests_failure")
                .description("Number of failed media requests")
                .register(meterRegistry);
        
        this.mediaRequestDuration = Timer.builder("media_request_duration")
                .description("Duration of media requests")
                .register(meterRegistry);

        // Register circuit breaker state transition event consumer
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    meterRegistry.counter("circuit_breaker_state_transition",
                            "name", MEDIA_CIRCUIT_BREAKER,
                            "from", event.getStateTransition().getFromState().name(),
                            "to", event.getStateTransition().getToState().name())
                            .increment();
                });
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        // Start OpenTelemetry span
        Span span = tracer.spanBuilder("MediaFilter.doFilter")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();

        // Increment total request counter
        mediaRequestsTotal.increment();

        // Start timer for request duration
        Timer.Sample timerSample = Timer.start(meterRegistry);

        try (Scope scope = span.makeCurrent()) {
            Long userId = authenticateRequest(httpRequest, span);
            if (userId == null) {
                span.setStatus(StatusCode.ERROR, "Authentication failed");
                mediaRequestsFailure.increment();
                httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED);
                return;
            }

            // Register request with statistics manager
            statisticsManager.registerRequest(userId);

            // Extract device ID from path
            String path = httpRequest.getPathInfo();
            span.setAttribute("path", path != null ? path : "");

            String[] parts = path != null ? path.split("/") : null;
            if (parts != null && parts.length >= 2) {
                String deviceUniqueId = parts[1];
                span.setAttribute("deviceUniqueId", deviceUniqueId);

                // Use circuit breaker to protect storage calls
                try {
                    Device device = circuitBreaker.executeSupplier(() -> {
                        try {
                            return storage.getObject(Device.class, new Request(
                                    new Columns.All(), new Condition.Equals("uniqueId", deviceUniqueId)));
                        } catch (StorageException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    if (device != null) {
                        span.setAttribute("deviceId", device.getId());
                        span.setAttribute("deviceName", device.getName());

                        // Check permission
                        permissionsServiceProvider.get().checkPermission(Device.class, userId, device.getId());

                        // Record successful metrics
                        mediaRequestsSuccess.increment();
                        span.setStatus(StatusCode.OK);

                        // Continue with the filter chain
                        chain.doFilter(request, response);
                        return;
                    }
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                }
            }

            // If we get here, either the path was invalid or device not found
            mediaRequestsFailure.increment();
            httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN);

        } catch (SecurityException | StorageException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            mediaRequestsFailure.increment();
            httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
            e.printStackTrace(httpResponse.getWriter());
        } finally {
            // Record request duration
            timerSample.stop(mediaRequestDuration);
            span.end();
        }
    }

    /**
     * Authenticates the request using JWT token or session.
     * 
     * @param request The HTTP request
     * @param span The current tracing span
     * @return User ID if authenticated, null otherwise
     */
    private Long authenticateRequest(HttpServletRequest request, Span span) {
        // First try JWT token authentication
        String authHeader = request.getHeader(AUTHORIZATION_HEADER);
        if (authHeader != null && authHeader.startsWith(BEARER_PREFIX)) {
            String token = authHeader.substring(BEARER_PREFIX.length());
            span.setAttribute("auth.type", "jwt");
            try {
                TokenManager.TokenData tokenData = tokenManager.verifyToken(token);
                Date expiration = tokenData.getExpiration();
                if (expiration != null && expiration.after(new Date())) {
                    return tokenData.getUserId();
                }
            } catch (IOException | GeneralSecurityException | StorageException e) {
                span.recordException(e);
            }
            return null;
        }

        // Fall back to session-based authentication
        span.setAttribute("auth.type", "session");
        HttpSession session = request.getSession(false);
        if (session != null) {
            Long userId = (Long) session.getAttribute(SessionHelper.USER_ID_KEY);
            Date expiration = (Date) session.getAttribute(SessionHelper.EXPIRATION_KEY);
            if (userId != null && (expiration == null || expiration.after(new Date()))) {
                return userId;
            }
        }

        return null;
    }
}