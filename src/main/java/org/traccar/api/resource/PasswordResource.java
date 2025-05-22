/*
 * Copyright 2021 - 2022 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.api.BaseResource;
import org.traccar.api.signature.TokenManager;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.mail.MailManager;
import org.traccar.model.User;
import org.traccar.notification.TextTemplateFormatter;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.mail.MessagingException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Password management resource for handling password reset and update operations.
 * This resource integrates with service discovery, circuit breaker, distributed tracing,
 * and metrics collection for enhanced reliability and observability.
 */
@Path("password")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_FORM_URLENCODED)
public class PasswordResource extends BaseResource {

    private final MailManager mailManager;
    private final TokenManager tokenManager;
    private final TextTemplateFormatter textTemplateFormatter;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscovery serviceDiscovery;
    private final CircuitBreaker mailCircuitBreaker;
    private final Timer resetPasswordTimer;
    private final Timer updatePasswordTimer;

    /**
     * Constructs a PasswordResource with the necessary dependencies.
     *
     * @param mailManager Mail service for sending password reset emails
     * @param tokenManager Token management for password reset tokens
     * @param textTemplateFormatter Template formatter for email content
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     * @param serviceDiscovery Service discovery for locating backend services
     */
    @Inject
    public PasswordResource(
            MailManager mailManager,
            TokenManager tokenManager,
            TextTemplateFormatter textTemplateFormatter,
            Tracer tracer,
            MeterRegistry meterRegistry,
            ServiceDiscovery serviceDiscovery) {
        this.mailManager = mailManager;
        this.tokenManager = tokenManager;
        this.textTemplateFormatter = textTemplateFormatter;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.serviceDiscovery = serviceDiscovery;
        
        // Configure circuit breaker for mail service
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before attempting again
                .slidingWindowSize(10) // Consider last 10 calls for failure rate
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.mailCircuitBreaker = circuitBreakerRegistry.circuitBreaker("mailService");
        
        // Initialize metrics timers
        this.resetPasswordTimer = Timer.builder("password.reset")
                .description("Time taken to process password reset requests")
                .tag("resource", "password")
                .tag("operation", "reset")
                .register(meterRegistry);
        
        this.updatePasswordTimer = Timer.builder("password.update")
                .description("Time taken to process password update requests")
                .tag("resource", "password")
                .tag("operation", "update")
                .register(meterRegistry);
    }

    /**
     * Handles password reset requests by sending a reset email to the user.
     * Uses circuit breaker pattern for mail service resilience and distributed tracing
     * for cross-service request tracking.
     *
     * @param email The email address of the user requesting password reset
     * @return HTTP response indicating success or failure
     * @throws StorageException If there's an issue with storage operations
     * @throws MessagingException If there's an issue with email sending
     * @throws GeneralSecurityException If there's a security-related issue
     * @throws IOException If there's an I/O issue
     */
    @Path("reset")
    @PermitAll
    @POST
    @Timed(value = "password.reset.time", description = "Time taken to process password reset requests")
    public Response reset(@FormParam("email") String email)
            throws StorageException, MessagingException, GeneralSecurityException, IOException {
        
        return resetPasswordTimer.record(() -> {
            // Create a span for distributed tracing
            Span span = tracer.spanBuilder("PasswordResource.reset")
                    .setSpanKind(SpanKind.SERVER)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                span.setAttribute("email", email);
                
                // Find user by email
                User user = storage.getObject(User.class, new Request(
                        new Columns.All(), new Condition.Equals("email", email)));
                
                if (user != null) {
                    span.setAttribute("userId", user.getId());
                    
                    // Prepare email content using template formatter
                    var velocityContext = textTemplateFormatter.prepareContext(permissionsService.getServer(), user);
                    var fullMessage = textTemplateFormatter.formatMessage(velocityContext, "passwordReset", "full");
                    
                    // Send email with circuit breaker pattern
                    try {
                        // Use circuit breaker to protect against mail service failures
                        mailCircuitBreaker.executeSupplier(() -> {
                            try {
                                // Create a child span for mail sending
                                Span mailSpan = tracer.spanBuilder("MailManager.sendMessage")
                                        .setParent(Context.current())
                                        .setSpanKind(SpanKind.CLIENT)
                                        .startSpan();
                                
                                try (Scope mailScope = mailSpan.makeCurrent()) {
                                    // Discover mail service if needed
                                    String mailServiceUrl = serviceDiscovery.getServiceUrl("mail-service");
                                    mailSpan.setAttribute("mail.service.url", mailServiceUrl != null ? mailServiceUrl : "default");
                                    
                                    // Send the email
                                    mailManager.sendMessage(user, true, fullMessage.getSubject(), fullMessage.getBody());
                                    mailSpan.setStatus(StatusCode.OK);
                                    return true;
                                } catch (Exception e) {
                                    mailSpan.recordException(e);
                                    mailSpan.setStatus(StatusCode.ERROR, e.getMessage());
                                    throw e;
                                } finally {
                                    mailSpan.end();
                                }
                            } catch (MessagingException e) {
                                span.recordException(e);
                                span.setStatus(StatusCode.ERROR, "Failed to send reset email");
                                // Re-throw to be handled by circuit breaker
                                throw new RuntimeException(e);
                            }
                        });
                    } catch (Exception e) {
                        // Circuit breaker will handle the exception, but we still log it in the span
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, "Mail service unavailable");
                        // We don't re-throw here to avoid exposing internal errors to the client
                        // Instead, we still return OK to not reveal if the email exists
                    }
                } else {
                    // User not found - we still return OK to not reveal if the email exists
                    span.setAttribute("userFound", false);
                }
                
                span.setStatus(StatusCode.OK);
                return Response.ok().build();
            } finally {
                span.end();
            }
        });
    }

    /**
     * Updates a user's password using a reset token.
     * Uses distributed tracing for cross-service request tracking and metrics
     * for performance monitoring.
     *
     * @param token The password reset token
     * @param password The new password
     * @return HTTP response indicating success or failure
     * @throws StorageException If there's an issue with storage operations
     * @throws GeneralSecurityException If there's a security-related issue
     * @throws IOException If there's an I/O issue
     */
    @Path("update")
    @PermitAll
    @POST
    @Timed(value = "password.update.time", description = "Time taken to process password update requests")
    public Response update(
            @FormParam("token") String token, @FormParam("password") String password)
            throws StorageException, GeneralSecurityException, IOException {
        
        return updatePasswordTimer.record(() -> {
            // Create a span for distributed tracing
            Span span = tracer.spanBuilder("PasswordResource.update")
                    .setSpanKind(SpanKind.SERVER)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                span.setAttribute("hasToken", token != null && !token.isEmpty());
                
                // Verify token and get user ID
                long userId;
                try {
                    userId = tokenManager.verifyToken(token).getUserId();
                    span.setAttribute("userId", userId);
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, "Invalid token");
                    return Response.status(Response.Status.BAD_REQUEST).entity("Invalid token").build();
                }
                
                // Find user by ID
                User user = storage.getObject(User.class, new Request(
                        new Columns.All(), new Condition.Equals("id", userId)));
                
                if (user != null) {
                    // Update password
                    user.setPassword(password);
                    storage.updateObject(user, new Request(
                            new Columns.Include("hashedPassword", "salt"),
                            new Condition.Equals("id", userId)));
                    
                    span.setStatus(StatusCode.OK);
                    return Response.ok().build();
                } else {
                    span.setStatus(StatusCode.ERROR, "User not found");
                    return Response.status(Response.Status.NOT_FOUND).build();
                }
            } finally {
                span.end();
            }
        });
    }

    /**
     * Asynchronously resets a user's password by sending a reset email.
     * This method provides a non-blocking alternative to the synchronous reset method.
     *
     * @param email The email address of the user requesting password reset
     * @return CompletableFuture with the HTTP response
     */
    @Path("reset-async")
    @PermitAll
    @POST
    public CompletableFuture<Response> resetAsync(@FormParam("email") String email) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return reset(email);
            } catch (Exception e) {
                // Log the exception but return OK to not reveal if the email exists
                return Response.ok().build();
            }
        });
    }
}