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
package org.traccar.handler.network;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Position;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Handler that enriches Position objects with the remote IP address of the connection.
 * Supports both direct and service-based processing with distributed tracing, metrics collection,
 * correlation ID propagation, and circuit breaker pattern for resilience.
 */
@Singleton
@ChannelHandler.Sharable
public class RemoteAddressHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteAddressHandler.class);
    private static final String CORRELATION_ID_KEY = "correlationId";
    private static final String CIRCUIT_BREAKER_NAME = "ipEnrichment";
    private static final String SPAN_NAME = "ip.enrichment";
    
    // OpenTelemetry attribute keys
    private static final AttributeKey<String> ATTR_REMOTE_IP = AttributeKey.stringKey("remote.ip");
    private static final AttributeKey<String> ATTR_CORRELATION_ID = AttributeKey.stringKey("correlation.id");
    private static final AttributeKey<String> ATTR_DEPLOYMENT_ENV = AttributeKey.stringKey("deployment.environment");
    private static final AttributeKey<Boolean> ATTR_SUCCESS = AttributeKey.booleanKey("enrichment.success");

    private final boolean enabled;
    private final String deploymentEnvironment;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Timer enrichmentTimer;

    /**
     * Creates a new RemoteAddressHandler with the specified dependencies.
     *
     * @param config Configuration for the handler
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     */
    @Inject
    public RemoteAddressHandler(Config config, Tracer tracer, MeterRegistry meterRegistry) {
        this.enabled = config.getBoolean(Keys.PROCESSING_REMOTE_ADDRESS_ENABLE);
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        this.deploymentEnvironment = config.getString("deployment.environment", "unknown");
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // Open circuit when 50% of calls fail
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds before trying again
                .slidingWindowSize(10) // Consider the last 10 calls
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker state transition listener for logging
        this.circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' state changed from {} to {}",
                        event.getCircuitBreakerName(), event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        // Initialize metrics
        this.successCounter = meterRegistry.counter("ip.enrichment.success", 
                "environment", deploymentEnvironment);
        this.failureCounter = meterRegistry.counter("ip.enrichment.failure", 
                "environment", deploymentEnvironment);
        this.enrichmentTimer = meterRegistry.timer("ip.enrichment.duration", 
                "environment", deploymentEnvironment);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // Generate or extract correlation ID
        String correlationId = extractOrGenerateCorrelationId(ctx);
        
        try {
            // Set correlation ID in MDC for logging
            MDC.put(CORRELATION_ID_KEY, correlationId);
            
            if (enabled && msg instanceof Position) {
                Position position = (Position) msg;
                
                // Start OpenTelemetry span
                Span span = tracer.spanBuilder(SPAN_NAME)
                        .setSpanKind(SpanKind.INTERNAL)
                        .setAttribute(ATTR_CORRELATION_ID, correlationId)
                        .setAttribute(ATTR_DEPLOYMENT_ENV, deploymentEnvironment)
                        .startSpan();
                
                try (Scope scope = span.makeCurrent()) {
                    // Execute IP enrichment with circuit breaker protection
                    enrichPositionWithIp(ctx, position, span);
                } catch (Exception e) {
                    span.setStatus(StatusCode.ERROR);
                    span.recordException(e);
                    LOGGER.error("Error enriching position with IP address", e);
                } finally {
                    span.end();
                }
            }
        } finally {
            MDC.remove(CORRELATION_ID_KEY);
            ctx.fireChannelRead(msg);
        }
    }
    
    /**
     * Extracts correlation ID from context or generates a new one if not present.
     *
     * @param ctx Channel handler context
     * @return Correlation ID for request tracking
     */
    private String extractOrGenerateCorrelationId(ChannelHandlerContext ctx) {
        // Try to extract from context attributes if present
        if (ctx.channel().hasAttr(io.netty.util.AttributeKey.valueOf(CORRELATION_ID_KEY))) {
            return ctx.channel().attr(io.netty.util.AttributeKey.valueOf(CORRELATION_ID_KEY)).get().toString();
        }
        
        // Generate new correlation ID if not present
        String correlationId = UUID.randomUUID().toString();
        ctx.channel().attr(io.netty.util.AttributeKey.valueOf(CORRELATION_ID_KEY)).set(correlationId);
        return correlationId;
    }
    
    /**
     * Enriches position with IP address using circuit breaker pattern for resilience.
     *
     * @param ctx Channel handler context
     * @param position Position to enrich
     * @param span Current OpenTelemetry span
     */
    private void enrichPositionWithIp(ChannelHandlerContext ctx, Position position, Span span) {
        Timer.Sample timerSample = Timer.start(meterRegistry);
        
        try {
            // Execute with circuit breaker protection
            String hostAddress = circuitBreaker.executeCallable(() -> {
                InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
                if (remoteAddress == null || remoteAddress.getAddress() == null) {
                    throw new IllegalStateException("Remote address not available");
                }
                return remoteAddress.getAddress().getHostAddress();
            });
            
            // Set IP address in position
            position.set(Position.KEY_IP, hostAddress);
            
            // Record success metrics and span attributes
            successCounter.increment();
            span.setAttribute(ATTR_REMOTE_IP, hostAddress);
            span.setAttribute(ATTR_SUCCESS, true);
            LOGGER.debug("Successfully enriched position with IP: {}", hostAddress);
            
        } catch (Exception e) {
            // Record failure metrics and span attributes
            failureCounter.increment();
            span.setAttribute(ATTR_SUCCESS, false);
            LOGGER.warn("Failed to enrich position with IP address: {}", e.getMessage());
            
            // Set null IP address in position as fallback
            position.set(Position.KEY_IP, null);
        } finally {
            // Record timing metrics
            timerSample.stop(enrichmentTimer);
        }
    }
}