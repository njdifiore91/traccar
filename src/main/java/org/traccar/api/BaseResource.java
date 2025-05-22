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
package org.traccar.api;

import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.health.ServiceHealth;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import org.traccar.api.security.PermissionsService;
import org.traccar.api.security.UserPrincipal;
import org.traccar.storage.Storage;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.SecurityContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Base resource class that provides common functionality for all REST resources.
 * Includes support for service discovery, circuit breakers, distributed tracing,
 * service-to-service communication, and metrics collection.
 */
@Singleton
public class BaseResource {

    @Context
    private SecurityContext securityContext;

    @Context
    private HttpHeaders httpHeaders;

    @Inject
    protected Storage storage;

    @Inject
    protected PermissionsService permissionsService;
    
    @Inject
    protected OpenTelemetry openTelemetry;
    
    @Inject
    protected Tracer tracer;
    
    @Inject
    protected MeterRegistry meterRegistry;
    
    @Inject
    protected Consul consulClient;
    
    @Inject
    protected CircuitBreakerRegistry circuitBreakerRegistry;
    
    // Cache of gRPC channels for service-to-service communication
    private final Map<String, ManagedChannel> grpcChannels = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * Gets the ID of the current user from the security context.
     *
     * @return The user ID, or 0 if no user is authenticated
     */
    protected long getUserId() {
        UserPrincipal principal = (UserPrincipal) securityContext.getUserPrincipal();
        if (principal != null) {
            return principal.getUserId();
        }
        return 0;
    }
    
    /**
     * Creates a new span for the current operation.
     *
     * @param operationName The name of the operation
     * @return The created span
     */
    protected Span createSpan(String operationName) {
        return tracer.spanBuilder(operationName)
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
    }
    
    /**
     * Executes the given supplier within a new span context.
     *
     * @param operationName The name of the operation
     * @param supplier The supplier to execute
     * @param <T> The return type of the supplier
     * @return The result of the supplier
     */
    protected <T> T traceOperation(String operationName, Supplier<T> supplier) {
        Span span = createSpan(operationName);
        try (Scope scope = span.makeCurrent()) {
            T result = supplier.get();
            span.setStatus(StatusCode.OK);
            return result;
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Executes the given supplier asynchronously within a new span context.
     *
     * @param operationName The name of the operation
     * @param supplier The supplier to execute
     * @param <T> The return type of the supplier
     * @return A CompletionStage that will complete with the result of the supplier
     */
    protected <T> CompletionStage<T> traceOperationAsync(String operationName, Supplier<CompletionStage<T>> supplier) {
        Span span = createSpan(operationName);
        Context context = Context.current().with(span);
        try (Scope scope = context.makeCurrent()) {
            return supplier.get().whenComplete((result, error) -> {
                if (error != null) {
                    span.setStatus(StatusCode.ERROR, error.getMessage());
                } else {
                    span.setStatus(StatusCode.OK);
                }
                span.end();
            });
        }
    }
    
    /**
     * Discovers a service instance using Consul service discovery.
     *
     * @param serviceName The name of the service to discover
     * @return An optional containing the service health information if found
     */
    protected Optional<ServiceHealth> discoverService(String serviceName) {
        HealthClient healthClient = consulClient.healthClient();
        List<ServiceHealth> instances = healthClient.getHealthyServiceInstances(serviceName).getResponse();
        
        if (instances.isEmpty()) {
            return Optional.empty();
        }
        
        // Simple round-robin load balancing - in production, you might want a more sophisticated approach
        int index = Math.abs(serviceName.hashCode()) % instances.size();
        return Optional.of(instances.get(index));
    }
    
    /**
     * Gets a gRPC channel for the specified service.
     *
     * @param serviceName The name of the service
     * @return The gRPC channel, or null if the service could not be discovered
     */
    protected ManagedChannel getGrpcChannel(String serviceName) {
        return grpcChannels.computeIfAbsent(serviceName, name -> {
            Optional<ServiceHealth> serviceHealth = discoverService(name);
            if (serviceHealth.isPresent()) {
                String host = serviceHealth.get().getService().getAddress();
                int port = serviceHealth.get().getService().getPort();
                return ManagedChannelBuilder.forAddress(host, port)
                        .usePlaintext() // In production, you would use TLS
                        .build();
            }
            return null;
        });
    }
    
    /**
     * Executes the given supplier with a circuit breaker.
     *
     * @param circuitBreakerId The ID of the circuit breaker
     * @param supplier The supplier to execute
     * @param <T> The return type of the supplier
     * @return The result of the supplier
     */
    protected <T> T executeWithCircuitBreaker(String circuitBreakerId, Supplier<T> supplier) {
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(circuitBreakerId);
        return CircuitBreaker.decorateSupplier(circuitBreaker, supplier).get();
    }
    
    /**
     * Executes the given supplier with a circuit breaker and fallback.
     *
     * @param circuitBreakerId The ID of the circuit breaker
     * @param supplier The supplier to execute
     * @param fallback The fallback value to return if the circuit breaker is open
     * @param <T> The return type of the supplier
     * @return The result of the supplier, or the fallback if the circuit breaker is open
     */
    protected <T> T executeWithCircuitBreakerAndFallback(String circuitBreakerId, Supplier<T> supplier, T fallback) {
        try {
            return executeWithCircuitBreaker(circuitBreakerId, supplier);
        } catch (Exception e) {
            return fallback;
        }
    }
    
    /**
     * Creates a timer for measuring the execution time of operations.
     *
     * @param name The name of the timer
     * @param tags Additional tags for the timer
     * @return The created timer
     */
    protected Timer createTimer(String name, String... tags) {
        return meterRegistry.timer(name, tags);
    }
    
    /**
     * Creates a counter for counting events.
     *
     * @param name The name of the counter
     * @param tags Additional tags for the counter
     * @return The created counter
     */
    protected Counter createCounter(String name, String... tags) {
        return meterRegistry.counter(name, tags);
    }
    
    /**
     * Measures the execution time of the given supplier.
     *
     * @param timerName The name of the timer
     * @param supplier The supplier to execute
     * @param <T> The return type of the supplier
     * @return The result of the supplier
     */
    protected <T> T measureExecutionTime(String timerName, Supplier<T> supplier) {
        Timer timer = createTimer(timerName);
        return timer.record(supplier);
    }
    
    /**
     * Measures the execution time of the given supplier with additional tags.
     *
     * @param timerName The name of the timer
     * @param supplier The supplier to execute
     * @param tags Additional tags for the timer
     * @param <T> The return type of the supplier
     * @return The result of the supplier
     */
    protected <T> T measureExecutionTime(String timerName, Supplier<T> supplier, String... tags) {
        Timer timer = createTimer(timerName, tags);
        return timer.record(supplier);
    }
}