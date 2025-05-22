/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.AbstractStub;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Factory for creating gRPC clients used for efficient service-to-service communication
 * between the API Gateway and backend microservices. It manages client lifecycle, integrates
 * with service discovery for endpoint resolution, applies circuit breakers for resilience,
 * and adds distributed tracing to all gRPC calls.
 */
@Singleton
public class GrpcClientFactory {

    private static final Logger LOGGER = LoggerFactory.getLogger(GrpcClientFactory.class);
    
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final OpenTelemetry openTelemetry;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscoveryResolver serviceDiscoveryResolver;
    
    /**
     * Creates a new GrpcClientFactory with the specified dependencies.
     *
     * @param circuitBreakerRegistry Registry for creating and managing circuit breakers
     * @param openTelemetry OpenTelemetry instance for distributed tracing
     * @param meterRegistry Metrics registry for collecting performance metrics
     * @param serviceDiscoveryResolver Service discovery resolver for endpoint resolution
     */
    @Inject
    public GrpcClientFactory(
            CircuitBreakerRegistry circuitBreakerRegistry,
            OpenTelemetry openTelemetry,
            MeterRegistry meterRegistry,
            ServiceDiscoveryResolver serviceDiscoveryResolver) {
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.openTelemetry = openTelemetry;
        this.meterRegistry = meterRegistry;
        this.serviceDiscoveryResolver = serviceDiscoveryResolver;
    }
    
    /**
     * Creates a gRPC stub of the specified type for the given service name.
     * The stub is enhanced with circuit breaker, tracing, and metrics.
     *
     * @param serviceName Name of the service to connect to
     * @param stubFactory Factory function to create the stub from a channel
     * @param <T> Type of the gRPC stub
     * @return A configured gRPC stub
     */
    public <T extends AbstractStub<T>> T createStub(String serviceName, Function<Channel, T> stubFactory) {
        ManagedChannel channel = getOrCreateChannel(serviceName);
        Channel interceptedChannel = applyInterceptors(channel, serviceName);
        T stub = stubFactory.apply(interceptedChannel);
        
        // Apply circuit breaker to the stub
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker(serviceName);
        return applyCircuitBreaker(stub, circuitBreaker);
    }
    
    /**
     * Creates a blocking stub of the specified type for the given service name.
     * This is a convenience method for services that use blocking stubs.
     *
     * @param serviceName Name of the service to connect to
     * @param stubFactory Factory function to create the blocking stub from a channel
     * @param <T> Type of the gRPC blocking stub
     * @return A configured gRPC blocking stub
     */
    public <T extends AbstractStub<T>> T createBlockingStub(String serviceName, Function<Channel, T> stubFactory) {
        return createStub(serviceName, stubFactory);
    }
    
    /**
     * Creates an async stub of the specified type for the given service name.
     * This is a convenience method for services that use async stubs.
     *
     * @param serviceName Name of the service to connect to
     * @param stubFactory Factory function to create the async stub from a channel
     * @param <T> Type of the gRPC async stub
     * @return A configured gRPC async stub
     */
    public <T extends AbstractStub<T>> T createAsyncStub(String serviceName, Function<Channel, T> stubFactory) {
        return createStub(serviceName, stubFactory);
    }
    
    /**
     * Creates a future stub of the specified type for the given service name.
     * This is a convenience method for services that use future stubs.
     *
     * @param serviceName Name of the service to connect to
     * @param stubFactory Factory function to create the future stub from a channel
     * @param <T> Type of the gRPC future stub
     * @return A configured gRPC future stub
     */
    public <T extends AbstractStub<T>> T createFutureStub(String serviceName, Function<Channel, T> stubFactory) {
        return createStub(serviceName, stubFactory);
    }
    
    /**
     * Gets an existing channel for the service or creates a new one if it doesn't exist.
     *
     * @param serviceName Name of the service to get or create a channel for
     * @return A managed channel for the specified service
     */
    private ManagedChannel getOrCreateChannel(String serviceName) {
        return channels.computeIfAbsent(serviceName, this::createChannel);
    }
    
    /**
     * Creates a new managed channel for the specified service.
     *
     * @param serviceName Name of the service to create a channel for
     * @return A new managed channel for the service
     */
    private ManagedChannel createChannel(String serviceName) {
        String endpoint = serviceDiscoveryResolver.resolveServiceEndpoint(serviceName);
        LOGGER.info("Creating gRPC channel for service {} at endpoint {}", serviceName, endpoint);
        
        String host = endpoint.split(":")[0];
        int port = Integer.parseInt(endpoint.split(":")[1]);
        
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext() // For development; use TLS in production
                .enableRetry()
                .maxRetryAttempts(3)
                .keepAliveTime(30, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .build();
    }
    
    /**
     * Applies interceptors to the channel for tracing and metrics.
     *
     * @param channel The channel to apply interceptors to
     * @param serviceName Name of the service for metrics labeling
     * @return A channel with interceptors applied
     */
    private Channel applyInterceptors(ManagedChannel channel, String serviceName) {
        // Create OpenTelemetry gRPC instrumentation
        GrpcTelemetry grpcTelemetry = GrpcTelemetry.create(openTelemetry);
        ClientInterceptor tracingInterceptor = grpcTelemetry.newClientInterceptor();
        
        // Create metrics interceptor
        ClientInterceptor metricsInterceptor = new MetricsClientInterceptor(meterRegistry, serviceName);
        
        // Apply all interceptors
        return ClientInterceptors.intercept(channel, tracingInterceptor, metricsInterceptor);
    }
    
    /**
     * Gets an existing circuit breaker for the service or creates a new one if it doesn't exist.
     *
     * @param serviceName Name of the service to get or create a circuit breaker for
     * @return A circuit breaker for the specified service
     */
    private CircuitBreaker getOrCreateCircuitBreaker(String serviceName) {
        String circuitBreakerName = serviceName + "CircuitBreaker";
        return circuitBreakerRegistry.circuitBreaker(circuitBreakerName, createCircuitBreakerConfig());
    }
    
    /**
     * Creates a default circuit breaker configuration.
     *
     * @return A circuit breaker configuration
     */
    private CircuitBreakerConfig createCircuitBreakerConfig() {
        return CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(10)) // Wait 10 seconds before trying again
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100) // Consider the last 100 calls
                .minimumNumberOfCalls(10) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in half-open state
                .recordExceptions(Exception.class) // Record all exceptions as failures
                .ignoreExceptions(Status.CANCELLED.asRuntimeException().getClass()) // Ignore cancelled calls
                .build();
    }
    
    /**
     * Applies a circuit breaker to a gRPC stub.
     *
     * @param stub The stub to apply the circuit breaker to
     * @param circuitBreaker The circuit breaker to apply
     * @param <T> Type of the gRPC stub
     * @return A stub with circuit breaker applied
     */
    private <T extends AbstractStub<T>> T applyCircuitBreaker(T stub, CircuitBreaker circuitBreaker) {
        // Create a proxy that wraps all calls with the circuit breaker
        return stub.withInterceptors(new CircuitBreakerClientInterceptor(circuitBreaker));
    }
    
    /**
     * Shuts down all managed channels.
     * This should be called when the application is shutting down.
     */
    public void shutdown() {
        LOGGER.info("Shutting down all gRPC channels");
        for (ManagedChannel channel : channels.values()) {
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.warn("Interrupted while shutting down channel", e);
                Thread.currentThread().interrupt();
            } finally {
                if (!channel.isTerminated()) {
                    channel.shutdownNow();
                }
            }
        }
        channels.clear();
    }
    
    /**
     * Client interceptor for collecting metrics on gRPC calls.
     */
    private static class MetricsClientInterceptor implements ClientInterceptor {
        
        private final MeterRegistry meterRegistry;
        private final String serviceName;
        
        MetricsClientInterceptor(MeterRegistry meterRegistry, String serviceName) {
            this.meterRegistry = meterRegistry;
            this.serviceName = serviceName;
        }
        
        @Override
        public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                io.grpc.MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions callOptions, io.grpc.Channel next) {
            
            Timer.Sample sample = Timer.start(meterRegistry);
            String methodName = method.getFullMethodName();
            
            return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    next.newCall(method, callOptions)) {
                
                @Override
                public void start(Listener<RespT> responseListener, io.grpc.Metadata headers) {
                    super.start(new io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        @Override
                        public void onClose(io.grpc.Status status, io.grpc.Metadata trailers) {
                            sample.stop(Timer.builder("grpc.client.calls")
                                    .tag("service", serviceName)
                                    .tag("method", methodName)
                                    .tag("status", status.getCode().name())
                                    .register(meterRegistry));
                            
                            // Count errors
                            if (!status.isOk()) {
                                meterRegistry.counter("grpc.client.errors",
                                        "service", serviceName,
                                        "method", methodName,
                                        "status", status.getCode().name())
                                        .increment();
                            }
                            
                            super.onClose(status, trailers);
                        }
                    }, headers);
                }
            };
        }
    }
    
    /**
     * Client interceptor for applying circuit breaker to gRPC calls.
     */
    private static class CircuitBreakerClientInterceptor implements ClientInterceptor {
        
        private final CircuitBreaker circuitBreaker;
        
        CircuitBreakerClientInterceptor(CircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
        }
        
        @Override
        public <ReqT, RespT> io.grpc.ClientCall<ReqT, RespT> interceptCall(
                io.grpc.MethodDescriptor<ReqT, RespT> method, io.grpc.CallOptions callOptions, io.grpc.Channel next) {
            
            return new io.grpc.ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
                    next.newCall(method, callOptions)) {
                
                @Override
                public void start(Listener<RespT> responseListener, io.grpc.Metadata headers) {
                    // Check if the circuit is closed before proceeding
                    if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                        responseListener.onClose(Status.UNAVAILABLE.withDescription(
                                "Circuit breaker is open for " + method.getFullMethodName()), new io.grpc.Metadata());
                        return;
                    }
                    
                    super.start(new io.grpc.ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(responseListener) {
                        @Override
                        public void onClose(io.grpc.Status status, io.grpc.Metadata trailers) {
                            if (status.isOk()) {
                                circuitBreaker.onSuccess(0);
                            } else {
                                // Only record certain types of failures
                                if (isRecordableError(status)) {
                                    circuitBreaker.onError(0, status.asException());
                                } else {
                                    circuitBreaker.onSuccess(0);
                                }
                            }
                            super.onClose(status, trailers);
                        }
                        
                        private boolean isRecordableError(io.grpc.Status status) {
                            // Don't record client cancellations or deadline exceeded as service failures
                            return status.getCode() != Status.Code.CANCELLED
                                    && status.getCode() != Status.Code.DEADLINE_EXCEEDED;
                        }
                    }, headers);
                }
            };
        }
    }
    
    /**
     * Interface for resolving service endpoints using service discovery.
     */
    public interface ServiceDiscoveryResolver {
        /**
         * Resolves a service name to an endpoint (host:port).
         *
         * @param serviceName Name of the service to resolve
         * @return Endpoint in the format "host:port"
         */
        String resolveServiceEndpoint(String serviceName);
    }
}