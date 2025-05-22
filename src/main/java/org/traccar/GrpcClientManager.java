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
package org.traccar;

import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.grpc.ClientInterceptor;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import io.grpc.stub.StreamObserver;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.LoadBalancerRegistry;
import io.grpc.internal.DnsNameResolverProvider;
import io.grpc.util.RoundRobinLoadBalancerFactory;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.LogAction;
import org.traccar.helper.model.AttributeUtil;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages gRPC client connections to microservices.
 * Provides channel management, connection pooling, and error handling for gRPC clients.
 */
@Singleton
public class GrpcClientManager {

    private static final Logger LOGGER = Logger.getLogger(GrpcClientManager.class.getName());

    private final Config config;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final GrpcTelemetry grpcTelemetry;
    private final CallCredentials callCredentials;
    private final NameResolverRegistry nameResolverRegistry;
    private final LoadBalancerRegistry loadBalancerRegistry;

    /**
     * Constructs a new GrpcClientManager with the specified configuration.
     *
     * @param config The application configuration
     * @param openTelemetry OpenTelemetry instance for distributed tracing
     * @param callCredentials Credentials for authenticating gRPC calls
     */
    @Inject
    public GrpcClientManager(Config config, OpenTelemetry openTelemetry, CallCredentials callCredentials) {
        this.config = config;
        this.grpcTelemetry = GrpcTelemetry.create(openTelemetry);
        this.callCredentials = callCredentials;
        this.nameResolverRegistry = NameResolverRegistry.getDefaultRegistry();
        this.loadBalancerRegistry = LoadBalancerRegistry.getDefaultRegistry();
        
        // Register DNS resolver if not already registered
        if (nameResolverRegistry.getProviders().isEmpty()) {
            nameResolverRegistry.register(new DnsNameResolverProvider());
        }
        
        // Register round-robin load balancer if not already registered
        if (!loadBalancerRegistry.getProviders().isEmpty() && 
            !loadBalancerRegistry.getProviders().containsKey("round_robin")) {
            loadBalancerRegistry.register(RoundRobinLoadBalancerFactory.getInstance());
        }
    }

    /**
     * Gets or creates a managed channel for the specified service.
     *
     * @param serviceName The name of the service to connect to
     * @return A managed channel for the service
     */
    public ManagedChannel getChannel(String serviceName) {
        return channels.computeIfAbsent(serviceName, this::createChannel);
    }

    /**
     * Creates a new managed channel for the specified service.
     *
     * @param serviceName The name of the service to connect to
     * @return A new managed channel for the service
     */
    private ManagedChannel createChannel(String serviceName) {
        String target = getServiceTarget(serviceName);
        boolean usePlaintext = config.getBoolean(Keys.GRPC_PREFIX.withPrefix(serviceName) + ".plaintext", true);
        int maxInboundMessageSize = config.getInteger(Keys.GRPC_PREFIX.withPrefix(serviceName) + ".maxInboundMessageSize", 4 * 1024 * 1024);
        int keepAliveTime = config.getInteger(Keys.GRPC_KEEPALIVE_TIME, 30);
        int keepAliveTimeout = config.getInteger(Keys.GRPC_KEEPALIVE_TIMEOUT, 10);
        int maxRetryAttempts = config.getInteger(Keys.GRPC_RETRY_ATTEMPTS, 5);
        int initialBackoffMs = config.getInteger(Keys.GRPC_INITIAL_BACKOFF_MS, 500);
        int maxBackoffMs = config.getInteger(Keys.GRPC_MAX_BACKOFF_MS, 15000);

        LOGGER.info(String.format("Creating gRPC channel to %s service at %s", serviceName, target));

        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(target)
                .intercept(grpcTelemetry.getClientInterceptor())
                .defaultLoadBalancingPolicy("round_robin")
                .enableRetry()
                .maxRetryAttempts(maxRetryAttempts)
                .initialRetryBackoffMillis(initialBackoffMs)
                .maxRetryBackoffMillis(maxBackoffMs)
                .keepAliveTime(keepAliveTime, TimeUnit.SECONDS)
                .keepAliveTimeout(keepAliveTimeout, TimeUnit.SECONDS)
                .maxInboundMessageSize(maxInboundMessageSize);

        if (usePlaintext) {
            builder.usePlaintext();
        }

        return builder.build();
    }
    
    /**
     * Gets the target string for the specified service.
     * This can be a direct host:port or a service discovery target.
     *
     * @param serviceName The name of the service
     * @return The target string for the service
     */
    private String getServiceTarget(String serviceName) {
        // Check if service discovery is enabled for this service
        boolean useServiceDiscovery = config.getBoolean(Keys.GRPC_PREFIX.withPrefix(serviceName) + ".useServiceDiscovery", false);
        
        if (useServiceDiscovery) {
            String discoveryScheme = config.getString(Keys.GRPC_PREFIX.withPrefix(serviceName) + ".discoveryScheme", "dns");
            String serviceDomain = config.getString(Keys.GRPC_PREFIX.withPrefix(serviceName) + ".serviceDomain", "service.consul");
            return discoveryScheme + "://" + serviceName + "." + serviceDomain;
        } else {
            String host = config.getString(Keys.GRPC_PREFIX.withPrefix(serviceName) + ".host", "localhost");
            int port = config.getInteger(Keys.GRPC_PREFIX.withPrefix(serviceName) + ".port", 0);
            return host + ":" + port;
        }
    }

    /**
     * Configures a gRPC stub with default settings.
     *
     * @param stub The stub to configure
     * @param <T> The type of the stub
     * @return The configured stub
     */
    public <T extends AbstractStub<T>> T configureStub(T stub) {
        return configureStub(stub, config.getInteger(Keys.GRPC_DEADLINE, 10));
    }
    
    /**
     * Configures a gRPC stub with the specified deadline.
     *
     * @param stub The stub to configure
     * @param deadlineSeconds The deadline in seconds
     * @param <T> The type of the stub
     * @return The configured stub
     */
    public <T extends AbstractStub<T>> T configureStub(T stub, int deadlineSeconds) {
        return stub.withCallCredentials(callCredentials)
                .withDeadlineAfter(deadlineSeconds, TimeUnit.SECONDS);
    }
    
    /**
     * Creates a deadline for a gRPC call.
     *
     * @param seconds The deadline in seconds
     * @return The deadline object
     */
    public Deadline createDeadline(int seconds) {
        return Deadline.after(seconds, TimeUnit.SECONDS);
    }

    /**
     * Executes a gRPC call with error handling.
     *
     * @param call The gRPC call to execute
     * @param <T> The return type of the call
     * @return The result of the call
     * @throws GrpcClientException If the call fails
     */
    public <T> T executeCall(GrpcCall<T> call) throws GrpcClientException {
        return executeCall(call, null);
    }
    
    /**
     * Executes a gRPC call with error handling and context information.
     *
     * @param call The gRPC call to execute
     * @param context Additional context information for logging
     * @param <T> The return type of the call
     * @return The result of the call
     * @throws GrpcClientException If the call fails
     */
    public <T> T executeCall(GrpcCall<T> call, String context) throws GrpcClientException {
        try {
            return call.execute();
        } catch (StatusRuntimeException e) {
            Status status = e.getStatus();
            String contextInfo = context != null ? " [" + context + "]" : "";
            String errorMessage = "gRPC call failed" + contextInfo + ": " + status.getCode() + " - " + status.getDescription();
            LOGGER.log(Level.WARNING, errorMessage, e);
            LogAction.exception(errorMessage, e);
            throw new GrpcClientException(errorMessage, e, status.getCode());
        } catch (Exception e) {
            String contextInfo = context != null ? " [" + context + "]" : "";
            String errorMessage = "Unexpected error in gRPC call" + contextInfo;
            LOGGER.log(Level.SEVERE, errorMessage, e);
            LogAction.exception(errorMessage, e);
            throw new GrpcClientException(errorMessage, e, Status.Code.UNKNOWN);
        }
    }

    /**
     * Functional interface for executing gRPC calls.
     *
     * @param <T> The return type of the call
     */
    @FunctionalInterface
    public interface GrpcCall<T> {
        T execute() throws Exception;
    }
    
    /**
     * Executes an asynchronous gRPC streaming call with error handling.
     *
     * @param responseObserver The observer to handle responses
     * @param call The streaming call to execute
     * @param <T> The response type
     */
    public <T> void executeStreamingCall(
            StreamObserver<T> responseObserver, 
            Consumer<StreamObserver<T>> call) {
        try {
            call.accept(responseObserver);
        } catch (StatusRuntimeException e) {
            Status status = e.getStatus();
            LOGGER.log(Level.WARNING, "gRPC streaming call failed: " + status.getCode() + " - " + status.getDescription(), e);
            responseObserver.onError(e);
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Unexpected error in gRPC streaming call", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Internal error during streaming call")
                    .withCause(e)
                    .asException());
        }
    }
    
    /**
     * Creates a CompletableFuture that will be completed when the StreamObserver receives a response.
     *
     * @param call The streaming call to execute
     * @param <T> The response type
     * @return A CompletableFuture that will be completed with the response
     */
    public <T> CompletableFuture<T> executeAsyncCall(Consumer<StreamObserver<T>> call) {
        CompletableFuture<T> future = new CompletableFuture<>();
        
        StreamObserver<T> responseObserver = new StreamObserver<T>() {
            private T response;
            
            @Override
            public void onNext(T value) {
                response = value;
            }
            
            @Override
            public void onError(Throwable t) {
                if (t instanceof StatusRuntimeException) {
                    StatusRuntimeException sre = (StatusRuntimeException) t;
                    future.completeExceptionally(new GrpcClientException(
                            "gRPC call failed: " + sre.getStatus().getCode(), 
                            t, 
                            sre.getStatus().getCode()));
                } else {
                    future.completeExceptionally(new GrpcClientException("Unexpected error in gRPC call", t));
                }
            }
            
            @Override
            public void onCompleted() {
                future.complete(response);
            }
        };
        
        executeStreamingCall(responseObserver, call);
        return future;
    }

    /**
     * Exception thrown when a gRPC client call fails.
     */
    public static class GrpcClientException extends Exception {
        private final Status.Code statusCode;
        
        public GrpcClientException(String message, Throwable cause) {
            super(message, cause);
            this.statusCode = Status.Code.UNKNOWN;
        }
        
        public GrpcClientException(String message, Throwable cause, Status.Code statusCode) {
            super(message, cause);
            this.statusCode = statusCode;
        }
        
        /**
         * Gets the gRPC status code associated with this exception.
         *
         * @return The gRPC status code
         */
        public Status.Code getStatusCode() {
            return statusCode;
        }
        
        /**
         * Checks if the exception represents a transient error that can be retried.
         *
         * @return true if the error is transient and can be retried, false otherwise
         */
        public boolean isTransient() {
            return statusCode == Status.Code.UNAVAILABLE || 
                   statusCode == Status.Code.RESOURCE_EXHAUSTED ||
                   statusCode == Status.Code.DEADLINE_EXCEEDED;
        }
    }

    /**
     * Shuts down all managed channels.
     */
    public void shutdown() {
        for (Map.Entry<String, ManagedChannel> entry : channels.entrySet()) {
            String serviceName = entry.getKey();
            ManagedChannel channel = entry.getValue();
            LOGGER.info("Shutting down gRPC channel to " + serviceName);
            try {
                // Initiate graceful shutdown
                channel.shutdown();
                
                // Wait for termination with timeout
                if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                    LOGGER.warning("Channel to " + serviceName + " did not terminate in time, forcing shutdown");
                    channel.shutdownNow();
                    
                    // Wait again, giving it a last chance to terminate
                    if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                        LOGGER.severe("Channel to " + serviceName + " could not be terminated");
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.log(Level.WARNING, "Error shutting down gRPC channel to " + serviceName, e);
                channel.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        channels.clear();
    }

    /**
     * Creates a Protocol Service client stub.
     *
     * @param <T> The type of the stub
     * @param stubFactory Factory method to create the stub
     * @return The configured stub
     */
    public <T extends AbstractStub<T>> T getProtocolServiceStub(StubFactory<T> stubFactory) {
        return configureStub(stubFactory.createStub(getChannel("protocol")));
    }

    /**
     * Creates a Position Service client stub.
     *
     * @param <T> The type of the stub
     * @param stubFactory Factory method to create the stub
     * @return The configured stub
     */
    public <T extends AbstractStub<T>> T getPositionServiceStub(StubFactory<T> stubFactory) {
        return configureStub(stubFactory.createStub(getChannel("position")));
    }

    /**
     * Creates an Event Service client stub.
     *
     * @param <T> The type of the stub
     * @param stubFactory Factory method to create the stub
     * @return The configured stub
     */
    public <T extends AbstractStub<T>> T getEventServiceStub(StubFactory<T> stubFactory) {
        return configureStub(stubFactory.createStub(getChannel("event")));
    }

    /**
     * Creates a Notification Service client stub.
     *
     * @param <T> The type of the stub
     * @param stubFactory Factory method to create the stub
     * @return The configured stub
     */
    public <T extends AbstractStub<T>> T getNotificationServiceStub(StubFactory<T> stubFactory) {
        return configureStub(stubFactory.createStub(getChannel("notification")));
    }

    /**
     * Creates a Reporting Service client stub.
     *
     * @param <T> The type of the stub
     * @param stubFactory Factory method to create the stub
     * @return The configured stub
     */
    public <T extends AbstractStub<T>> T getReportingServiceStub(StubFactory<T> stubFactory) {
        return configureStub(stubFactory.createStub(getChannel("reporting")));
    }

    /**
     * Functional interface for creating gRPC stubs.
     *
     * @param <T> The type of the stub
     */
    @FunctionalInterface
    public interface StubFactory<T extends AbstractStub<T>> {
        T createStub(Channel channel);
    }
    
    /**
     * Checks the health of a gRPC service.
     *
     * @param serviceName The name of the service to check
     * @return true if the service is healthy, false otherwise
     */
    public boolean isServiceHealthy(String serviceName) {
        try {
            ManagedChannel channel = getChannel(serviceName);
            HealthGrpc.HealthBlockingStub healthStub = configureStub(HealthGrpc.newBlockingStub(channel), 5);
            
            HealthCheckRequest request = HealthCheckRequest.newBuilder().build();
            HealthCheckResponse response = executeCall(() -> healthStub.check(request), "Health check for " + serviceName);
            
            return response.getStatus() == HealthCheckResponse.ServingStatus.SERVING;
        } catch (GrpcClientException e) {
            LOGGER.log(Level.WARNING, "Health check failed for service: " + serviceName, e);
            return false;
        }
    }
    
    /**
     * Gets the service configuration value for the specified key.
     *
     * @param serviceName The name of the service
     * @param key The configuration key
     * @param defaultValue The default value if the key is not found
     * @return The configuration value
     */
    public String getServiceConfig(String serviceName, String key, String defaultValue) {
        return config.getString(Keys.GRPC_PREFIX.withPrefix(serviceName) + "." + key, defaultValue);
    }
    
    /**
     * Gets the service configuration value for the specified key.
     *
     * @param serviceName The name of the service
     * @param key The configuration key
     * @param defaultValue The default value if the key is not found
     * @return The configuration value
     */
    public int getServiceConfig(String serviceName, String key, int defaultValue) {
        return config.getInteger(Keys.GRPC_PREFIX.withPrefix(serviceName) + "." + key, defaultValue);
    }
    
    /**
     * Gets the service configuration value for the specified key.
     *
     * @param serviceName The name of the service
     * @param key The configuration key
     * @param defaultValue The default value if the key is not found
     * @return The configuration value
     */
    public boolean getServiceConfig(String serviceName, String key, boolean defaultValue) {
        return config.getBoolean(Keys.GRPC_PREFIX.withPrefix(serviceName) + "." + key, defaultValue);
    }
}