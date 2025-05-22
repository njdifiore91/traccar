package org.traccar;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Provides utilities for testing service client interactions across microservices.
 * This class supports mocking and verification of both gRPC and REST service clients,
 * enabling isolated testing of components that depend on other services.
 */
@ExtendWith(MockitoExtension.class)
public class ServiceClientTest {

    /**
     * Represents a mock gRPC service client with configurable behavior.
     * @param <RequestT> The request type for the gRPC method
     * @param <ResponseT> The response type for the gRPC method
     */
    public static class MockGrpcServiceClient<RequestT, ResponseT> {
        private final Map<String, Function<RequestT, ResponseT>> methodResponses = new HashMap<>();
        private final Map<String, Function<RequestT, StatusRuntimeException>> methodExceptions = new HashMap<>();
        private final Map<String, List<RequestT>> methodRequests = new HashMap<>();

        /**
         * Configures a successful response for a specific gRPC method.
         * @param methodName The name of the gRPC method
         * @param responseFunction A function that generates a response based on the request
         * @return This MockGrpcServiceClient instance for method chaining
         */
        public MockGrpcServiceClient<RequestT, ResponseT> withSuccessResponse(String methodName, Function<RequestT, ResponseT> responseFunction) {
            methodResponses.put(methodName, responseFunction);
            return this;
        }

        /**
         * Configures a static successful response for a specific gRPC method.
         * @param methodName The name of the gRPC method
         * @param response The static response to return
         * @return This MockGrpcServiceClient instance for method chaining
         */
        public MockGrpcServiceClient<RequestT, ResponseT> withSuccessResponse(String methodName, ResponseT response) {
            return withSuccessResponse(methodName, request -> response);
        }

        /**
         * Configures an error response for a specific gRPC method.
         * @param methodName The name of the gRPC method
         * @param status The gRPC status to return in the exception
         * @return This MockGrpcServiceClient instance for method chaining
         */
        public MockGrpcServiceClient<RequestT, ResponseT> withErrorResponse(String methodName, Status status) {
            methodExceptions.put(methodName, request -> status.asRuntimeException());
            return this;
        }

        /**
         * Configures a dynamic error response for a specific gRPC method.
         * @param methodName The name of the gRPC method
         * @param exceptionFunction A function that generates an exception based on the request
         * @return This MockGrpcServiceClient instance for method chaining
         */
        public MockGrpcServiceClient<RequestT, ResponseT> withErrorResponse(String methodName, Function<RequestT, StatusRuntimeException> exceptionFunction) {
            methodExceptions.put(methodName, exceptionFunction);
            return this;
        }

        /**
         * Handles a gRPC method call with the configured behavior.
         * @param methodName The name of the gRPC method being called
         * @param request The request object
         * @param responseObserver The observer for the response
         */
        public void handleMethod(String methodName, RequestT request, StreamObserver<ResponseT> responseObserver) {
            recordRequest(methodName, request);
            
            if (methodExceptions.containsKey(methodName)) {
                StatusRuntimeException exception = methodExceptions.get(methodName).apply(request);
                responseObserver.onError(exception);
                return;
            }
            
            if (methodResponses.containsKey(methodName)) {
                ResponseT response = methodResponses.get(methodName).apply(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            }
            
            // Default behavior if no configuration is provided
            responseObserver.onError(Status.UNIMPLEMENTED.withDescription("Method not configured: " + methodName).asRuntimeException());
        }

        /**
         * Records a request for later verification.
         * @param methodName The name of the gRPC method
         * @param request The request object
         */
        private void recordRequest(String methodName, RequestT request) {
            methodRequests.computeIfAbsent(methodName, k -> new ArrayList<>()).add(request);
        }

        /**
         * Verifies that a specific method was called with the expected request.
         * @param methodName The name of the gRPC method
         * @param expectedRequest The expected request object
         * @throws AssertionError if the method was not called with the expected request
         */
        public void verifyMethodCalled(String methodName, RequestT expectedRequest) {
            List<RequestT> requests = methodRequests.getOrDefault(methodName, List.of());
            if (requests.isEmpty()) {
                throw new AssertionError("Method " + methodName + " was not called");
            }
            
            boolean found = requests.stream().anyMatch(request -> request.equals(expectedRequest));
            if (!found) {
                throw new AssertionError("Method " + methodName + " was not called with expected request: " + expectedRequest);
            }
        }

        /**
         * Verifies that a specific method was called with a request matching the given predicate.
         * @param methodName The name of the gRPC method
         * @param requestMatcher A predicate to match the request
         * @throws AssertionError if the method was not called with a matching request
         */
        public void verifyMethodCalled(String methodName, java.util.function.Predicate<RequestT> requestMatcher) {
            List<RequestT> requests = methodRequests.getOrDefault(methodName, List.of());
            if (requests.isEmpty()) {
                throw new AssertionError("Method " + methodName + " was not called");
            }
            
            boolean found = requests.stream().anyMatch(requestMatcher);
            if (!found) {
                throw new AssertionError("Method " + methodName + " was not called with a matching request");
            }
        }

        /**
         * Verifies that a specific method was called exactly the expected number of times.
         * @param methodName The name of the gRPC method
         * @param expectedCount The expected number of calls
         * @throws AssertionError if the method was not called the expected number of times
         */
        public void verifyMethodCalledTimes(String methodName, int expectedCount) {
            List<RequestT> requests = methodRequests.getOrDefault(methodName, List.of());
            if (requests.size() != expectedCount) {
                throw new AssertionError("Method " + methodName + " was called " + requests.size() + 
                                        " times, expected " + expectedCount);
            }
        }

        /**
         * Gets all recorded requests for a specific method.
         * @param methodName The name of the gRPC method
         * @return A list of all recorded requests for the method
         */
        public List<RequestT> getRecordedRequests(String methodName) {
            return new ArrayList<>(methodRequests.getOrDefault(methodName, List.of()));
        }
    }

    /**
     * Represents a mock REST service client with configurable behavior.
     * @param <RequestT> The request type for the REST method
     * @param <ResponseT> The response type for the REST method
     */
    public static class MockRestServiceClient<RequestT, ResponseT> {
        private final Map<String, Function<RequestT, ResponseT>> endpointResponses = new HashMap<>();
        private final Map<String, Function<RequestT, Exception>> endpointExceptions = new HashMap<>();
        private final Map<String, List<RequestT>> endpointRequests = new HashMap<>();

        /**
         * Configures a successful response for a specific REST endpoint.
         * @param endpoint The REST endpoint path
         * @param responseFunction A function that generates a response based on the request
         * @return This MockRestServiceClient instance for method chaining
         */
        public MockRestServiceClient<RequestT, ResponseT> withSuccessResponse(String endpoint, Function<RequestT, ResponseT> responseFunction) {
            endpointResponses.put(endpoint, responseFunction);
            return this;
        }

        /**
         * Configures a static successful response for a specific REST endpoint.
         * @param endpoint The REST endpoint path
         * @param response The static response to return
         * @return This MockRestServiceClient instance for method chaining
         */
        public MockRestServiceClient<RequestT, ResponseT> withSuccessResponse(String endpoint, ResponseT response) {
            return withSuccessResponse(endpoint, request -> response);
        }

        /**
         * Configures an error response for a specific REST endpoint.
         * @param endpoint The REST endpoint path
         * @param exception The exception to throw
         * @return This MockRestServiceClient instance for method chaining
         */
        public MockRestServiceClient<RequestT, ResponseT> withErrorResponse(String endpoint, Exception exception) {
            endpointExceptions.put(endpoint, request -> exception);
            return this;
        }

        /**
         * Configures a dynamic error response for a specific REST endpoint.
         * @param endpoint The REST endpoint path
         * @param exceptionFunction A function that generates an exception based on the request
         * @return This MockRestServiceClient instance for method chaining
         */
        public MockRestServiceClient<RequestT, ResponseT> withErrorResponse(String endpoint, Function<RequestT, Exception> exceptionFunction) {
            endpointExceptions.put(endpoint, exceptionFunction);
            return this;
        }

        /**
         * Handles a REST endpoint call with the configured behavior.
         * @param endpoint The REST endpoint path
         * @param request The request object
         * @return A CompletableFuture containing the response or exception
         */
        public CompletableFuture<ResponseT> handleEndpoint(String endpoint, RequestT request) {
            recordRequest(endpoint, request);
            
            if (endpointExceptions.containsKey(endpoint)) {
                Exception exception = endpointExceptions.get(endpoint).apply(request);
                CompletableFuture<ResponseT> future = new CompletableFuture<>();
                future.completeExceptionally(exception);
                return future;
            }
            
            if (endpointResponses.containsKey(endpoint)) {
                ResponseT response = endpointResponses.get(endpoint).apply(request);
                return CompletableFuture.completedFuture(response);
            }
            
            // Default behavior if no configuration is provided
            CompletableFuture<ResponseT> future = new CompletableFuture<>();
            future.completeExceptionally(new IllegalStateException("Endpoint not configured: " + endpoint));
            return future;
        }

        /**
         * Records a request for later verification.
         * @param endpoint The REST endpoint path
         * @param request The request object
         */
        private void recordRequest(String endpoint, RequestT request) {
            endpointRequests.computeIfAbsent(endpoint, k -> new ArrayList<>()).add(request);
        }

        /**
         * Verifies that a specific endpoint was called with the expected request.
         * @param endpoint The REST endpoint path
         * @param expectedRequest The expected request object
         * @throws AssertionError if the endpoint was not called with the expected request
         */
        public void verifyEndpointCalled(String endpoint, RequestT expectedRequest) {
            List<RequestT> requests = endpointRequests.getOrDefault(endpoint, List.of());
            if (requests.isEmpty()) {
                throw new AssertionError("Endpoint " + endpoint + " was not called");
            }
            
            boolean found = requests.stream().anyMatch(request -> request.equals(expectedRequest));
            if (!found) {
                throw new AssertionError("Endpoint " + endpoint + " was not called with expected request: " + expectedRequest);
            }
        }

        /**
         * Verifies that a specific endpoint was called with a request matching the given predicate.
         * @param endpoint The REST endpoint path
         * @param requestMatcher A predicate to match the request
         * @throws AssertionError if the endpoint was not called with a matching request
         */
        public void verifyEndpointCalled(String endpoint, java.util.function.Predicate<RequestT> requestMatcher) {
            List<RequestT> requests = endpointRequests.getOrDefault(endpoint, List.of());
            if (requests.isEmpty()) {
                throw new AssertionError("Endpoint " + endpoint + " was not called");
            }
            
            boolean found = requests.stream().anyMatch(requestMatcher);
            if (!found) {
                throw new AssertionError("Endpoint " + endpoint + " was not called with a matching request");
            }
        }

        /**
         * Verifies that a specific endpoint was called exactly the expected number of times.
         * @param endpoint The REST endpoint path
         * @param expectedCount The expected number of calls
         * @throws AssertionError if the endpoint was not called the expected number of times
         */
        public void verifyEndpointCalledTimes(String endpoint, int expectedCount) {
            List<RequestT> requests = endpointRequests.getOrDefault(endpoint, List.of());
            if (requests.size() != expectedCount) {
                throw new AssertionError("Endpoint " + endpoint + " was called " + requests.size() + 
                                        " times, expected " + expectedCount);
            }
        }

        /**
         * Gets all recorded requests for a specific endpoint.
         * @param endpoint The REST endpoint path
         * @return A list of all recorded requests for the endpoint
         */
        public List<RequestT> getRecordedRequests(String endpoint) {
            return new ArrayList<>(endpointRequests.getOrDefault(endpoint, List.of()));
        }
    }

    /**
     * Creates a mock gRPC service client with configurable behavior.
     * @param <RequestT> The request type for the gRPC method
     * @param <ResponseT> The response type for the gRPC method
     * @return A new MockGrpcServiceClient instance
     */
    public static <RequestT, ResponseT> MockGrpcServiceClient<RequestT, ResponseT> createMockGrpcServiceClient() {
        return new MockGrpcServiceClient<>();
    }

    /**
     * Creates a mock REST service client with configurable behavior.
     * @param <RequestT> The request type for the REST method
     * @param <ResponseT> The response type for the REST method
     * @return A new MockRestServiceClient instance
     */
    public static <RequestT, ResponseT> MockRestServiceClient<RequestT, ResponseT> createMockRestServiceClient() {
        return new MockRestServiceClient<>();
    }

    /**
     * Creates a mock gRPC channel that can be used to inject mock stubs.
     * @param cleanupRule The GrpcCleanupRule to register the channel with
     * @return A mock ManagedChannel
     */
    public static ManagedChannel createMockChannel(GrpcCleanupRule cleanupRule) {
        ManagedChannel channel = mock(ManagedChannel.class);
        when(channel.shutdown()).thenReturn(channel);
        when(channel.awaitTermination(anyLong(), any(TimeUnit.class))).thenReturn(true);
        if (cleanupRule != null) {
            cleanupRule.register(channel);
        }
        return channel;
    }

    /**
     * Simulates a circuit breaker failure by throwing a StatusRuntimeException with UNAVAILABLE status.
     * @param methodName The name of the method that is failing
     * @return A StatusRuntimeException with UNAVAILABLE status
     */
    public static StatusRuntimeException simulateCircuitBreakerFailure(String methodName) {
        return Status.UNAVAILABLE
                .withDescription("Circuit breaker open for method: " + methodName)
                .asRuntimeException();
    }

    /**
     * Simulates a timeout failure by throwing a StatusRuntimeException with DEADLINE_EXCEEDED status.
     * @param methodName The name of the method that is timing out
     * @return A StatusRuntimeException with DEADLINE_EXCEEDED status
     */
    public static StatusRuntimeException simulateTimeoutFailure(String methodName) {
        return Status.DEADLINE_EXCEEDED
                .withDescription("Timeout exceeded for method: " + methodName)
                .asRuntimeException();
    }

    /**
     * Simulates a service discovery failure by throwing a StatusRuntimeException with UNAVAILABLE status.
     * @param serviceName The name of the service that cannot be discovered
     * @return A StatusRuntimeException with UNAVAILABLE status
     */
    public static StatusRuntimeException simulateServiceDiscoveryFailure(String serviceName) {
        return Status.UNAVAILABLE
                .withDescription("Service discovery failed for service: " + serviceName)
                .asRuntimeException();
    }

    /**
     * Simulates an authentication failure by throwing a StatusRuntimeException with UNAUTHENTICATED status.
     * @return A StatusRuntimeException with UNAUTHENTICATED status
     */
    public static StatusRuntimeException simulateAuthenticationFailure() {
        return Status.UNAUTHENTICATED
                .withDescription("Authentication failed")
                .asRuntimeException();
    }

    /**
     * Simulates an authorization failure by throwing a StatusRuntimeException with PERMISSION_DENIED status.
     * @return A StatusRuntimeException with PERMISSION_DENIED status
     */
    public static StatusRuntimeException simulateAuthorizationFailure() {
        return Status.PERMISSION_DENIED
                .withDescription("Authorization failed")
                .asRuntimeException();
    }

    /**
     * Creates a mock StreamObserver that captures the response or error.
     * @param <T> The type of response expected
     * @return A mock StreamObserver
     */
    @SuppressWarnings("unchecked")
    public static <T> StreamObserver<T> createMockStreamObserver() {
        return mock(StreamObserver.class);
    }

    /**
     * Creates a mock StreamObserver that captures the response or error and allows verification.
     * @param <T> The type of response expected
     * @return A mock StreamObserver with verification capabilities
     */
    public static <T> StreamObserver<T> createVerifiableStreamObserver() {
        StreamObserver<T> mockObserver = createMockStreamObserver();
        
        // Capture arguments for verification
        ArgumentCaptor<T> responseCaptor = ArgumentCaptor.forClass((Class<T>) Object.class);
        ArgumentCaptor<Throwable> errorCaptor = ArgumentCaptor.forClass(Throwable.class);
        
        doNothing().when(mockObserver).onNext(responseCaptor.capture());
        doNothing().when(mockObserver).onError(errorCaptor.capture());
        doNothing().when(mockObserver).onCompleted();
        
        return mockObserver;
    }

    /**
     * Verifies that a CompletableFuture completes with the expected result.
     * @param future The CompletableFuture to verify
     * @param expectedResult The expected result
     * @param <T> The type of result expected
     * @throws AssertionError if the future does not complete with the expected result
     */
    public static <T> void verifyFutureResult(CompletableFuture<T> future, T expectedResult) {
        try {
            T result = future.join();
            if (!expectedResult.equals(result)) {
                throw new AssertionError("Expected future to complete with " + expectedResult + 
                                        " but got " + result);
            }
        } catch (CompletionException e) {
            throw new AssertionError("Expected future to complete normally but it completed exceptionally", e);
        }
    }

    /**
     * Verifies that a CompletableFuture completes exceptionally with an exception of the expected type.
     * @param future The CompletableFuture to verify
     * @param expectedExceptionType The expected exception type
     * @param <T> The type of result expected
     * @throws AssertionError if the future does not complete exceptionally with the expected exception type
     */
    public static <T> void verifyFutureException(CompletableFuture<T> future, Class<? extends Throwable> expectedExceptionType) {
        try {
            T result = future.join();
            throw new AssertionError("Expected future to complete exceptionally but it completed normally with " + result);
        } catch (CompletionException e) {
            if (!expectedExceptionType.isInstance(e.getCause())) {
                throw new AssertionError("Expected future to complete exceptionally with " + 
                                        expectedExceptionType.getSimpleName() + 
                                        " but it completed with " + e.getCause().getClass().getSimpleName(), e);
            }
        }
    }

    /**
     * Creates a mock service discovery client that returns the specified service endpoints.
     * @param serviceEndpoints A map of service names to their endpoints
     * @return A function that simulates service discovery lookups
     */
    public static Function<String, String> createMockServiceDiscovery(Map<String, String> serviceEndpoints) {
        return serviceName -> {
            if (!serviceEndpoints.containsKey(serviceName)) {
                throw new IllegalStateException("Service not found: " + serviceName);
            }
            return serviceEndpoints.get(serviceName);
        };
    }

    /**
     * Creates a mock authentication token provider for service-to-service authentication.
     * @param validTokens A map of service names to their valid authentication tokens
     * @return A function that provides authentication tokens for services
     */
    public static Function<String, String> createMockAuthTokenProvider(Map<String, String> validTokens) {
        return serviceName -> {
            if (!validTokens.containsKey(serviceName)) {
                throw new IllegalStateException("No authentication token available for service: " + serviceName);
            }
            return validTokens.get(serviceName);
        };
    }

    /**
     * Creates a mock circuit breaker that can be configured to allow or block calls.
     * @param <T> The type of result expected from the protected call
     * @return A function that simulates a circuit breaker
     */
    public static <T> Function<Runnable, T> createMockCircuitBreaker(boolean isOpen) {
        return protectedCall -> {
            if (isOpen) {
                throw new StatusRuntimeException(Status.UNAVAILABLE.withDescription("Circuit breaker is open"));
            }
            
            try {
                protectedCall.run();
                return null; // This would be the result in a real implementation
            } catch (Exception e) {
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                }
                throw new RuntimeException("Error in protected call", e);
            }
        };
    }

    /**
     * Creates a mock retry mechanism that can be configured to retry a specified number of times.
     * @param maxRetries The maximum number of retries to simulate
     * @param shouldSucceedOnRetry Whether the operation should succeed after retries
     * @return A function that simulates a retry mechanism
     */
    public static Function<Runnable, Void> createMockRetryMechanism(int maxRetries, boolean shouldSucceedOnRetry) {
        return protectedCall -> {
            Exception lastException = null;
            
            for (int i = 0; i <= maxRetries; i++) {
                try {
                    if (i == maxRetries && shouldSucceedOnRetry) {
                        // Succeed on the last retry if configured to do so
                        protectedCall.run();
                        return null;
                    } else if (!shouldSucceedOnRetry) {
                        // Always fail if configured to do so
                        throw new StatusRuntimeException(Status.INTERNAL.withDescription("Simulated failure"));
                    } else {
                        // Fail on all but the last retry
                        throw new StatusRuntimeException(Status.INTERNAL.withDescription("Simulated failure on retry " + i));
                    }
                } catch (Exception e) {
                    lastException = e;
                    // In a real implementation, there would be a delay here
                }
            }
            
            // If we get here, all retries failed
            if (lastException instanceof RuntimeException) {
                throw (RuntimeException) lastException;
            }
            throw new RuntimeException("All retries failed", lastException);
        };
    }

    /**
     * Creates a mock bulkhead that limits concurrent executions.
     * @param maxConcurrentCalls The maximum number of concurrent calls to allow
     * @return A function that simulates a bulkhead
     */
    public static Function<Runnable, Void> createMockBulkhead(int maxConcurrentCalls) {
        return new Function<>() {
            private int currentCalls = 0;
            
            @Override
            public Void apply(Runnable protectedCall) {
                synchronized (this) {
                    if (currentCalls >= maxConcurrentCalls) {
                        throw new StatusRuntimeException(Status.RESOURCE_EXHAUSTED
                                .withDescription("Bulkhead capacity exceeded"));
                    }
                    currentCalls++;
                }
                
                try {
                    protectedCall.run();
                    return null;
                } finally {
                    synchronized (this) {
                        currentCalls--;
                    }
                }
            }
        };
    }

    /**
     * Creates a mock timeout mechanism that simulates operation timeouts.
     * @param shouldTimeout Whether the operation should timeout
     * @return A function that simulates a timeout mechanism
     */
    public static Function<Runnable, Void> createMockTimeout(boolean shouldTimeout) {
        return protectedCall -> {
            if (shouldTimeout) {
                throw new StatusRuntimeException(Status.DEADLINE_EXCEEDED
                        .withDescription("Operation timed out"));
            }
            
            protectedCall.run();
            return null;
        };
    }

    /**
     * Creates a mock rate limiter that can be configured to allow or block calls based on rate limits.
     * @param isRateLimited Whether the operation should be rate limited
     * @return A function that simulates a rate limiter
     */
    public static Function<Runnable, Void> createMockRateLimiter(boolean isRateLimited) {
        return protectedCall -> {
            if (isRateLimited) {
                throw new StatusRuntimeException(Status.RESOURCE_EXHAUSTED
                        .withDescription("Rate limit exceeded"));
            }
            
            protectedCall.run();
            return null;
        };
    }

    /**
     * Creates a composite resilience pattern that combines multiple resilience mechanisms.
     * @param circuitBreaker The circuit breaker function
     * @param bulkhead The bulkhead function
     * @param timeout The timeout function
     * @param rateLimiter The rate limiter function
     * @return A function that applies all resilience patterns in sequence
     */
    public static Function<Runnable, Void> createCompositeResilience(
            Function<Runnable, Void> circuitBreaker,
            Function<Runnable, Void> bulkhead,
            Function<Runnable, Void> timeout,
            Function<Runnable, Void> rateLimiter) {
        
        return protectedCall -> {
            // Apply resilience patterns in sequence
            circuitBreaker.apply(() -> {
                bulkhead.apply(() -> {
                    timeout.apply(() -> {
                        rateLimiter.apply(protectedCall);
                    });
                });
            });
            
            return null;
        };
    }
}