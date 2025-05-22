import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Utility class that provides mocking capabilities for service clients in the microservices architecture.
 * Supports creating mock implementations of service clients (REST/gRPC), simulating various service responses
 * and failures, and verifying service interactions.
 */
public final class ServiceClientMockUtils {

    private ServiceClientMockUtils() {
        // Utility class, prevent instantiation
    }

    /**
     * Creates a mock REST client with configurable response behavior.
     *
     * @param clientClass The class of the REST client to mock
     * @param <T> The type of the REST client
     * @return A mock implementation of the REST client
     */
    public static <T> T mockRestClient(Class<T> clientClass) {
        return mock(clientClass);
    }

    /**
     * Creates a mock gRPC client with configurable response behavior.
     *
     * @param clientClass The class of the gRPC client to mock
     * @param <T> The type of the gRPC client
     * @return A mock implementation of the gRPC client
     */
    public static <T> T mockGrpcClient(Class<T> clientClass) {
        return mock(clientClass);
    }

    /**
     * Configures a REST client method to return a successful response.
     *
     * @param client The mock REST client
     * @param methodName The name of the method to configure
     * @param returnValue The value to return when the method is called
     * @param <T> The type of the REST client
     * @param <R> The return type of the method
     */
    @SuppressWarnings("unchecked")
    public static <T, R> void stubRestSuccess(T client, String methodName, R returnValue) {
        try {
            when(client.getClass().getMethod(methodName, any(Class.class)).invoke(client, any()))
                .thenReturn(CompletableFuture.completedFuture(returnValue));
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub REST client method: " + methodName, e);
        }
    }

    /**
     * Configures a REST client method to return a successful response with specific argument matchers.
     *
     * @param client The mock REST client
     * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
     * @param returnValue The value to return when the method is called
     * @param <T> The type of the REST client
     * @param <R> The return type of the method
     */
    @SuppressWarnings("unchecked")
    public static <T, R> void stubRestSuccess(T client, Function<T, CompletableFuture<R>> methodInvoker, R returnValue) {
        try {
            when(methodInvoker.apply(client)).thenReturn(CompletableFuture.completedFuture(returnValue));
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub REST client method", e);
        }
    }

    /**
     * Configures a REST client method to throw an exception.
     *
     * @param client The mock REST client
     * @param methodName The name of the method to configure
     * @param exception The exception to throw when the method is called
     * @param <T> The type of the REST client
     */
    @SuppressWarnings("unchecked")
    public static <T> void stubRestFailure(T client, String methodName, Throwable exception) {
        try {
            CompletableFuture<Object> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            when(client.getClass().getMethod(methodName, any(Class.class)).invoke(client, any()))
                .thenReturn(future);
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub REST client method: " + methodName, e);
        }
    }

    /**
     * Configures a REST client method to throw an exception with specific argument matchers.
     *
     * @param client The mock REST client
     * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
     * @param exception The exception to throw when the method is called
     * @param <T> The type of the REST client
     * @param <R> The return type of the method
     */
    @SuppressWarnings("unchecked")
    public static <T, R> void stubRestFailure(T client, Function<T, CompletableFuture<R>> methodInvoker, Throwable exception) {
        try {
            CompletableFuture<R> future = new CompletableFuture<>();
            future.completeExceptionally(exception);
            when(methodInvoker.apply(client)).thenReturn(future);
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub REST client method", e);
        }
    }

    /**
     * Configures a REST client method to simulate a timeout.
     *
     * @param client The mock REST client
     * @param methodName The name of the method to configure
     * @param <T> The type of the REST client
     */
    public static <T> void stubRestTimeout(T client, String methodName) {
        stubRestFailure(client, methodName, new TimeoutException("Service timeout"));
    }

    /**
     * Configures a REST client method to simulate a timeout with specific argument matchers.
     *
     * @param client The mock REST client
     * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
     * @param <T> The type of the REST client
     * @param <R> The return type of the method
     */
    public static <T, R> void stubRestTimeout(T client, Function<T, CompletableFuture<R>> methodInvoker) {
        stubRestFailure(client, methodInvoker, new TimeoutException("Service timeout"));
    }

    /**
     * Configures a REST client method to simulate a connection failure.
     *
     * @param client The mock REST client
     * @param methodName The name of the method to configure
     * @param <T> The type of the REST client
     */
    public static <T> void stubRestConnectionFailure(T client, String methodName) {
        stubRestFailure(client, methodName, new CompletionException(new ConnectException("Connection refused")));
    }

    /**
     * Configures a REST client method to simulate a connection failure with specific argument matchers.
     *
     * @param client The mock REST client
     * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
     * @param <T> The type of the REST client
     * @param <R> The return type of the method
     */
    public static <T, R> void stubRestConnectionFailure(T client, Function<T, CompletableFuture<R>> methodInvoker) {
        stubRestFailure(client, methodInvoker, new CompletionException(new ConnectException("Connection refused")));
    }

    /**
     * Configures a gRPC client method to return a successful response.
     *
     * @param client The mock gRPC client
     * @param methodName The name of the method to configure
     * @param request The expected request object
     * @param response The response to return when the method is called
     * @param <T> The type of the gRPC client
     * @param <Req> The type of the request object
     * @param <Resp> The type of the response object
     */
    @SuppressWarnings("unchecked")
    public static <T, Req, Resp> void stubGrpcSuccess(T client, String methodName, Req request, Resp response) {
        try {
            when(client.getClass().getMethod(methodName, request.getClass()).invoke(client, request))
                .thenReturn(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub gRPC client method: " + methodName, e);
        }
    }

    /**
     * Configures a gRPC client method to return a successful response with specific argument matchers.
     *
     * @param client The mock gRPC client
     * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
     * @param response The response to return when the method is called
     * @param <T> The type of the gRPC client
     * @param <Resp> The type of the response object
     */
    @SuppressWarnings("unchecked")
    public static <T, Resp> void stubGrpcSuccess(T client, Function<T, Resp> methodInvoker, Resp response) {
        try {
            when(methodInvoker.apply(client)).thenReturn(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub gRPC client method", e);
        }
    }

    /**
     * Configures a gRPC client method to throw an exception.
     *
     * @param client The mock gRPC client
     * @param methodName The name of the method to configure
     * @param request The expected request object
     * @param status The gRPC status to use for the exception
     * @param <T> The type of the gRPC client
     * @param <Req> The type of the request object
     */
    @SuppressWarnings("unchecked")
    public static <T, Req> void stubGrpcFailure(T client, String methodName, Req request, Status status) {
        try {
            when(client.getClass().getMethod(methodName, request.getClass()).invoke(client, request))
                .thenThrow(status.asRuntimeException());
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub gRPC client method: " + methodName, e);
        }
    }

    /**
     * Configures a gRPC client method to throw an exception with specific argument matchers.
     *
     * @param client The mock gRPC client
     * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
     * @param status The gRPC status to use for the exception
     * @param <T> The type of the gRPC client
     * @param <Resp> The type of the response object
     */
    @SuppressWarnings("unchecked")
    public static <T, Resp> void stubGrpcFailure(T client, Function<T, Resp> methodInvoker, Status status) {
        try {
            when(methodInvoker.apply(client)).thenThrow(status.asRuntimeException());
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub gRPC client method", e);
        }
    }

    /**
     * Configures a gRPC client method to simulate a timeout.
     *
     * @param client The mock gRPC client
     * @param methodName The name of the method to configure
     * @param request The expected request object
     * @param <T> The type of the gRPC client
     * @param <Req> The type of the request object
     */
    public static <T, Req> void stubGrpcTimeout(T client, String methodName, Req request) {
        stubGrpcFailure(client, methodName, request, Status.DEADLINE_EXCEEDED.withDescription("Deadline exceeded"));
    }

    /**
     * Configures a gRPC client method to simulate a timeout with specific argument matchers.
     *
     * @param client The mock gRPC client
     * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
     * @param <T> The type of the gRPC client
     * @param <Resp> The type of the response object
     */
    public static <T, Resp> void stubGrpcTimeout(T client, Function<T, Resp> methodInvoker) {
        stubGrpcFailure(client, methodInvoker, Status.DEADLINE_EXCEEDED.withDescription("Deadline exceeded"));
    }

    /**
     * Configures a gRPC client method to simulate a connection failure.
     *
     * @param client The mock gRPC client
     * @param methodName The name of the method to configure
     * @param request The expected request object
     * @param <T> The type of the gRPC client
     * @param <Req> The type of the request object
     */
    public static <T, Req> void stubGrpcConnectionFailure(T client, String methodName, Req request) {
        stubGrpcFailure(client, methodName, request, Status.UNAVAILABLE.withDescription("Connection refused"));
    }

    /**
     * Configures a gRPC client method to simulate a connection failure with specific argument matchers.
     *
     * @param client The mock gRPC client
     * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
     * @param <T> The type of the gRPC client
     * @param <Resp> The type of the response object
     */
    public static <T, Resp> void stubGrpcConnectionFailure(T client, Function<T, Resp> methodInvoker) {
        stubGrpcFailure(client, methodInvoker, Status.UNAVAILABLE.withDescription("Connection refused"));
    }

    /**
     * Configures a gRPC streaming method to return a successful response.
     *
     * @param client The mock gRPC client
     * @param methodName The name of the method to configure
     * @param request The expected request object
     * @param responseConsumer A consumer that will be called with the StreamObserver to send responses
     * @param <T> The type of the gRPC client
     * @param <Req> The type of the request object
     * @param <Resp> The type of the response object
     */
    @SuppressWarnings("unchecked")
    public static <T, Req, Resp> void stubGrpcStreamingSuccess(
            T client, String methodName, Req request, Consumer<StreamObserver<Resp>> responseConsumer) {
        try {
            doAnswer(invocation -> {
                StreamObserver<Resp> observer = invocation.getArgument(1);
                responseConsumer.accept(observer);
                return null;
            }).when(client).getClass().getMethod(methodName, request.getClass(), StreamObserver.class)
                .invoke(client, request, any(StreamObserver.class));
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub gRPC streaming method: " + methodName, e);
        }
    }

    /**
     * Configures a gRPC streaming method to throw an exception.
     *
     * @param client The mock gRPC client
     * @param methodName The name of the method to configure
     * @param request The expected request object
     * @param status The gRPC status to use for the exception
     * @param <T> The type of the gRPC client
     * @param <Req> The type of the request object
     * @param <Resp> The type of the response object
     */
    @SuppressWarnings("unchecked")
    public static <T, Req, Resp> void stubGrpcStreamingFailure(
            T client, String methodName, Req request, Status status) {
        try {
            doAnswer(invocation -> {
                StreamObserver<Resp> observer = invocation.getArgument(1);
                observer.onError(status.asRuntimeException());
                return null;
            }).when(client).getClass().getMethod(methodName, request.getClass(), StreamObserver.class)
                .invoke(client, request, any(StreamObserver.class));
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub gRPC streaming method: " + methodName, e);
        }
    }

    /**
     * Verifies that a REST client method was called with the expected arguments.
     *
     * @param client The mock REST client
     * @param methodName The name of the method to verify
     * @param times The number of times the method should have been called
     * @param <T> The type of the REST client
     */
    public static <T> void verifyRestCall(T client, String methodName, int times) {
        try {
            verify(client, times(times)).getClass().getMethod(methodName, any(Class.class)).invoke(client, any());
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify REST client method: " + methodName, e);
        }
    }

    /**
     * Verifies that a gRPC client method was called with the expected arguments.
     *
     * @param client The mock gRPC client
     * @param methodName The name of the method to verify
     * @param request The expected request object
     * @param times The number of times the method should have been called
     * @param <T> The type of the gRPC client
     * @param <Req> The type of the request object
     */
    public static <T, Req> void verifyGrpcCall(T client, String methodName, Req request, int times) {
        try {
            verify(client, times(times)).getClass().getMethod(methodName, request.getClass()).invoke(client, request);
        } catch (Exception e) {
            throw new RuntimeException("Failed to verify gRPC client method: " + methodName, e);
        }
    }

    /**
     * A recorder for service client interactions that can be used to record and replay service calls.
     *
     * @param <Req> The type of the request object
     * @param <Resp> The type of the response object
     */
    public static class ServiceInteractionRecorder<Req, Resp> {
        private final List<ServiceInteraction<Req, Resp>> interactions = new ArrayList<>();
        private final Map<Req, Resp> responseMap = new HashMap<>();
        private final Map<Req, Throwable> exceptionMap = new HashMap<>();

        /**
         * Records a successful service interaction.
         *
         * @param request The request object
         * @param response The response object
         */
        public void recordSuccess(Req request, Resp response) {
            interactions.add(new ServiceInteraction<>(request, response, null));
            responseMap.put(request, response);
        }

        /**
         * Records a failed service interaction.
         *
         * @param request The request object
         * @param exception The exception that was thrown
         */
        public void recordFailure(Req request, Throwable exception) {
            interactions.add(new ServiceInteraction<>(request, null, exception));
            exceptionMap.put(request, exception);
        }

        /**
         * Configures a REST client to replay the recorded interactions.
         *
         * @param client The mock REST client
         * @param methodName The name of the method to configure
         * @param <T> The type of the REST client
         */
        @SuppressWarnings("unchecked")
        public <T> void replayRestInteractions(T client, String methodName) {
            try {
                when(client.getClass().getMethod(methodName, any(Class.class)).invoke(client, any()))
                    .thenAnswer(invocation -> {
                        Req request = (Req) invocation.getArgument(0);
                        if (responseMap.containsKey(request)) {
                            return CompletableFuture.completedFuture(responseMap.get(request));
                        } else if (exceptionMap.containsKey(request)) {
                            CompletableFuture<Resp> future = new CompletableFuture<>();
                            future.completeExceptionally(exceptionMap.get(request));
                            return future;
                        } else {
                            throw new IllegalArgumentException("No recorded interaction for request: " + request);
                        }
                    });
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure REST client for replay: " + methodName, e);
            }
        }

        /**
         * Configures a gRPC client to replay the recorded interactions.
         *
         * @param client The mock gRPC client
         * @param methodName The name of the method to configure
         * @param <T> The type of the gRPC client
         */
        @SuppressWarnings("unchecked")
        public <T> void replayGrpcInteractions(T client, String methodName) {
            try {
                when(client.getClass().getMethod(methodName, any(Class.class)).invoke(client, any()))
                    .thenAnswer(invocation -> {
                        Req request = (Req) invocation.getArgument(0);
                        if (responseMap.containsKey(request)) {
                            return responseMap.get(request);
                        } else if (exceptionMap.containsKey(request)) {
                            throw exceptionMap.get(request);
                        } else {
                            throw new IllegalArgumentException("No recorded interaction for request: " + request);
                        }
                    });
            } catch (Exception e) {
                throw new RuntimeException("Failed to configure gRPC client for replay: " + methodName, e);
            }
        }

        /**
         * Returns all recorded interactions.
         *
         * @return A list of all recorded interactions
         */
        public List<ServiceInteraction<Req, Resp>> getInteractions() {
            return new ArrayList<>(interactions);
        }

        /**
         * Clears all recorded interactions.
         */
        public void clear() {
            interactions.clear();
            responseMap.clear();
            exceptionMap.clear();
        }
    }

    /**
     * Represents a single service interaction (request/response pair).
     *
     * @param <Req> The type of the request object
     * @param <Resp> The type of the response object
     */
    public static class ServiceInteraction<Req, Resp> {
        private final Req request;
        private final Resp response;
        private final Throwable exception;

        /**
         * Creates a new service interaction.
         *
         * @param request The request object
         * @param response The response object (null if the interaction failed)
         * @param exception The exception that was thrown (null if the interaction succeeded)
         */
        public ServiceInteraction(Req request, Resp response, Throwable exception) {
            this.request = request;
            this.response = response;
            this.exception = exception;
        }

        /**
         * Returns the request object.
         *
         * @return The request object
         */
        public Req getRequest() {
            return request;
        }

        /**
         * Returns the response object.
         *
         * @return The response object (null if the interaction failed)
         */
        public Resp getResponse() {
            return response;
        }

        /**
         * Returns the exception that was thrown.
         *
         * @return The exception that was thrown (null if the interaction succeeded)
         */
        public Throwable getException() {
            return exception;
        }

        /**
         * Returns whether the interaction succeeded.
         *
         * @return true if the interaction succeeded, false otherwise
         */
        public boolean isSuccess() {
            return exception == null;
        }
    }

    /**
     * Creates a mock factory for REST clients.
     *
     * @param factoryClass The class of the factory to mock
     * @param <T> The type of the factory
     * @return A mock implementation of the factory
     */
    public static <T> T mockRestClientFactory(Class<T> factoryClass) {
        return mock(factoryClass);
    }

    /**
     * Creates a mock factory for gRPC clients.
     *
     * @param factoryClass The class of the factory to mock
     * @param <T> The type of the factory
     * @return A mock implementation of the factory
     */
    public static <T> T mockGrpcClientFactory(Class<T> factoryClass) {
        return mock(factoryClass);
    }

    /**
     * Configures a REST client factory to return a specific mock client.
     *
     * @param factory The mock factory
     * @param methodName The name of the factory method to configure
     * @param client The mock client to return
     * @param <F> The type of the factory
     * @param <C> The type of the client
     */
    @SuppressWarnings("unchecked")
    public static <F, C> void stubRestClientFactory(F factory, String methodName, C client) {
        try {
            when(factory.getClass().getMethod(methodName).invoke(factory)).thenReturn(client);
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub REST client factory method: " + methodName, e);
        }
    }

    /**
     * Configures a gRPC client factory to return a specific mock client.
     *
     * @param factory The mock factory
     * @param methodName The name of the factory method to configure
     * @param client The mock client to return
     * @param <F> The type of the factory
     * @param <C> The type of the client
     */
    @SuppressWarnings("unchecked")
    public static <F, C> void stubGrpcClientFactory(F factory, String methodName, C client) {
        try {
            when(factory.getClass().getMethod(methodName).invoke(factory)).thenReturn(client);
        } catch (Exception e) {
            throw new RuntimeException("Failed to stub gRPC client factory method: " + methodName, e);
        }
    }

    /**
     * Creates a test fixture for a common service client pattern.
     *
     * @param clientClass The class of the client to mock
     * @param <T> The type of the client
     * @return A test fixture for the client
     */
    public static <T> ServiceClientFixture<T> createFixture(Class<T> clientClass) {
        return new ServiceClientFixture<>(clientClass);
    }

    /**
     * A test fixture for a service client that provides common testing patterns.
     *
     * @param <T> The type of the client
     */
    public static class ServiceClientFixture<T> {
        private final T client;
        private final List<ServiceInteraction<Object, Object>> interactions = new ArrayList<>();

        /**
         * Creates a new service client fixture.
         *
         * @param clientClass The class of the client to mock
         */
        @SuppressWarnings("unchecked")
        public ServiceClientFixture(Class<T> clientClass) {
            this.client = (T) mock(clientClass);
        }

        /**
         * Returns the mock client.
         *
         * @return The mock client
         */
        public T getClient() {
            return client;
        }

        /**
         * Configures the client to return a successful response for a REST method.
         *
         * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
         * @param returnValue The value to return when the method is called
         * @param <R> The return type of the method
         * @return This fixture for method chaining
         */
        @SuppressWarnings("unchecked")
        public <R> ServiceClientFixture<T> withRestSuccess(Function<T, CompletableFuture<R>> methodInvoker, R returnValue) {
            stubRestSuccess(client, methodInvoker, returnValue);
            return this;
        }

        /**
         * Configures the client to throw an exception for a REST method.
         *
         * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
         * @param exception The exception to throw when the method is called
         * @param <R> The return type of the method
         * @return This fixture for method chaining
         */
        @SuppressWarnings("unchecked")
        public <R> ServiceClientFixture<T> withRestFailure(Function<T, CompletableFuture<R>> methodInvoker, Throwable exception) {
            stubRestFailure(client, methodInvoker, exception);
            return this;
        }

        /**
         * Configures the client to simulate a timeout for a REST method.
         *
         * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
         * @param <R> The return type of the method
         * @return This fixture for method chaining
         */
        @SuppressWarnings("unchecked")
        public <R> ServiceClientFixture<T> withRestTimeout(Function<T, CompletableFuture<R>> methodInvoker) {
            stubRestTimeout(client, methodInvoker);
            return this;
        }

        /**
         * Configures the client to return a successful response for a gRPC method.
         *
         * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
         * @param response The response to return when the method is called
         * @param <R> The type of the response object
         * @return This fixture for method chaining
         */
        @SuppressWarnings("unchecked")
        public <R> ServiceClientFixture<T> withGrpcSuccess(Function<T, R> methodInvoker, R response) {
            stubGrpcSuccess(client, methodInvoker, response);
            return this;
        }

        /**
         * Configures the client to throw an exception for a gRPC method.
         *
         * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
         * @param status The gRPC status to use for the exception
         * @param <R> The type of the response object
         * @return This fixture for method chaining
         */
        @SuppressWarnings("unchecked")
        public <R> ServiceClientFixture<T> withGrpcFailure(Function<T, R> methodInvoker, Status status) {
            stubGrpcFailure(client, methodInvoker, status);
            return this;
        }

        /**
         * Configures the client to simulate a timeout for a gRPC method.
         *
         * @param methodInvoker A function that invokes the method on the client with the desired argument matchers
         * @param <R> The type of the response object
         * @return This fixture for method chaining
         */
        @SuppressWarnings("unchecked")
        public <R> ServiceClientFixture<T> withGrpcTimeout(Function<T, R> methodInvoker) {
            stubGrpcTimeout(client, methodInvoker);
            return this;
        }

        /**
         * Records an interaction with the client.
         *
         * @param request The request object
         * @param response The response object
         * @return This fixture for method chaining
         */
        @SuppressWarnings("unchecked")
        public ServiceClientFixture<T> recordInteraction(Object request, Object response) {
            interactions.add(new ServiceInteraction<>(request, response, null));
            return this;
        }

        /**
         * Records a failed interaction with the client.
         *
         * @param request The request object
         * @param exception The exception that was thrown
         * @return This fixture for method chaining
         */
        @SuppressWarnings("unchecked")
        public ServiceClientFixture<T> recordFailedInteraction(Object request, Throwable exception) {
            interactions.add(new ServiceInteraction<>(request, null, exception));
            return this;
        }

        /**
         * Returns all recorded interactions.
         *
         * @return A list of all recorded interactions
         */
        @SuppressWarnings("unchecked")
        public List<ServiceInteraction<Object, Object>> getInteractions() {
            return new ArrayList<>(interactions);
        }

        /**
         * Clears all recorded interactions.
         *
         * @return This fixture for method chaining
         */
        public ServiceClientFixture<T> clearInteractions() {
            interactions.clear();
            return this;
        }

        /**
         * Resets the mock client, clearing all stubbed methods and interactions.
         *
         * @return This fixture for method chaining
         */
        public ServiceClientFixture<T> reset() {
            clearInteractions();
            Mockito.reset(client);
            return this;
        }
    }
}