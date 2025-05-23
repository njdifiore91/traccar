/*
 * Copyright 2015 - 2025 Anton Tananaev (anton@traccar.org)
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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceInstance;
import org.traccar.discovery.ServiceRegistry;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Client for connecting to remote services in the microservices architecture.
 * Includes service discovery, circuit breaker, and metrics collection.
 */
@Singleton
public class TrackerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(TrackerClient.class);

    private final ServiceRegistry serviceRegistry;
    private final MeterRegistry meterRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final RetryRegistry retryRegistry;

    private final Counter requestCounter;
    private final Counter successCounter;
    private final Counter failureCounter;
    private final Timer requestTimer;

    /**
     * Constructs a new TrackerClient with the specified dependencies.
     *
     * @param serviceRegistry The service registry for service discovery
     * @param meterRegistry The meter registry for metrics collection
     */
    @Inject
    public TrackerClient(ServiceRegistry serviceRegistry, MeterRegistry meterRegistry) {
        this.serviceRegistry = serviceRegistry;
        this.meterRegistry = meterRegistry;

        // Initialize metrics
        this.requestCounter = Counter.builder("tracker_client_requests_total")
                .description("Total number of requests made by the tracker client")
                .register(meterRegistry);
        this.successCounter = Counter.builder("tracker_client_success_total")
                .description("Total number of successful requests made by the tracker client")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("tracker_client_failures_total")
                .description("Total number of failed requests made by the tracker client")
                .register(meterRegistry);
        this.requestTimer = Timer.builder("tracker_client_request_duration")
                .description("Duration of requests made by the tracker client")
                .register(meterRegistry);

        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(IOException.class, ConnectException.class, TimeoutException.class)
                .build();
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);

        // Configure retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(IOException.class, ConnectException.class)
                .build();
        this.retryRegistry = RetryRegistry.of(retryConfig);
    }

    /**
     * Connects to a service with the specified service ID.
     *
     * @param serviceId The ID of the service to connect to
     * @return A Socket connected to the service
     * @throws IOException If an I/O error occurs when creating the socket
     */
    public Socket connect(String serviceId) throws IOException {
        requestCounter.increment();
        Timer.Sample sample = Timer.start(meterRegistry);

        try {
            // Get circuit breaker for this service
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(serviceId);
            Retry retry = retryRegistry.retry(serviceId);

            // Use circuit breaker and retry with service discovery
            Socket socket = Retry.decorateSupplier(retry, 
                    CircuitBreaker.decorateSupplier(circuitBreaker, 
                            () -> connectToService(serviceId))).get();

            successCounter.increment();
            sample.stop(requestTimer);
            return socket;
        } catch (Exception e) {
            failureCounter.increment();
            sample.stop(requestTimer);
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Failed to connect to service: " + serviceId, e);
            }
        }
    }

    /**
     * Asynchronously connects to a service with the specified service ID.
     *
     * @param serviceId The ID of the service to connect to
     * @return A CompletableFuture that will complete with a Socket connected to the service
     */
    public CompletableFuture<Socket> connectAsync(String serviceId) {
        requestCounter.increment();
        Timer.Sample sample = Timer.start(meterRegistry);

        // Get circuit breaker for this service
        CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(serviceId);
        Retry retry = retryRegistry.retry(serviceId);

        // Create supplier for the connection
        Supplier<Socket> connectionSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, 
                () -> {
                    try {
                        return connectToService(serviceId);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });

        // Apply retry and execute asynchronously
        return CompletableFuture.supplyAsync(Retry.decorateSupplier(retry, connectionSupplier))
                .whenComplete((socket, throwable) -> {
                    if (throwable == null) {
                        successCounter.increment();
                    } else {
                        failureCounter.increment();
                    }
                    sample.stop(requestTimer);
                });
    }

    /**
     * Connects to a service using service discovery.
     *
     * @param serviceId The ID of the service to connect to
     * @return A Socket connected to the service
     * @throws IOException If an I/O error occurs when creating the socket
     */
    private Socket connectToService(String serviceId) throws IOException {
        // Discover service instance using service registry
        ServiceInstance instance = serviceRegistry.getServiceInstances(serviceId)
                .stream()
                .findFirst()
                .orElseThrow(() -> new IOException("Service not found: " + serviceId));

        LOGGER.debug("Connecting to service {} at {}:{}", serviceId, instance.getHost(), instance.getPort());

        // Create and connect socket
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(instance.getHost(), instance.getPort()), 10000);
        return socket;
    }

    /**
     * Gets the current state of the circuit breaker for a service.
     *
     * @param serviceId The ID of the service
     * @return The state of the circuit breaker
     */
    public CircuitBreaker.State getCircuitBreakerState(String serviceId) {
        return circuitBreakerRegistry.circuitBreaker(serviceId).getState();
    }

    /**
     * Resets the circuit breaker for a service.
     *
     * @param serviceId The ID of the service
     */
    public void resetCircuitBreaker(String serviceId) {
        circuitBreakerRegistry.circuitBreaker(serviceId).reset();
    }
}