/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.sms;

import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.health.ServiceHealth;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.notification.MessageException;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Singleton
public class ServiceDiscoverySmsClient implements SmsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoverySmsClient.class);
    private static final String SMS_SERVICE_NAME = "sms-service";
    private static final String CIRCUIT_BREAKER_NAME = "smsService";
    private static final String RETRY_NAME = "smsService";
    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";

    private final Client client;
    private final Config config;
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    private final Consul consul;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Counter smsRequestCounter;
    private final Counter smsSuccessCounter;
    private final Counter smsFailureCounter;
    private final Timer smsRequestTimer;

    private static final TextMapSetter<Map<String, String>> SETTER = new TextMapSetter<>() {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            carrier.put(key, value);
        }
    };

    @Inject
    public ServiceDiscoverySmsClient(
            Client client,
            Config config,
            CircuitBreakerRegistry circuitBreakerRegistry,
            RetryRegistry retryRegistry,
            Consul consul,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.client = client;
        this.config = config;
        this.consul = consul;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, circuitBreakerConfig);

        // Configure retry
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofSeconds(1))
                .retryExceptions(MessageException.class)
                .build();
        this.retry = retryRegistry.retry(RETRY_NAME, retryConfig);

        // Initialize metrics
        this.smsRequestCounter = meterRegistry.counter("sms.requests.total", "type", "service_discovery");
        this.smsSuccessCounter = meterRegistry.counter("sms.requests.success", "type", "service_discovery");
        this.smsFailureCounter = meterRegistry.counter("sms.requests.failure", "type", "service_discovery");
        this.smsRequestTimer = meterRegistry.timer("sms.requests.duration", "type", "service_discovery");

        LOGGER.info("ServiceDiscoverySmsClient initialized with circuit breaker and retry mechanisms");
    }

    @Override
    public void sendMessage(String phone, String message, boolean command) throws MessageException {
        smsRequestCounter.increment();
        Timer.Sample timerSample = Timer.start(meterRegistry);

        Span span = tracer.spanBuilder("sendSmsMessage")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("phone.number", phone);
            span.setAttribute("message.command", command);
            span.setAttribute("message.length", message.length());

            String correlationId = Span.current().getSpanContext().getTraceId();
            span.setAttribute("correlation.id", correlationId);

            Supplier<Void> sendMessageSupplier = Retry.decorateSupplier(retry, () -> {
                try {
                    doSendMessage(phone, message, command, correlationId, span);
                    return null;
                } catch (Exception e) {
                    span.recordException(e);
                    if (e instanceof MessageException) {
                        throw (MessageException) e;
                    } else {
                        throw new MessageException(e);
                    }
                }
            });

            try {
                CircuitBreaker.decorateSupplier(circuitBreaker, sendMessageSupplier).get();
                span.setStatus(StatusCode.OK);
                smsSuccessCounter.increment();
            } catch (Exception e) {
                span.setStatus(StatusCode.ERROR, e.getMessage());
                smsFailureCounter.increment();
                throw e;
            } finally {
                timerSample.stop(smsRequestTimer);
            }
        } finally {
            span.end();
        }
    }

    private void doSendMessage(String phone, String message, boolean command, String correlationId, Span parentSpan) 
            throws MessageException {
        Span span = tracer.spanBuilder("discoverAndSendSms")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            String serviceUrl = discoverSmsService();
            span.setAttribute("service.url", serviceUrl);

            Map<String, String> headers = new HashMap<>();
            headers.put(CORRELATION_ID_HEADER, correlationId);

            // Prepare the request payload
            Map<String, Object> payload = new HashMap<>();
            payload.put("phone", phone);
            payload.put("message", message);
            payload.put("command", command);

            // Send the request to the discovered SMS service
            try (Response response = getRequestBuilder(serviceUrl, headers)
                    .post(Entity.entity(payload, MediaType.APPLICATION_JSON))) {
                
                span.setAttribute("http.status_code", response.getStatus());
                
                if (response.getStatus() / 100 != 2) {
                    String errorMessage = response.readEntity(String.class);
                    span.setStatus(StatusCode.ERROR, errorMessage);
                    throw new MessageException(errorMessage);
                }
                
                span.setStatus(StatusCode.OK);
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                if (e instanceof MessageException) {
                    throw (MessageException) e;
                } else {
                    throw new MessageException(e);
                }
            }
        } finally {
            span.end();
        }
    }

    private String discoverSmsService() throws MessageException {
        Span span = tracer.spanBuilder("discoverSmsService")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("service.name", SMS_SERVICE_NAME);

            // Check if we should use a static URL from config instead of service discovery
            if (config.hasKey(Keys.SMS_URL)) {
                String staticUrl = config.getString(Keys.SMS_URL);
                span.setAttribute("service.discovery", "static");
                span.setAttribute("service.url", staticUrl);
                return staticUrl;
            }

            // Use Consul for service discovery
            try {
                HealthClient healthClient = consul.healthClient();
                List<ServiceHealth> services = healthClient.getHealthyServiceInstances(SMS_SERVICE_NAME).getResponse();

                span.setAttribute("service.discovery", "consul");
                span.setAttribute("service.instances.count", services.size());

                if (services.isEmpty()) {
                    span.setStatus(StatusCode.ERROR, "No healthy SMS service instances found");
                    throw new MessageException("No healthy SMS service instances found");
                }

                // Select the first healthy service instance
                ServiceHealth serviceHealth = services.get(0);
                String host = serviceHealth.getService().getAddress();
                int port = serviceHealth.getService().getPort();
                String scheme = Optional.ofNullable(serviceHealth.getService().getMeta().get("scheme")).orElse("http");

                String serviceUrl = String.format("%s://%s:%d/api/sms", scheme, host, port);
                span.setAttribute("service.url", serviceUrl);
                return serviceUrl;
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                if (e instanceof MessageException) {
                    throw e;
                } else {
                    throw new MessageException("Failed to discover SMS service: " + e.getMessage());
                }
            }
        } finally {
            span.end();
        }
    }

    private Invocation.Builder getRequestBuilder(String url, Map<String, String> headers) {
        Invocation.Builder builder = client.target(url).request(MediaType.APPLICATION_JSON);
        
        // Add headers for tracing and correlation
        headers.forEach(builder::header);
        
        // Add authorization if configured
        if (config.hasKey(Keys.SMS_AUTHORIZATION)) {
            builder = builder.header("Authorization", config.getString(Keys.SMS_AUTHORIZATION));
        }
        
        return builder;
    }
}