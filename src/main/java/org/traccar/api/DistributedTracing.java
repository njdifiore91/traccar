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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ResourceAttributes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Provides distributed tracing capabilities for the API Gateway, enabling end-to-end request tracking
 * across microservices. This class creates, propagates, and collects trace context using OpenTelemetry,
 * attaches correlation IDs to all outgoing requests, and enhances logging with trace information.
 */
@Singleton
public class DistributedTracing {

    private static final Logger LOGGER = LoggerFactory.getLogger(DistributedTracing.class);

    private static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    private static final String TRACE_ID_HEADER = "X-Trace-ID";
    private static final String SPAN_ID_HEADER = "X-Span-ID";
    private static final String PARENT_SPAN_ID_HEADER = "X-Parent-Span-ID";
    
    private static final AttributeKey<String> USER_ID_KEY = AttributeKey.stringKey("user.id");
    private static final AttributeKey<String> REQUEST_PATH_KEY = AttributeKey.stringKey("http.request.path");
    private static final AttributeKey<String> REQUEST_METHOD_KEY = AttributeKey.stringKey("http.request.method");
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlation.id");
    private static final AttributeKey<String> SERVICE_NAME_KEY = AttributeKey.stringKey("service.name");

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final Map<String, Span> activeSpans;
    private final String serviceName;

    /**
     * HTTP header getter for OpenTelemetry context extraction
     */
    private static final TextMapGetter<HttpServletRequest> REQUEST_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(HttpServletRequest carrier) {
            return Collections.list(carrier.getHeaderNames());
        }

        @Override
        public String get(HttpServletRequest carrier, String key) {
            return carrier.getHeader(key);
        }
    };

    /**
     * HTTP header setter for OpenTelemetry context injection
     */
    private static final TextMapSetter<HttpServletResponse> RESPONSE_SETTER = new TextMapSetter<>() {
        @Override
        public void set(HttpServletResponse carrier, String key, String value) {
            carrier.setHeader(key, value);
        }
    };

    /**
     * Initializes the distributed tracing system with OpenTelemetry.
     *
     * @param config Application configuration for tracing settings
     */
    @Inject
    public DistributedTracing(Config config) {
        boolean tracingEnabled = config.getBoolean(Keys.WEB_TRACING_ENABLED, false);
        serviceName = config.getString(Keys.WEB_SERVICE_NAME, "api-gateway");
        String exporterEndpoint = config.getString(Keys.WEB_TRACING_EXPORTER, "http://localhost:4317");
        double samplingRatio = config.getDouble(Keys.WEB_TRACING_SAMPLING_RATIO, 0.1);

        activeSpans = new ConcurrentHashMap<>();

        if (tracingEnabled) {
            LOGGER.info("Initializing distributed tracing with OpenTelemetry. Exporter: {}, Sampling ratio: {}",
                    exporterEndpoint, samplingRatio);

            Resource resource = Resource.getDefault()
                    .merge(Resource.create(Attributes.of(
                            ResourceAttributes.SERVICE_NAME, serviceName)));

            SpanExporter spanExporter = createSpanExporter(exporterEndpoint);

            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                    .setResource(resource)
                    .setSampler(Sampler.traceIdRatioBased(samplingRatio))
                    .build();

            openTelemetry = OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .build();

            tracer = openTelemetry.getTracer("org.traccar.api");
            propagator = openTelemetry.getPropagators().getTextMapPropagator();
        } else {
            LOGGER.info("Distributed tracing is disabled. Enable with 'web.tracing.enabled' configuration");
            openTelemetry = OpenTelemetry.noop();
            tracer = openTelemetry.getTracer("noop");
            propagator = TextMapPropagator.noop();
        }
    }

    /**
     * Creates a span exporter based on the configured endpoint.
     * This method supports different exporters based on the endpoint URL scheme.
     *
     * @param endpoint The exporter endpoint URL
     * @return A configured SpanExporter
     */
    private SpanExporter createSpanExporter(String endpoint) {
        try {
            // Default to OTLP gRPC exporter
            return io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter.builder()
                    .setEndpoint(endpoint)
                    .build();
        } catch (Exception e) {
            LOGGER.warn("Failed to create OTLP exporter: {}", e.getMessage());
            LOGGER.debug("Exporter creation error", e);
            
            // Fallback to logging exporter if OTLP fails
            return io.opentelemetry.sdk.trace.export.LoggingSpanExporter.create();
        }
    }

    /**
     * Starts a new trace for an incoming HTTP request.
     *
     * @param request The HTTP servlet request
     * @param userId The authenticated user ID or null if not authenticated
     * @return A correlation ID for the request
     */
    public String startTrace(HttpServletRequest request, String userId) {
        String correlationId = request.getHeader(CORRELATION_ID_HEADER);
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = generateCorrelationId();
        }

        Context extractedContext = propagator.extract(Context.current(), request, REQUEST_GETTER);
        SpanBuilder spanBuilder = tracer.spanBuilder(request.getRequestURI())
                .setSpanKind(SpanKind.SERVER)
                .setAttribute(REQUEST_PATH_KEY, request.getRequestURI())
                .setAttribute(REQUEST_METHOD_KEY, request.getMethod())
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(SERVICE_NAME_KEY, serviceName);

        if (userId != null && !userId.isEmpty()) {
            spanBuilder.setAttribute(USER_ID_KEY, userId);
        }

        Span span = spanBuilder.setParent(extractedContext).startSpan();
        try (Scope scope = span.makeCurrent()) {
            activeSpans.put(correlationId, span);
            return correlationId;
        }
    }

    /**
     * Ends the trace for a request and adds the trace context to the response.
     *
     * @param correlationId The correlation ID for the request
     * @param response The HTTP servlet response
     * @param requestContext The JAX-RS container request context
     * @param responseContext The JAX-RS container response context
     */
    public void endTrace(String correlationId, HttpServletResponse response,
                         ContainerRequestContext requestContext,
                         ContainerResponseContext responseContext) {
        Span span = activeSpans.remove(correlationId);
        if (span != null) {
            try (Scope scope = span.makeCurrent()) {
                // Add status code to span
                int statusCode = responseContext.getStatus();
                span.setAttribute("http.response.status_code", statusCode);

                // Set span status based on HTTP status code
                if (statusCode >= 400) {
                    span.setStatus(StatusCode.ERROR, "HTTP error " + statusCode);
                } else {
                    span.setStatus(StatusCode.OK);
                }

                // Add trace headers to response
                response.setHeader(CORRELATION_ID_HEADER, correlationId);
                response.setHeader(TRACE_ID_HEADER, span.getSpanContext().getTraceId());
                response.setHeader(SPAN_ID_HEADER, span.getSpanContext().getSpanId());
                if (span.getParentSpanContext().isValid()) {
                    response.setHeader(PARENT_SPAN_ID_HEADER, span.getParentSpanContext().getSpanId());
                }

                // Inject context into response for downstream services
                propagator.inject(Context.current(), response, RESPONSE_SETTER);
            } finally {
                span.end();
            }
        }
    }

    /**
     * Creates a child span for outgoing service calls.
     *
     * @param operationName The name of the operation being performed
     * @param correlationId The correlation ID for the parent request
     * @return A new span and scope that must be closed after the operation
     */
    public SpanScope createServiceSpan(String operationName, String correlationId) {
        Span parentSpan = activeSpans.get(correlationId);
        Context parentContext = parentSpan != null ? 
                Context.current().with(parentSpan) : Context.current();

        Span span = tracer.spanBuilder(operationName)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute(CORRELATION_ID_KEY, correlationId)
                .setAttribute(SERVICE_NAME_KEY, serviceName)
                .setParent(parentContext)
                .startSpan();

        Scope scope = span.makeCurrent();
        return new SpanScope(span, scope);
    }

    /**
     * Adds an attribute to an active span.
     *
     * @param correlationId The correlation ID for the request
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addAttribute(String correlationId, String key, String value) {
        Span span = activeSpans.get(correlationId);
        if (span != null) {
            span.setAttribute(key, value);
        }
    }
    
    /**
     * Adds an event to the current span.
     *
     * @param correlationId The correlation ID for the request
     * @param name The name of the event
     * @param attributes Additional attributes for the event
     */
    public void addEvent(String correlationId, String name, Attributes attributes) {
        Span span = activeSpans.get(correlationId);
        if (span != null) {
            span.addEvent(name, attributes);
        }
    }
    
    /**
     * Adds a simple event to the current span.
     *
     * @param correlationId The correlation ID for the request
     * @param name The name of the event
     */
    public void addEvent(String correlationId, String name) {
        Span span = activeSpans.get(correlationId);
        if (span != null) {
            span.addEvent(name);
        }
    }

    /**
     * Records an exception in the current span.
     *
     * @param correlationId The correlation ID for the request
     * @param exception The exception to record
     */
    public void recordException(String correlationId, Throwable exception) {
        Span span = activeSpans.get(correlationId);
        if (span != null) {
            span.recordException(exception);
            span.setStatus(StatusCode.ERROR, exception.getMessage());
        }
    }

    /**
     * Generates a unique correlation ID for a request.
     *
     * @return A new correlation ID
     */
    public String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Gets the current trace context for propagation to other services.
     *
     * @return The current trace context
     */
    public Context getCurrentContext() {
        return Context.current();
    }
    
    /**
     * Gets the current active span.
     *
     * @return The current active span or null if no span is active
     */
    public Span getCurrentSpan() {
        return Span.current();
    }
    
    /**
     * Gets the active span for a specific correlation ID.
     *
     * @param correlationId The correlation ID for the request
     * @return The active span or null if not found
     */
    public Span getSpanByCorrelationId(String correlationId) {
        return activeSpans.get(correlationId);
    }

    /**
     * Injects the current trace context into a map for propagation.
     *
     * @param carrier The map to inject the context into
     */
    public void injectContext(Map<String, String> carrier) {
        propagator.inject(Context.current(), carrier, (c, k, v) -> c.put(k, v));
    }
    
    /**
     * Extracts trace context from a map of headers.
     *
     * @param headers The map containing trace context headers
     * @return The extracted context
     */
    public Context extractContext(Map<String, String> headers) {
        return propagator.extract(Context.current(), headers, (carrier, key) -> carrier.get(key));
    }

    /**
     * A wrapper class that holds both a span and its scope for easy cleanup.
     */
    public static class SpanScope implements AutoCloseable {
        private final Span span;
        private final Scope scope;

        public SpanScope(Span span, Scope scope) {
            this.span = span;
            this.scope = scope;
        }

        public Span getSpan() {
            return span;
        }

        @Override
        public void close() {
            scope.close();
            span.end();
        }
    }
}