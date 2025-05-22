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

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.baggage.propagation.W3CBaggagePropagator;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.traccar.config.Config;
// Using our own Keys class for OpenTelemetry configuration
import org.traccar.config.ConfigKey;
import org.traccar.helper.Log;
import org.traccar.helper.LogAction;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages OpenTelemetry instrumentation for distributed tracing across service boundaries.
 * This component provides end-to-end visibility of requests flowing through the monolithic
 * Traccar server and extracted microservices, enabling effective monitoring and troubleshooting
 * during the transition to microservices architecture.
 */
@Singleton
public class OpenTelemetryManager {

    private static final Logger LOGGER = Logger.getLogger(OpenTelemetryManager.class.getName());
    private static final String CORRELATION_ID_KEY = "correlation.id";
    private static final String SERVICE_NAME = "traccar";
    
    /**
     * Configuration keys for OpenTelemetry.
     */
    public static final class Keys {
        /**
         * Enable OpenTelemetry instrumentation.
         */
        public static final ConfigKey TELEMETRY_ENABLE = new ConfigKey(
                "telemetry.enable", Boolean.class, false);

        /**
         * OpenTelemetry exporter type (otlp, jaeger).
         */
        public static final ConfigKey TELEMETRY_EXPORTER_TYPE = new ConfigKey(
                "telemetry.exporter.type", String.class, "otlp");

        /**
         * OpenTelemetry endpoint URL.
         */
        public static final ConfigKey TELEMETRY_ENDPOINT = new ConfigKey(
                "telemetry.endpoint", String.class, "http://localhost:4317");

        /**
         * OpenTelemetry export interval in milliseconds.
         */
        public static final ConfigKey TELEMETRY_EXPORT_INTERVAL = new ConfigKey(
                "telemetry.export.interval", Long.class, 60000L);

        /**
         * OpenTelemetry sampler type (always_on, always_off, traceidratio, parentbased_traceidratio).
         */
        public static final ConfigKey TELEMETRY_SAMPLER_TYPE = new ConfigKey(
                "telemetry.sampler.type", String.class, "parentbased_always_on");

        /**
         * OpenTelemetry sampling ratio (0.0 - 1.0) for traceidratio sampler.
         */
        public static final ConfigKey TELEMETRY_SAMPLING_RATIO = new ConfigKey(
                "telemetry.sampling.ratio", Double.class, 1.0);
    }

    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;
    private final Map<String, String> correlationIds = new ConcurrentHashMap<>();

    /**
     * Initializes the OpenTelemetry manager with configuration from the Traccar config.
     *
     * @param config The Traccar configuration
     */
    @Inject
    public OpenTelemetryManager(Config config) {
        LOGGER.info("Initializing OpenTelemetry Manager");
        
        // Initialize OpenTelemetry SDK with appropriate exporters based on configuration
        openTelemetry = initializeOpenTelemetry(config);
        tracer = openTelemetry.getTracer("org.traccar");
        
        // Add a shutdown hook to properly close OpenTelemetry resources
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOGGER.info("Shutting down OpenTelemetry Manager");
            if (openTelemetry instanceof OpenTelemetrySdk) {
                ((OpenTelemetrySdk) openTelemetry).close();
            }
        }));
        
        LOGGER.info("OpenTelemetry Manager initialized successfully");
    }

    /**
     * Initializes the OpenTelemetry SDK with appropriate exporters based on configuration.
     *
     * @param config The Traccar configuration
     * @return Configured OpenTelemetry instance
     */
    private OpenTelemetry initializeOpenTelemetry(Config config) {
        boolean enabled = config.getBoolean(Keys.TELEMETRY_ENABLE.getKey());
        if (!enabled) {
            LOGGER.info("OpenTelemetry is disabled");
            return OpenTelemetry.noop();
        }

        try {
            // Create a resource with service information
            Resource resource = Resource.getDefault()
                    .merge(Resource.create(Attributes.of(
                            ResourceAttributes.SERVICE_NAME, SERVICE_NAME,
                            ResourceAttributes.SERVICE_VERSION, Version.VERSION)));

            // Configure trace exporter based on configuration
            SpanExporter spanExporter = createSpanExporter(config);
            
            // Configure tracer provider with appropriate sampler
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                    .setResource(resource)
                    .setSampler(configureSampler(config))
                    .addSpanProcessor(BatchSpanProcessor.builder(spanExporter)
                            .setScheduleDelay(config.getLong(Keys.TELEMETRY_EXPORT_INTERVAL.getKey()), TimeUnit.MILLISECONDS)
                            .build())
                    .build();

            // Configure metrics exporter
            SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                    .setResource(resource)
                    .registerMetricReader(PeriodicMetricReader.builder(
                            OtlpGrpcMetricExporter.builder()
                                    .setEndpoint(config.getString(Keys.TELEMETRY_ENDPOINT.getKey()))
                                    .build())
                            .setInterval(config.getLong(Keys.TELEMETRY_EXPORT_INTERVAL.getKey()), TimeUnit.MILLISECONDS)
                            .build())
                    .build();

            // Configure logger provider
            SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
                    .setResource(resource)
                    .addLogRecordProcessor(BatchLogRecordProcessor.builder(
                            OtlpGrpcLogRecordExporter.builder()
                                    .setEndpoint(config.getString(Keys.TELEMETRY_ENDPOINT.getKey()))
                                    .build())
                            .build())
                    .build();

            // Configure context propagators for distributed tracing
            ContextPropagators propagators = ContextPropagators.create(
                    TextMapPropagator.composite(
                            W3CTraceContextPropagator.getInstance(),
                            W3CBaggagePropagator.getInstance()));

            // Build and return the OpenTelemetry SDK
            return OpenTelemetrySdk.builder()
                    .setTracerProvider(tracerProvider)
                    .setMeterProvider(meterProvider)
                    .setLoggerProvider(loggerProvider)
                    .setPropagators(propagators)
                    .build();

        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Failed to initialize OpenTelemetry: " + e.getMessage(), e);
            return OpenTelemetry.noop();
        }
    }

    /**
     * Creates a span exporter based on configuration.
     *
     * @param config The Traccar configuration
     * @return Configured SpanExporter
     */
    private SpanExporter createSpanExporter(Config config) {
        String exporterType = config.getString(Keys.TELEMETRY_EXPORTER_TYPE.getKey(), "otlp");
        String endpoint = config.getString(Keys.TELEMETRY_ENDPOINT.getKey(), "http://localhost:4317");

        return switch (exporterType.toLowerCase()) {
            case "jaeger" -> JaegerGrpcSpanExporter.builder()
                    .setEndpoint(endpoint)
                    .build();
            default -> OtlpGrpcSpanExporter.builder()
                    .setEndpoint(endpoint)
                    .build();
        };
    }

    /**
     * Configures the sampler based on configuration.
     *
     * @param config The Traccar configuration
     * @return Configured Sampler
     */
    private Sampler configureSampler(Config config) {
        String samplerType = config.getString(Keys.TELEMETRY_SAMPLER_TYPE.getKey(), "parentbased_always_on");
        double samplingRatio = config.getDouble(Keys.TELEMETRY_SAMPLING_RATIO.getKey(), 1.0);

        return switch (samplerType.toLowerCase()) {
            case "always_off" -> Sampler.alwaysOff();
            case "traceidratio" -> Sampler.traceIdRatioBased(samplingRatio);
            case "parentbased_traceidratio" -> Sampler.parentBased(Sampler.traceIdRatioBased(samplingRatio));
            default -> Sampler.alwaysOn();
        };
    }

    /**
     * Creates a new span with the given name and kind.
     *
     * @param spanName The name of the span
     * @param kind The kind of span (CLIENT, SERVER, etc.)
     * @return The created span
     */
    public Span createSpan(String spanName, SpanKind kind) {
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName).setSpanKind(kind);
        Span span = spanBuilder.startSpan();
        span.setAttribute(CORRELATION_ID_KEY, getOrCreateCorrelationId());
        return span;
    }

    /**
     * Creates a new span as a child of the current context.
     *
     * @param spanName The name of the span
     * @param kind The kind of span (CLIENT, SERVER, etc.)
     * @return The created span and its scope (must be closed)
     */
    public ScopedSpan createScopedSpan(String spanName, SpanKind kind) {
        SpanBuilder spanBuilder = tracer.spanBuilder(spanName).setSpanKind(kind);
        Span span = spanBuilder.startSpan();
        span.setAttribute(CORRELATION_ID_KEY, getOrCreateCorrelationId());
        Scope scope = span.makeCurrent();
        return new ScopedSpan(span, scope);
    }

    /**
     * Adds an attribute to the current span.
     *
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addSpanAttribute(String key, String value) {
        Span.current().setAttribute(key, value);
    }

    /**
     * Adds an attribute to the current span.
     *
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addSpanAttribute(String key, long value) {
        Span.current().setAttribute(key, value);
    }

    /**
     * Adds an attribute to the current span.
     *
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addSpanAttribute(String key, double value) {
        Span.current().setAttribute(key, value);
    }

    /**
     * Adds an attribute to the current span.
     *
     * @param key The attribute key
     * @param value The attribute value
     */
    public void addSpanAttribute(String key, boolean value) {
        Span.current().setAttribute(key, value);
    }

    /**
     * Adds an event to the current span.
     *
     * @param name The event name
     */
    public void addSpanEvent(String name) {
        Span.current().addEvent(name);
    }

    /**
     * Adds an event with attributes to the current span.
     *
     * @param name The event name
     * @param attributes The event attributes
     */
    public void addSpanEvent(String name, Attributes attributes) {
        Span.current().addEvent(name, attributes);
    }

    /**
     * Sets the status of the current span.
     *
     * @param statusCode The status code
     * @param description The status description
     */
    public void setSpanStatus(StatusCode statusCode, String description) {
        Span.current().setStatus(statusCode, description);
    }

    /**
     * Records an exception in the current span.
     *
     * @param exception The exception to record
     */
    public void recordException(Throwable exception) {
        Span.current().recordException(exception);
        Span.current().setStatus(StatusCode.ERROR, exception.getMessage());
    }

    /**
     * Gets the current correlation ID or creates a new one if none exists.
     *
     * @return The correlation ID
     */
    public String getOrCreateCorrelationId() {
        String threadId = String.valueOf(Thread.currentThread().getId());
        return correlationIds.computeIfAbsent(threadId, k -> generateCorrelationId());
    }

    /**
     * Sets the correlation ID for the current thread.
     *
     * @param correlationId The correlation ID to set
     */
    public void setCorrelationId(String correlationId) {
        String threadId = String.valueOf(Thread.currentThread().getId());
        correlationIds.put(threadId, correlationId);
    }

    /**
     * Clears the correlation ID for the current thread.
     */
    public void clearCorrelationId() {
        String threadId = String.valueOf(Thread.currentThread().getId());
        correlationIds.remove(threadId);
    }

    /**
     * Generates a new correlation ID.
     *
     * @return A new correlation ID
     */
    private String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Gets the OpenTelemetry instance.
     *
     * @return The OpenTelemetry instance
     */
    public OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }

    /**
     * Gets the tracer instance.
     *
     * @return The tracer instance
     */
    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Injects the current context into the carrier using the given setter.
     *
     * @param carrier The carrier to inject into
     * @param setter The setter to use for injection
     * @param <C> The carrier type
     */
    public <C> void injectContext(C carrier, TextMapSetter<C> setter) {
        Context context = Context.current();
        openTelemetry.getPropagators().getTextMapPropagator().inject(context, carrier, setter);
    }

    /**
     * Extracts the context from the carrier using the given getter.
     *
     * @param carrier The carrier to extract from
     * @param getter The getter to use for extraction
     * @param <C> The carrier type
     * @return The extracted context
     */
    public <C> Context extractContext(C carrier, TextMapGetter<C> getter) {
        return openTelemetry.getPropagators().getTextMapPropagator().extract(Context.current(), carrier, getter);
    }

    /**
     * Creates a span for a message processing operation.
     *
     * @param protocol The protocol name
     * @param deviceId The device ID (if available)
     * @return The created span and its scope (must be closed)
     */
    public ScopedSpan createMessageProcessingSpan(String protocol, String deviceId) {
        SpanBuilder spanBuilder = tracer.spanBuilder("process_message")
                .setSpanKind(SpanKind.CONSUMER);
        
        Span span = spanBuilder.startSpan();
        span.setAttribute("protocol", protocol);
        if (deviceId != null) {
            span.setAttribute("device.id", deviceId);
        }
        span.setAttribute(CORRELATION_ID_KEY, getOrCreateCorrelationId());
        
        Scope scope = span.makeCurrent();
        return new ScopedSpan(span, scope);
    }
    
    /**
     * Creates a span for a database operation.
     *
     * @param operation The database operation (e.g., "query", "update")
     * @param table The database table
     * @return The created span and its scope (must be closed)
     */
    public ScopedSpan createDatabaseSpan(String operation, String table) {
        SpanBuilder spanBuilder = tracer.spanBuilder("db_" + operation)
                .setSpanKind(SpanKind.CLIENT);
        
        Span span = spanBuilder.startSpan();
        span.setAttribute("db.operation", operation);
        span.setAttribute("db.table", table);
        span.setAttribute(CORRELATION_ID_KEY, getOrCreateCorrelationId());
        
        Scope scope = span.makeCurrent();
        return new ScopedSpan(span, scope);
    }
    
    /**
     * Creates a span for an API operation.
     *
     * @param method The HTTP method
     * @param path The API path
     * @return The created span and its scope (must be closed)
     */
    public ScopedSpan createApiSpan(String method, String path) {
        SpanBuilder spanBuilder = tracer.spanBuilder(method + " " + path)
                .setSpanKind(SpanKind.SERVER);
        
        Span span = spanBuilder.startSpan();
        span.setAttribute("http.method", method);
        span.setAttribute("http.path", path);
        span.setAttribute(CORRELATION_ID_KEY, getOrCreateCorrelationId());
        
        Scope scope = span.makeCurrent();
        return new ScopedSpan(span, scope);
    }
    
    /**
     * Integrates with Traccar's logging system to add trace and correlation IDs to log entries.
     * 
     * @param message The log message
     * @param level The log level
     * @return The enhanced log message with trace and correlation IDs
     */
    public String enhanceLogMessage(String message, Level level) {
        Span currentSpan = Span.current();
        if (currentSpan == Span.getInvalid()) {
            return message;
        }
        
        String traceId = currentSpan.getSpanContext().getTraceId();
        String spanId = currentSpan.getSpanContext().getSpanId();
        String correlationId = getOrCreateCorrelationId();
        
        return String.format("[trace_id=%s span_id=%s correlation_id=%s] %s", 
                traceId, spanId, correlationId, message);
    }
    
    /**
     * A class that holds a span and its scope, allowing for easy cleanup.
     */
    public static class ScopedSpan implements AutoCloseable {
        private final Span span;
        private final Scope scope;

        /**
         * Creates a new ScopedSpan.
         *
         * @param span The span
         * @param scope The scope
         */
        public ScopedSpan(Span span, Scope scope) {
            this.span = span;
            this.scope = scope;
        }

        /**
         * Gets the span.
         *
         * @return The span
         */
        public Span getSpan() {
            return span;
        }

        /**
         * Ends the span and closes the scope.
         */
        @Override
        public void close() {
            scope.close();
            span.end();
        }
    }
}