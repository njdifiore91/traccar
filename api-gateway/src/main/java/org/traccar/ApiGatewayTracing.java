package org.traccar;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Singleton;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Distributed tracing configuration for the API Gateway service.
 * Initializes OpenTelemetry tracing, creates and propagates trace context across service boundaries,
 * and enhances logging with trace information.
 */
@Singleton
public class ApiGatewayTracing {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewayTracing.class);
    
    private static final String SERVICE_NAME = "api-gateway";
    private static final String TRACE_ID_KEY = "traceId";
    private static final String SPAN_ID_KEY = "spanId";
    private static final String CORRELATION_ID_KEY = "correlationId";
    
    private final OpenTelemetry openTelemetry;
    private final Tracer tracer;

    /**
     * Initializes the OpenTelemetry SDK and creates a tracer for the API Gateway service.
     */
    public ApiGatewayTracing() {
        LOGGER.info("Initializing API Gateway tracing with OpenTelemetry");
        
        String otelEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (otelEndpoint == null || otelEndpoint.isEmpty()) {
            otelEndpoint = "http://otel-collector:4317"; // Default endpoint
        }
        
        // Configure the OpenTelemetry SDK with W3C Trace Context propagation
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        ResourceAttributes.SERVICE_NAME, SERVICE_NAME,
                        ResourceAttributes.SERVICE_VERSION, getServiceVersion())));
        
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .setSampler(configureSampler())
                .addSpanProcessor(BatchSpanProcessor.builder(
                        OtlpGrpcSpanExporter.builder()
                                .setEndpoint(otelEndpoint)
                                .setTimeout(5, TimeUnit.SECONDS)
                                .build())
                        .build())
                .build();
        
        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
        
        tracer = openTelemetry.getTracer(SERVICE_NAME);
        LOGGER.info("API Gateway tracing initialized successfully");
    }

    /**
     * Configures the sampling strategy for traces.
     * Uses environment variables to determine sampling rate.
     *
     * @return The configured sampler
     */
    private Sampler configureSampler() {
        String samplerType = System.getenv("OTEL_TRACES_SAMPLER");
        String samplerArg = System.getenv("OTEL_TRACES_SAMPLER_ARG");
        
        if ("parentbased_traceidratio".equals(samplerType) && samplerArg != null) {
            try {
                double ratio = Double.parseDouble(samplerArg);
                return Sampler.parentBased(Sampler.traceIdRatioBased(ratio));
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid sampling ratio: {}, using default", samplerArg);
            }
        }
        
        // Default: sample 10% of traces
        return Sampler.parentBased(Sampler.traceIdRatioBased(0.1));
    }

    /**
     * Gets the service version from environment or default value.
     *
     * @return The service version string
     */
    private String getServiceVersion() {
        String version = System.getenv("SERVICE_VERSION");
        return version != null ? version : "1.0.0";
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
     * Gets the Tracer instance.
     *
     * @return The Tracer instance
     */
    public Tracer getTracer() {
        return tracer;
    }

    /**
     * Creates a new span for an incoming API request.
     *
     * @param method The HTTP method (GET, POST, etc.)
     * @param path The request path
     * @param headers The request headers for context extraction
     * @return The created span
     */
    public Span createRequestSpan(String method, String path, Map<String, String> headers) {
        // Extract context from incoming headers if present
        Context extractedContext = extractContextFromHeaders(headers);
        Context context = extractedContext != null ? extractedContext : Context.current();
        
        // Generate a correlation ID if not present
        String correlationId = headers.get(CORRELATION_ID_KEY);
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = generateCorrelationId();
        }
        
        // Create a span for the API request
        Span span = tracer.spanBuilder(method + " " + path)
                .setParent(context)
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("http.method", method)
                .setAttribute("http.path", path)
                .setAttribute("http.correlation_id", correlationId)
                .startSpan();
        
        // Add trace and span IDs to MDC for logging correlation
        MDC.put(TRACE_ID_KEY, span.getSpanContext().getTraceId());
        MDC.put(SPAN_ID_KEY, span.getSpanContext().getSpanId());
        MDC.put(CORRELATION_ID_KEY, correlationId);
        
        LOGGER.debug("Created request span for {} {}, traceId={}, spanId={}, correlationId={}", 
                method, path, span.getSpanContext().getTraceId(), 
                span.getSpanContext().getSpanId(), correlationId);
        
        return span;
    }

    /**
     * Creates a child span for a backend service call.
     *
     * @param parentSpan The parent span (usually the request span)
     * @param serviceName The name of the service being called
     * @param operation The operation being performed
     * @return The created child span
     */
    public Span createServiceSpan(Span parentSpan, String serviceName, String operation) {
        Span span = tracer.spanBuilder(serviceName + "." + operation)
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("service.name", serviceName)
                .setAttribute("service.operation", operation)
                .startSpan();
        
        LOGGER.debug("Created service span for {}.{}, traceId={}, spanId={}", 
                serviceName, operation, span.getSpanContext().getTraceId(), 
                span.getSpanContext().getSpanId());
        
        return span;
    }

    /**
     * Injects the current trace context into outgoing headers for propagation.
     *
     * @param headers The headers map to inject context into
     */
    public void injectTraceContext(Map<String, String> headers) {
        TextMapSetter<Map<String, String>> setter = new TextMapSetter<>() {
            @Override
            public void set(Map<String, String> carrier, String key, String value) {
                carrier.put(key, value);
            }
        };
        
        openTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), headers, setter);
        
        // Also inject correlation ID from MDC if available
        String correlationId = MDC.get(CORRELATION_ID_KEY);
        if (correlationId != null && !correlationId.isEmpty()) {
            headers.put(CORRELATION_ID_KEY, correlationId);
        }
        
        LOGGER.debug("Injected trace context into outgoing headers");
    }

    /**
     * Extracts trace context from incoming headers.
     *
     * @param headers The headers containing trace context
     * @return The extracted context, or null if no valid context found
     */
    public Context extractContextFromHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        
        TextMapGetter<Map<String, String>> getter = new TextMapGetter<>() {
            @Override
            public String get(Map<String, String> carrier, String key) {
                return carrier.get(key);
            }

            @Override
            public Iterable<String> keys(Map<String, String> carrier) {
                return carrier.keySet();
            }
        };
        
        return openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), headers, getter);
    }

    /**
     * Ends a span with success status.
     *
     * @param span The span to end
     */
    public void endSpan(Span span) {
        if (span != null) {
            span.setStatus(StatusCode.OK);
            span.end();
            LOGGER.debug("Ended span successfully: {}", span.getSpanContext().getSpanId());
        }
    }

    /**
     * Ends a span with error status and records the exception.
     *
     * @param span The span to end
     * @param throwable The exception that occurred
     */
    public void endSpanWithError(Span span, Throwable throwable) {
        if (span != null) {
            span.setStatus(StatusCode.ERROR, throwable.getMessage());
            span.recordException(throwable);
            span.end();
            LOGGER.debug("Ended span with error: {}, exception: {}", 
                    span.getSpanContext().getSpanId(), throwable.getMessage());
        }
    }

    /**
     * Generates a unique correlation ID for request tracking.
     *
     * @return A unique correlation ID string
     */
    public String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Cleans up trace context from thread-local storage.
     * Should be called at the end of request processing.
     */
    public void clearTraceContext() {
        MDC.remove(TRACE_ID_KEY);
        MDC.remove(SPAN_ID_KEY);
        MDC.remove(CORRELATION_ID_KEY);
    }
}