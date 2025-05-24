package org.traccar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.MeterBinder;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.ObservableDoubleGauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricReader;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Metrics collection and exposure for the API Gateway service.
 * 
 * Collects and exposes metrics for API request rates, response times, error rates,
 * circuit breaker states, and WebSocket connections using OpenTelemetry and Micrometer.
 * Metrics are exposed through a /metrics endpoint for Prometheus scraping.
 */
@Singleton
public class ApiGatewayMetrics implements MeterBinder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewayMetrics.class);

    private final MeterRegistry meterRegistry;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Meter meter;
    
    // Cache for request counters by endpoint
    private final Map<String, Counter> requestCounters = new ConcurrentHashMap<>();
    private final Map<String, LongCounter> otelRequestCounters = new ConcurrentHashMap<>();
    
    // Cache for error counters by endpoint and status code
    private final Map<String, Counter> errorCounters = new ConcurrentHashMap<>();
    private final Map<String, LongCounter> otelErrorCounters = new ConcurrentHashMap<>();
    
    // Cache for response time timers by endpoint
    private final Map<String, Timer> responseTimers = new ConcurrentHashMap<>();
    private final Map<String, DoubleHistogram> otelResponseHistograms = new ConcurrentHashMap<>();
    
    // WebSocket connection metrics
    private Counter webSocketConnectionsTotal;
    private Counter webSocketDisconnectsTotal;
    private Gauge webSocketActiveConnections;
    private int activeWebSocketConnections = 0;

    /**
     * Creates a new ApiGatewayMetrics instance.
     *
     * @param meterRegistry The Micrometer registry for metrics collection
     * @param circuitBreakerRegistry The Resilience4j circuit breaker registry
     * @param openTelemetry The OpenTelemetry instance
     */
    @Inject
    public ApiGatewayMetrics(MeterRegistry meterRegistry, 
                            CircuitBreakerRegistry circuitBreakerRegistry,
                            OpenTelemetry openTelemetry) {
        this.meterRegistry = meterRegistry;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.meter = openTelemetry.getMeter("org.traccar.api-gateway");
        
        // Register this instance as a MeterBinder
        this.bindTo(meterRegistry);
        
        // Initialize WebSocket metrics
        initializeWebSocketMetrics();
        
        // Initialize Prometheus metrics endpoint
        initializePrometheusExporter();
        
        LOGGER.info("API Gateway Metrics initialized successfully");
    }

    /**
     * Initializes the Prometheus metrics exporter.
     */
    private void initializePrometheusExporter() {
        try {
            // Create and start Prometheus HTTP server on port 9464 (default OpenTelemetry Prometheus port)
            MetricReader prometheusReader = PrometheusHttpServer.builder()
                    .setPort(9464)
                    .build();
            
            // Register the reader with the OpenTelemetry SDK if needed
            // This is typically done at the SDK initialization level, but included here for completeness
            SdkMeterProvider.builder()
                    .registerMetricReader(prometheusReader)
                    .build();
            
            LOGGER.info("Prometheus metrics endpoint initialized at http://localhost:9464/metrics");
        } catch (Exception e) {
            // Log the error but don't fail startup
            LOGGER.error("Failed to initialize Prometheus exporter: {}", e.getMessage(), e);
        }
    }

    /**
     * Initializes WebSocket connection metrics.
     */
    private void initializeWebSocketMetrics() {
        webSocketConnectionsTotal = Counter.builder("websocket_connections_total")
                .description("Total number of WebSocket connections established")
                .tag("service", "api-gateway")
                .register(meterRegistry);
        
        webSocketDisconnectsTotal = Counter.builder("websocket_disconnects_total")
                .description("Total number of WebSocket disconnections")
                .tag("service", "api-gateway")
                .register(meterRegistry);
        
        webSocketActiveConnections = Gauge.builder("websocket_active_connections", 
                () -> activeWebSocketConnections)
                .description("Current number of active WebSocket connections")
                .tag("service", "api-gateway")
                .register(meterRegistry);
        
        // Create OpenTelemetry metrics for WebSocket connections
        meter.gaugeBuilder("websocket_active_connections")
                .setDescription("Current number of active WebSocket connections")
                .setUnit("connections")
                .buildWithCallback(
                        r -> r.record(activeWebSocketConnections, 
                                Attributes.builder().put("service", "api-gateway").build()));
    }

    /**
     * Records a new WebSocket connection.
     * 
     * @param sessionId The WebSocket session ID
     */
    public void recordWebSocketConnection(String sessionId) {
        webSocketConnectionsTotal.increment();
        activeWebSocketConnections++;
        LOGGER.debug("WebSocket connection established: {} (active: {})", sessionId, activeWebSocketConnections);
    }

    /**
     * Records a WebSocket disconnection.
     * 
     * @param sessionId The WebSocket session ID
     */
    public void recordWebSocketDisconnection(String sessionId) {
        webSocketDisconnectsTotal.increment();
        activeWebSocketConnections = Math.max(0, activeWebSocketConnections - 1);
        LOGGER.debug("WebSocket connection closed: {} (active: {})", sessionId, activeWebSocketConnections);
    }

    /**
     * Records an API request.
     *
     * @param endpoint The API endpoint being called
     * @param method The HTTP method used
     */
    public void recordRequest(String endpoint, String method) {
        String key = method + "_" + endpoint;
        requestCounters.computeIfAbsent(key, k -> Counter.builder("api_requests_total")
                .description("Total number of API requests")
                .tag("endpoint", endpoint)
                .tag("method", method)
                .tag("service", "api-gateway")
                .register(meterRegistry))
                .increment();
        
        // Also record using OpenTelemetry
        otelRequestCounters.computeIfAbsent(key, k -> meter.counterBuilder("api_requests_total")
                .setDescription("Total number of API requests")
                .build())
                .add(1, Attributes.builder()
                        .put("endpoint", endpoint)
                        .put("method", method)
                        .put("service", "api-gateway")
                        .build());
    }

    /**
     * Records an API error.
     *
     * @param endpoint The API endpoint that returned an error
     * @param statusCode The HTTP status code
     * @param method The HTTP method used
     */
    public void recordError(String endpoint, int statusCode, String method) {
        String key = method + "_" + endpoint + "_" + statusCode;
        errorCounters.computeIfAbsent(key, k -> Counter.builder("api_errors_total")
                .description("Total number of API errors")
                .tag("endpoint", endpoint)
                .tag("status", String.valueOf(statusCode))
                .tag("method", method)
                .tag("service", "api-gateway")
                .register(meterRegistry))
                .increment();
        
        // Also record using OpenTelemetry
        otelErrorCounters.computeIfAbsent(key, k -> meter.counterBuilder("api_errors_total")
                .setDescription("Total number of API errors")
                .build())
                .add(1, Attributes.builder()
                        .put("endpoint", endpoint)
                        .put("status", String.valueOf(statusCode))
                        .put("method", method)
                        .put("service", "api-gateway")
                        .build());
    }

    /**
     * Records the response time for an API request.
     *
     * @param endpoint The API endpoint
     * @param method The HTTP method used
     * @param timeMs The response time in milliseconds
     */
    public void recordResponseTime(String endpoint, String method, long timeMs) {
        String key = method + "_" + endpoint;
        responseTimers.computeIfAbsent(key, k -> Timer.builder("api_request_duration_seconds")
                .description("API request duration in seconds")
                .tag("endpoint", endpoint)
                .tag("method", method)
                .tag("service", "api-gateway")
                .publishPercentiles(0.5, 0.95, 0.99) // Add percentiles for p50, p95, p99
                .register(meterRegistry))
                .record(timeMs, TimeUnit.MILLISECONDS);
        
        // Also record using OpenTelemetry
        otelResponseHistograms.computeIfAbsent(key, k -> meter.histogramBuilder("api_request_duration_seconds")
                .setDescription("API request duration in seconds")
                .setUnit("s")
                .build())
                .record(timeMs / 1000.0, Attributes.builder()
                        .put("endpoint", endpoint)
                        .put("method", method)
                        .put("service", "api-gateway")
                        .build());
    }

    /**
     * Records metrics for a complete API request cycle.
     *
     * @param endpoint The API endpoint
     * @param method The HTTP method used
     * @param statusCode The HTTP status code
     * @param timeMs The response time in milliseconds
     */
    public void recordApiCall(String endpoint, String method, int statusCode, long timeMs) {
        recordRequest(endpoint, method);
        recordResponseTime(endpoint, method, timeMs);
        
        // Record error if status code is 4xx or 5xx
        if (statusCode >= 400) {
            recordError(endpoint, statusCode, method);
        }
        
        // Log detailed metrics for debugging at trace level
        LOGGER.trace("API Call: {} {} - Status: {} - Time: {}ms", method, endpoint, statusCode, timeMs);
    }

    /**
     * Binds circuit breaker metrics to the meter registry.
     * This method is called by Spring Boot when the MeterBinder is registered.
     *
     * @param registry The meter registry
     */
    @Override
    public void bindTo(MeterRegistry registry) {
        // Register JVM metrics
        new JvmMemoryMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        
        // Register circuit breaker metrics
        circuitBreakerRegistry.getAllCircuitBreakers().forEach(circuitBreaker -> {
            String name = circuitBreaker.getName();
            
            // Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)
            Gauge.builder("circuit_breaker_state", circuitBreaker, cb -> getStateValue(cb))
                    .description("Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)")
                    .tag("name", name)
                    .tag("service", "api-gateway")
                    .register(registry);
            
            // Failure rate
            Gauge.builder("circuit_breaker_failure_rate", circuitBreaker, 
                    CircuitBreaker::getMetrics, 
                    metrics -> metrics.getFailureRate())
                    .description("Circuit breaker failure rate")
                    .tag("name", name)
                    .tag("service", "api-gateway")
                    .register(registry);
            
            // Slow call rate
            Gauge.builder("circuit_breaker_slow_call_rate", circuitBreaker, 
                    CircuitBreaker::getMetrics, 
                    metrics -> metrics.getSlowCallRate())
                    .description("Circuit breaker slow call rate")
                    .tag("name", name)
                    .tag("service", "api-gateway")
                    .register(registry);
            
            // Number of successful calls
            Gauge.builder("circuit_breaker_successful_calls", circuitBreaker, 
                    CircuitBreaker::getMetrics, 
                    metrics -> metrics.getNumberOfSuccessfulCalls())
                    .description("Number of successful calls")
                    .tag("name", name)
                    .tag("service", "api-gateway")
                    .register(registry);
            
            // Number of failed calls
            Gauge.builder("circuit_breaker_failed_calls", circuitBreaker, 
                    CircuitBreaker::getMetrics, 
                    metrics -> metrics.getNumberOfFailedCalls())
                    .description("Number of failed calls")
                    .tag("name", name)
                    .tag("service", "api-gateway")
                    .register(registry);
            
            // Number of slow calls
            Gauge.builder("circuit_breaker_slow_calls", circuitBreaker, 
                    CircuitBreaker::getMetrics, 
                    metrics -> metrics.getNumberOfSlowCalls())
                    .description("Number of slow calls")
                    .tag("name", name)
                    .tag("service", "api-gateway")
                    .register(registry);
            
            // Also register with OpenTelemetry
            registerCircuitBreakerOtelMetrics(circuitBreaker);
        });
    }

    /**
     * Registers circuit breaker metrics with OpenTelemetry.
     *
     * @param circuitBreaker The circuit breaker to monitor
     */
    private void registerCircuitBreakerOtelMetrics(CircuitBreaker circuitBreaker) {
        String name = circuitBreaker.getName();
        Attributes attributes = Attributes.builder()
                .put("name", name)
                .put("service", "api-gateway")
                .build();
        
        // Circuit breaker state
        meter.gaugeBuilder("circuit_breaker_state")
                .setDescription("Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN)")
                .buildWithCallback(r -> r.record(getStateValue(circuitBreaker), attributes));
        
        // Failure rate
        meter.gaugeBuilder("circuit_breaker_failure_rate")
                .setDescription("Circuit breaker failure rate")
                .setUnit("%")
                .buildWithCallback(r -> r.record(circuitBreaker.getMetrics().getFailureRate(), attributes));
        
        // Slow call rate
        meter.gaugeBuilder("circuit_breaker_slow_call_rate")
                .setDescription("Circuit breaker slow call rate")
                .setUnit("%")
                .buildWithCallback(r -> r.record(circuitBreaker.getMetrics().getSlowCallRate(), attributes));
    }

    /**
     * Converts a circuit breaker state to a numeric value.
     *
     * @param circuitBreaker The circuit breaker
     * @return 0 for CLOSED, 1 for OPEN, 2 for HALF_OPEN
     */
    private int getStateValue(CircuitBreaker circuitBreaker) {
        switch (circuitBreaker.getState()) {
            case CLOSED:
                return 0;
            case OPEN:
                return 1;
            case HALF_OPEN:
                return 2;
            case DISABLED:
                return 3;
            case FORCED_OPEN:
                return 4;
            default:
                return -1;
        }
    }
}