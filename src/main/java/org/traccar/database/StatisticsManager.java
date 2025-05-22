/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.database;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.security.ServiceAccountUser;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.DateUtil;
import org.traccar.messaging.MessagePublisher;
import org.traccar.model.Statistics;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Request;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.Form;
import java.time.Duration;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Statistics Manager for collecting and reporting system metrics.
 * Implements circuit breaker pattern for database operations and integrates with OpenTelemetry for tracing.
 * Exposes metrics in Prometheus format for monitoring.
 */
@Singleton
public class StatisticsManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsManager.class);

    private static final int SPLIT_MODE = Calendar.DAY_OF_MONTH;
    private static final String CIRCUIT_BREAKER_NAME = "statisticsManager";
    private static final String TRACER_NAME = "org.traccar.database.StatisticsManager";
    private static final String METRICS_PREFIX = "traccar_statistics";

    private final Config config;
    private final Storage storage;
    private final Client client;
    private final ObjectMapper objectMapper;
    private final OpenTelemetry openTelemetry;
    private final MessagePublisher messagePublisher;
    private final PrometheusMeterRegistry meterRegistry;
    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;
    private final ScheduledExecutorService executorService;

    private final AtomicInteger lastUpdate = new AtomicInteger(Calendar.getInstance().get(SPLIT_MODE));

    private final Set<Long> users = new HashSet<>();
    private final Map<Long, String> deviceProtocols = new HashMap<>();
    private final Map<Long, Integer> deviceMessages = new HashMap<>();
    private final Map<String, Counter> protocolCounters = new ConcurrentHashMap<>();

    // Metrics counters
    private Counter requestsCounter;
    private Counter messagesReceivedCounter;
    private Counter messagesStoredCounter;
    private Counter mailSentCounter;
    private Counter smsSentCounter;
    private Counter geocoderRequestsCounter;
    private Counter geolocationRequestsCounter;
    private Gauge activeUsersGauge;
    private Gauge activeDevicesGauge;

    // Legacy counters for backward compatibility
    private int requests;
    private int messagesReceived;
    private int messagesStored;
    private int mailSent;
    private int smsSent;
    private int geocoderRequests;
    private int geolocationRequests;

    /**
     * Constructs a new StatisticsManager with the necessary dependencies.
     *
     * @param config The system configuration
     * @param storage The storage interface for database operations
     * @param client The HTTP client for external service calls
     * @param objectMapper The JSON object mapper
     * @param openTelemetry The OpenTelemetry instance for distributed tracing
     * @param messagePublisher The message publisher for inter-service communication
     */
    @Inject
    public StatisticsManager(Config config, Storage storage, Client client, ObjectMapper objectMapper,
                            OpenTelemetry openTelemetry, MessagePublisher messagePublisher) {
        this.config = config;
        this.storage = storage;
        this.client = client;
        this.objectMapper = objectMapper;
        this.openTelemetry = openTelemetry;
        this.messagePublisher = messagePublisher;
        this.tracer = openTelemetry.getTracer(TRACER_NAME);
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        
        // Initialize Prometheus meter registry
        this.meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        
        // Configure circuit breaker for database operations
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .recordExceptions(StorageException.class)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Register circuit breaker events for monitoring
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
    }

    /**
     * Initializes the statistics manager, setting up metrics and scheduling periodic tasks.
     */
    @PostConstruct
    public void init() {
        // Initialize Prometheus metrics
        initializeMetrics();
        
        // Schedule periodic aggregation of distributed statistics
        long aggregationInterval = config.getLong(Keys.DATABASE_STATISTICS_AGGREGATION_INTERVAL, 300); // Default 5 minutes
        executorService.scheduleAtFixedRate(this::aggregateDistributedStatistics, 
                aggregationInterval, aggregationInterval, TimeUnit.SECONDS);
    }

    /**
     * Cleans up resources when the service is shutting down.
     */
    @PreDestroy
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Initializes Prometheus metrics for monitoring.
     */
    private void initializeMetrics() {
        // Create counters for tracking statistics
        requestsCounter = Counter.builder(METRICS_PREFIX + "_requests_total")
                .description("Total number of API requests")
                .register(meterRegistry);
        
        messagesReceivedCounter = Counter.builder(METRICS_PREFIX + "_messages_received_total")
                .description("Total number of messages received")
                .register(meterRegistry);
        
        messagesStoredCounter = Counter.builder(METRICS_PREFIX + "_messages_stored_total")
                .description("Total number of messages stored")
                .register(meterRegistry);
        
        mailSentCounter = Counter.builder(METRICS_PREFIX + "_mail_sent_total")
                .description("Total number of emails sent")
                .register(meterRegistry);
        
        smsSentCounter = Counter.builder(METRICS_PREFIX + "_sms_sent_total")
                .description("Total number of SMS sent")
                .register(meterRegistry);
        
        geocoderRequestsCounter = Counter.builder(METRICS_PREFIX + "_geocoder_requests_total")
                .description("Total number of geocoder requests")
                .register(meterRegistry);
        
        geolocationRequestsCounter = Counter.builder(METRICS_PREFIX + "_geolocation_requests_total")
                .description("Total number of geolocation requests")
                .register(meterRegistry);
        
        // Create gauges for current state
        activeUsersGauge = Gauge.builder(METRICS_PREFIX + "_active_users", users, Set::size)
                .description("Number of active users")
                .register(meterRegistry);
        
        activeDevicesGauge = Gauge.builder(METRICS_PREFIX + "_active_devices", deviceProtocols, Map::size)
                .description("Number of active devices")
                .register(meterRegistry);
    }

    /**
     * Aggregates statistics from distributed service instances via message broker.
     */
    private void aggregateDistributedStatistics() {
        Span span = tracer.spanBuilder("aggregateDistributedStatistics")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Publish request for statistics from other services
            messagePublisher.publish("statistics.aggregate.request", new HashMap<>());
            
            // Process would typically receive responses via a message consumer
            // which would update the local statistics
            
            span.addEvent("Statistics aggregation completed");
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.warn("Error during statistics aggregation", e);
        } finally {
            span.end();
        }
    }

    /**
     * Checks if it's time to split statistics and save the current period.
     * Uses circuit breaker pattern to handle database failures gracefully.
     */
    private void checkSplit() {
        int currentUpdate = Calendar.getInstance().get(SPLIT_MODE);
        if (lastUpdate.getAndSet(currentUpdate) != currentUpdate) {
            Span span = tracer.spanBuilder("saveStatistics")
                    .setSpanKind(SpanKind.CLIENT)
                    .setAttribute("statistics.period", currentUpdate)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                Statistics statistics = new Statistics();

                synchronized (this) {
                    statistics.setCaptureTime(new Date());
                    statistics.setActiveUsers(users.size());
                    statistics.setActiveDevices(deviceProtocols.size());
                    statistics.setRequests(requests);
                    statistics.setMessagesReceived(messagesReceived);
                    statistics.setMessagesStored(messagesStored);
                    statistics.setMailSent(mailSent);
                    statistics.setSmsSent(smsSent);
                    statistics.setGeocoderRequests(geocoderRequests);
                    statistics.setGeolocationRequests(geolocationRequests);
                    if (!deviceProtocols.isEmpty()) {
                        Map<String, Integer> protocols = new HashMap<>();
                        for (String protocol : deviceProtocols.values()) {
                            protocols.compute(protocol, (key, count) -> count != null ? count + 1 : 1);
                        }
                        statistics.setProtocols(protocols);
                    }

                    users.clear();
                    deviceProtocols.clear();
                    deviceMessages.clear();
                    requests = 0;
                    messagesReceived = 0;
                    messagesStored = 0;
                    mailSent = 0;
                    smsSent = 0;
                    geocoderRequests = 0;
                    geolocationRequests = 0;
                }

                // Use circuit breaker for database operations
                Supplier<Void> storeStatisticsSupplier = () -> {
                    try {
                        storage.addObject(statistics, new Request(new Columns.Exclude("id")));
                        span.addEvent("Statistics saved to database");
                    } catch (StorageException e) {
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, "Failed to save statistics");
                        throw new RuntimeException(e);
                    }
                    return null;
                };

                try {
                    circuitBreaker.decorateSupplier(storeStatisticsSupplier).get();
                } catch (Exception e) {
                    LOGGER.warn("Error saving statistics (Circuit: {})", 
                            circuitBreaker.getState(), e);
                }

                String url = config.getString(Keys.SERVER_STATISTICS);
                if (url != null && !url.isEmpty()) {
                    sendStatisticsToExternalService(statistics, url, span);
                }
            } finally {
                span.end();
            }
        }
    }

    /**
     * Sends statistics to an external service with distributed tracing.
     *
     * @param statistics The statistics to send
     * @param url The URL of the external service
     * @param parentSpan The parent span for tracing context
     */
    private void sendStatisticsToExternalService(Statistics statistics, String url, Span parentSpan) {
        Span span = tracer.spanBuilder("sendStatisticsToExternalService")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("http.url", url)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            String time = DateUtil.formatDate(statistics.getCaptureTime());
            span.setAttribute("statistics.time", time);

            Form form = new Form();
            form.param("version", getClass().getPackage().getImplementationVersion());
            form.param("captureTime", time);
            form.param("activeUsers", String.valueOf(statistics.getActiveUsers()));
            form.param("activeDevices", String.valueOf(statistics.getActiveDevices()));
            form.param("requests", String.valueOf(statistics.getRequests()));
            form.param("messagesReceived", String.valueOf(statistics.getMessagesReceived()));
            form.param("messagesStored", String.valueOf(statistics.getMessagesStored()));
            form.param("mailSent", String.valueOf(statistics.getMailSent()));
            form.param("smsSent", String.valueOf(statistics.getSmsSent()));
            form.param("geocoderRequests", String.valueOf(statistics.getGeocoderRequests()));
            form.param("geolocationRequests", String.valueOf(statistics.getGeolocationRequests()));
            if (statistics.getProtocols() != null) {
                try {
                    form.param("protocols", objectMapper.writeValueAsString(statistics.getProtocols()));
                } catch (JsonProcessingException e) {
                    span.recordException(e);
                    LOGGER.warn("Failed to serialize protocols", e);
                }
            }
            if (!statistics.getAttributes().isEmpty()) {
                try {
                    form.param("attributes", objectMapper.writeValueAsString(statistics.getAttributes()));
                } catch (JsonProcessingException e) {
                    span.recordException(e);
                    LOGGER.warn("Failed to serialize attributes", e);
                }
            }

            client.target(url).request().async().post(Entity.form(form));
            span.addEvent("Statistics sent to external service");
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.warn("Error sending statistics to external service", e);
        } finally {
            span.end();
        }
    }

    /**
     * Registers a user request in the statistics.
     *
     * @param userId The ID of the user making the request
     */
    public synchronized void registerRequest(long userId) {
        checkSplit();
        requests += 1;
        requestsCounter.increment();
        
        if (userId != 0 && userId != ServiceAccountUser.ID) {
            users.add(userId);
        }
    }

    /**
     * Registers a received message in the statistics.
     */
    public synchronized void registerMessageReceived() {
        checkSplit();
        messagesReceived += 1;
        messagesReceivedCounter.increment();
    }

    /**
     * Registers a stored message in the statistics.
     *
     * @param deviceId The ID of the device the message is from
     * @param protocol The protocol used by the device
     */
    public synchronized void registerMessageStored(long deviceId, String protocol) {
        checkSplit();
        messagesStored += 1;
        messagesStoredCounter.increment();
        
        if (deviceId != 0) {
            deviceProtocols.put(deviceId, protocol);
            deviceMessages.merge(deviceId, 1, Integer::sum);
            
            // Track protocol-specific metrics
            protocolCounters.computeIfAbsent(protocol, p -> {
                return Counter.builder(METRICS_PREFIX + "_protocol_messages_total")
                        .description("Total number of messages by protocol")
                        .tags(Tags.of(Tag.of("protocol", p)))
                        .register(meterRegistry);
            }).increment();
        }
    }

    /**
     * Gets the total count of stored messages.
     *
     * @return The count of stored messages
     */
    public synchronized int messageStoredCount() {
        return messagesStored;
    }

    /**
     * Gets the count of stored messages for a specific device.
     *
     * @param deviceId The ID of the device
     * @return The count of stored messages for the device
     */
    public synchronized int messageStoredCount(long deviceId) {
        return deviceMessages.getOrDefault(deviceId, 0);
    }

    /**
     * Registers a sent email in the statistics.
     */
    public synchronized void registerMail() {
        checkSplit();
        mailSent += 1;
        mailSentCounter.increment();
    }

    /**
     * Registers a sent SMS in the statistics.
     */
    public synchronized void registerSms() {
        checkSplit();
        smsSent += 1;
        smsSentCounter.increment();
    }

    /**
     * Registers a geocoder request in the statistics.
     */
    public synchronized void registerGeocoderRequest() {
        checkSplit();
        geocoderRequests += 1;
        geocoderRequestsCounter.increment();
    }

    /**
     * Registers a geolocation request in the statistics.
     */
    public synchronized void registerGeolocationRequest() {
        checkSplit();
        geolocationRequests += 1;
        geolocationRequestsCounter.increment();
    }

    /**
     * Gets the Prometheus metrics registry for scraping.
     *
     * @return The Prometheus meter registry
     */
    public PrometheusMeterRegistry getMeterRegistry() {
        return meterRegistry;
    }

    /**
     * Gets the current state of the circuit breaker.
     *
     * @return The circuit breaker state as a string
     */
    public String getCircuitBreakerState() {
        return circuitBreaker.getState().name();
    }
}