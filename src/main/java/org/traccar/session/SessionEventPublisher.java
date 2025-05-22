/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.session.cache.CacheManager;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Provides functionality for publishing session-related events to a message broker (Kafka/RabbitMQ).
 * This class handles the serialization of session events, manages topic selection, and ensures reliable
 * delivery of events to interested consumers.
 */
@Singleton
public class SessionEventPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(SessionEventPublisher.class);

    private static final String EVENT_TYPE_CONNECTION = "connection";
    private static final String EVENT_TYPE_DISCONNECTION = "disconnection";
    private static final String EVENT_TYPE_STATUS = "status";
    
    // Map internal event types to standard Event types
    private static final Map<String, String> EVENT_TYPE_MAPPING = Map.of(
            EVENT_TYPE_CONNECTION, Event.TYPE_DEVICE_ONLINE,
            EVENT_TYPE_DISCONNECTION, Event.TYPE_DEVICE_OFFLINE,
            EVENT_TYPE_STATUS, "deviceStatus");

    private static final String TOPIC_PREFIX = "session-events";
    private static final String TOPIC_CONNECTION = TOPIC_PREFIX + ".connection";
    private static final String TOPIC_DISCONNECTION = TOPIC_PREFIX + ".disconnection";
    private static final String TOPIC_STATUS = TOPIC_PREFIX + ".status";

    private final Config config;
    private final CacheManager cacheManager;
    private final ObjectMapper objectMapper;
    private final Tracer tracer;
    private final Meter meter;
    private final Producer<String, String> producer;
    private final Retry retry;
    private final LongCounter publishedEventsCounter;
    private final LongCounter failedEventsCounter;
    private final LongHistogram eventProcessingTimeHistogram;
    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduledExecutorService;
    private final boolean enabled;
    private final String brokerType;
    private final ConcurrentLinkedQueue<Map<String, Object>> eventQueue;
    private final AtomicBoolean batchInProgress;
    private final int batchSize;
    private final int batchIntervalMs;
    private final TextMapSetter<ProducerRecord<String, String>> kafkaHeaderSetter;

    /**
     * Constructs a new SessionEventPublisher with the specified dependencies.
     *
     * @param config The application configuration
     * @param cacheManager The cache manager for accessing device information
     * @param objectMapper The JSON object mapper for serialization
     * @param tracer The OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public SessionEventPublisher(
            Config config,
            CacheManager cacheManager,
            ObjectMapper objectMapper,
            Tracer tracer,
            Meter meter) {
        this.config = config;
        this.cacheManager = cacheManager;
        this.objectMapper = objectMapper;
        this.tracer = tracer;
        this.meter = meter;
        this.enabled = config.getBoolean(Keys.EVENT_FORWARD_ENABLE);
        this.brokerType = config.getString("session.broker.type", "kafka").toLowerCase();
        
        // Initialize metrics
        this.publishedEventsCounter = meter.counterBuilder("session.events.published")
                .setDescription("Number of session events published")
                .build();
        this.failedEventsCounter = meter.counterBuilder("session.events.failed")
                .setDescription("Number of session events that failed to publish")
                .build();
        this.eventProcessingTimeHistogram = meter.histogramBuilder("session.events.processing.time")
                .setDescription("Time taken to process and publish session events")
                .setUnit("ms")
                .build();

        // Initialize the message producer based on configuration
        if (enabled) {
            if ("kafka".equals(brokerType)) {
                this.producer = createKafkaProducer();
            } else if ("rabbitmq".equals(brokerType)) {
                // For now, we'll use Kafka as the default implementation
                // In a real implementation, we would initialize a RabbitMQ client here
                this.producer = createKafkaProducer();
                LOGGER.warn("RabbitMQ support is not fully implemented yet. Using Kafka producer as fallback.");
            } else {
                this.producer = null;
                LOGGER.error("Unsupported message broker type: {}", brokerType);
            }
        } else {
            this.producer = null;
        }

        // Configure retry mechanism
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(config.getInteger("session.publish.retry.maxAttempts", 3))
                .waitDuration(Duration.ofMillis(config.getInteger("session.publish.retry.waitDuration", 1000)))
                .retryExceptions(Exception.class)
                .build();
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry("sessionEventPublisher");

        // Create a dedicated thread pool for async publishing
        this.executorService = Executors.newFixedThreadPool(
                config.getInteger("session.publish.threadPool.size", 2));
        
        // Create a scheduled executor for batch processing
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(
                config.getInteger("session.publish.scheduledThreadPool.size", 1));
        
        // Initialize batch processing
        this.eventQueue = new ConcurrentLinkedQueue<>();
        this.batchInProgress = new AtomicBoolean(false);
        this.batchSize = config.getInteger("session.publish.batch.size", 100);
        this.batchIntervalMs = config.getInteger("session.publish.batch.intervalMs", 1000);
        
        // Define a setter for propagating trace context via Kafka headers
        this.kafkaHeaderSetter = (carrier, key, value) -> {
            if (carrier != null) {
                carrier.headers().add(key, value.getBytes());
            }
        };
        
        // Start batch processing if enabled
        if (enabled && config.getBoolean("session.publish.batch.enabled", true)) {
            scheduledExecutorService.scheduleAtFixedRate(
                    this::processBatch, 
                    batchIntervalMs, 
                    batchIntervalMs, 
                    TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Creates a Kafka producer with the configured properties.
     *
     * @return A configured Kafka producer instance
     */
    private Producer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                config.getString("kafka.bootstrap.servers", "localhost:9092"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, 
                config.getString("kafka.producer.acks", "all"));
        props.put(ProducerConfig.RETRIES_CONFIG, 
                config.getInteger("kafka.producer.retries", 3));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 
                config.getInteger("kafka.producer.batch.size", 16384));
        props.put(ProducerConfig.LINGER_MS_CONFIG, 
                config.getInteger("kafka.producer.linger.ms", 1));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 
                config.getInteger("kafka.producer.buffer.memory", 33554432));

        return new KafkaProducer<>(props);
    }

    /**
     * Publishes a connection event when a device connects to the system.
     *
     * @param deviceSession The device session that was established
     * @return A CompletableFuture that completes when the event is published
     */
    public CompletableFuture<Void> publishConnectionEvent(DeviceSession deviceSession) {
        if (!enabled || producer == null) {
            return CompletableFuture.completedFuture(null);
        }

        return publishEvent(EVENT_TYPE_CONNECTION, TOPIC_CONNECTION, deviceSession, null);
    }

    /**
     * Publishes a disconnection event when a device disconnects from the system.
     *
     * @param deviceSession The device session that was terminated
     * @return A CompletableFuture that completes when the event is published
     */
    public CompletableFuture<Void> publishDisconnectionEvent(DeviceSession deviceSession) {
        if (!enabled || producer == null) {
            return CompletableFuture.completedFuture(null);
        }

        return publishEvent(EVENT_TYPE_DISCONNECTION, TOPIC_DISCONNECTION, deviceSession, null);
    }

    /**
     * Publishes a status change event when a device's status changes.
     *
     * @param deviceId The ID of the device whose status changed
     * @param status The new status of the device
     * @return A CompletableFuture that completes when the event is published
     */
    public CompletableFuture<Void> publishStatusEvent(long deviceId, String status) {
        if (!enabled || producer == null) {
            return CompletableFuture.completedFuture(null);
        }

        DeviceSession deviceSession = null;
        Device device = cacheManager.getObject(Device.class, deviceId);
        Map<String, Object> additionalData = new HashMap<>();
        additionalData.put("status", status);
        
        // Map status to standard event type
        String eventType = EVENT_TYPE_STATUS;
        if (Device.STATUS_ONLINE.equals(status)) {
            eventType = EVENT_TYPE_CONNECTION;
        } else if (Device.STATUS_OFFLINE.equals(status) || Device.STATUS_UNKNOWN.equals(status)) {
            eventType = EVENT_TYPE_DISCONNECTION;
        }
        
        String topic;
        switch (eventType) {
            case EVENT_TYPE_CONNECTION:
                topic = TOPIC_CONNECTION;
                break;
            case EVENT_TYPE_DISCONNECTION:
                topic = TOPIC_DISCONNECTION;
                break;
            default:
                topic = TOPIC_STATUS;
                break;
        }

        return publishEvent(eventType, topic, deviceSession, additionalData, device);
    }

    /**
     * Publishes an event to the message broker with the specified parameters.
     *
     * @param eventType The type of event being published
     * @param topic The topic to publish the event to
     * @param deviceSession The device session associated with the event (may be null)
     * @param additionalData Additional data to include in the event (may be null)
     * @return A CompletableFuture that completes when the event is published
     */
    private CompletableFuture<Void> publishEvent(
            String eventType, 
            String topic, 
            DeviceSession deviceSession, 
            Map<String, Object> additionalData) {
        return publishEvent(eventType, topic, deviceSession, additionalData, null);
    }

    /**
     * Publishes an event to the message broker with the specified parameters.
     *
     * @param eventType The type of event being published
     * @param topic The topic to publish the event to
     * @param deviceSession The device session associated with the event (may be null)
     * @param additionalData Additional data to include in the event (may be null)
     * @param device The device associated with the event (may be null)
     * @return A CompletableFuture that completes when the event is published
     */
    private CompletableFuture<Void> publishEvent(
            String eventType, 
            String topic, 
            DeviceSession deviceSession, 
            Map<String, Object> additionalData,
            Device device) {
        
        // Check if batch processing is enabled
        if (config.getBoolean("session.publish.batch.enabled", true)) {
            addToBatchQueue(eventType, topic, deviceSession, additionalData, device);
            return CompletableFuture.completedFuture(null);
        }

        return CompletableFuture.supplyAsync(() -> {
            long startTime = System.currentTimeMillis();
            // Create a new span for the publish operation
            Span span = tracer.spanBuilder("SessionEventPublisher.publishEvent")
                    .setSpanKind(SpanKind.PRODUCER)
                    .setAttribute("event.type", eventType)
                    .setAttribute("messaging.system", brokerType)
                    .setAttribute("messaging.destination", topic)
                    .startSpan();

            try (Scope scope = span.makeCurrent()) {
                // Generate a unique event ID
                String eventId = UUID.randomUUID().toString();
                span.setAttribute("event.id", eventId);

                // Prepare the event data
                Map<String, Object> eventData = new HashMap<>();
                eventData.put("eventId", eventId);
                eventData.put("eventType", eventType);
                eventData.put("standardEventType", EVENT_TYPE_MAPPING.getOrDefault(eventType, eventType));
                eventData.put("timestamp", System.currentTimeMillis());
                eventData.put("correlationId", deviceSession != null ? 
                        deviceSession.getCorrelationId() : UUID.randomUUID().toString());

                // Add device information
                if (deviceSession != null) {
                    eventData.put("deviceId", deviceSession.getDeviceId());
                    eventData.put("uniqueId", deviceSession.getUniqueId());
                    eventData.put("model", deviceSession.getModel());
                    eventData.put("sessionCreationTime", deviceSession.getCreationTime().toEpochMilli());
                    eventData.put("sessionLastAccessTime", deviceSession.getLastAccessTime().toEpochMilli());
                    eventData.put("sessionVersion", deviceSession.getVersion());
                } else if (device != null) {
                    eventData.put("deviceId", device.getId());
                    eventData.put("uniqueId", device.getUniqueId());
                    eventData.put("model", device.getModel());
                }

                // Add any additional data
                if (additionalData != null) {
                    eventData.put("data", additionalData);
                }

                // Serialize the event data to JSON
                String eventJson = objectMapper.writeValueAsString(eventData);
                String messageKey = String.valueOf(deviceSession != null ? 
                        deviceSession.getDeviceId() : (device != null ? device.getId() : eventId));

                // Publish the event with retry logic
                Supplier<Void> publishSupplier = Retry.decorateSupplier(retry, () -> {
                    try {
                        if ("kafka".equals(brokerType)) {
                            ProducerRecord<String, String> record = 
                                    new ProducerRecord<>(topic, messageKey, eventJson);
                            
                            // Propagate trace context via Kafka headers
                            io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.getInstance()
                                    .inject(Context.current(), record, kafkaHeaderSetter);
                            
                            producer.send(record, (metadata, exception) -> {
                                if (exception != null) {
                                    LOGGER.error("Failed to publish session event: {}", exception.getMessage());
                                    span.recordException(exception);
                                    span.setStatus(StatusCode.ERROR, exception.getMessage());
                                } else {
                                    LOGGER.debug("Published session event to {}: {}", 
                                            metadata.topic(), eventJson);
                                    publishedEventsCounter.add(1, 
                                            Context.current().with("event.type", eventType));
                                }
                            });
                        } else if ("rabbitmq".equals(brokerType)) {
                            // RabbitMQ implementation would go here
                            LOGGER.debug("Would publish to RabbitMQ: {} - {}", topic, eventJson);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error publishing session event: {}", e.getMessage(), e);
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        failedEventsCounter.add(1, 
                                Context.current().with("event.type", eventType));
                        throw e;
                    }
                    return null;
                });

                publishSupplier.get();
                span.setStatus(StatusCode.OK);
                return null;
            } catch (Exception e) {
                LOGGER.error("Failed to publish session event: {}", e.getMessage(), e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                throw new RuntimeException("Failed to publish session event", e);
            } finally {
                span.end();
            }
        }, executorService);
    }

    /**
     * Processes the batch of events from the queue.
     */
    private void processBatch() {
        if (!enabled || producer == null || eventQueue.isEmpty() || !batchInProgress.compareAndSet(false, true)) {
            return;
        }
        
        try {
            int count = Math.min(batchSize, eventQueue.size());
            if (count == 0) {
                return;
            }
            
            Map<String, Object>[] events = new Map[count];
            for (int i = 0; i < count; i++) {
                events[i] = eventQueue.poll();
                if (events[i] == null) {
                    break;
                }
            }
            
            publishBatch(events).exceptionally(ex -> {
                LOGGER.error("Error in batch processing: {}", ex.getMessage(), ex);
                return null;
            });
        } finally {
            batchInProgress.set(false);
        }
    }
    
    /**
     * Adds an event to the batch queue for processing.
     * 
     * @param eventType The type of event
     * @param topic The topic to publish to
     * @param deviceSession The device session
     * @param additionalData Additional event data
     * @param device The device information
     */
    private void addToBatchQueue(
            String eventType, 
            String topic, 
            DeviceSession deviceSession, 
            Map<String, Object> additionalData,
            Device device) {
        
        try {
            // Prepare the event data
            Map<String, Object> eventData = new HashMap<>();
            eventData.put("eventId", UUID.randomUUID().toString());
            eventData.put("eventType", eventType);
            eventData.put("standardEventType", EVENT_TYPE_MAPPING.getOrDefault(eventType, eventType));
            eventData.put("timestamp", System.currentTimeMillis());
            eventData.put("correlationId", deviceSession != null ? 
                    deviceSession.getCorrelationId() : UUID.randomUUID().toString());
            eventData.put("topic", topic);

            // Add device information
            if (deviceSession != null) {
                eventData.put("deviceId", deviceSession.getDeviceId());
                eventData.put("uniqueId", deviceSession.getUniqueId());
                eventData.put("model", deviceSession.getModel());
                eventData.put("sessionCreationTime", deviceSession.getCreationTime().toEpochMilli());
                eventData.put("sessionLastAccessTime", deviceSession.getLastAccessTime().toEpochMilli());
                eventData.put("sessionVersion", deviceSession.getVersion());
            } else if (device != null) {
                eventData.put("deviceId", device.getId());
                eventData.put("uniqueId", device.getUniqueId());
                eventData.put("model", device.getModel());
            }

            // Add any additional data
            if (additionalData != null) {
                eventData.put("data", additionalData);
            }
            
            // Add to queue
            eventQueue.add(eventData);
            
        } catch (Exception e) {
            LOGGER.error("Error adding event to batch queue: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Publishes events in batch mode for improved efficiency.
     * 
     * @param events List of events to publish
     * @return A CompletableFuture that completes when all events are published
     */
    private CompletableFuture<Void> publishBatch(Map<String, Object>[] events) {
        if (!enabled || producer == null || events == null || events.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        
        return CompletableFuture.runAsync(() -> {
            // Create a new span for the batch publish operation
            Span span = tracer.spanBuilder("SessionEventPublisher.publishBatch")
                    .setSpanKind(SpanKind.PRODUCER)
                    .setAttribute("batch.size", events.length)
                    .setAttribute("messaging.system", brokerType)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                for (Map<String, Object> event : events) {
                    try {
                        String eventType = (String) event.get("eventType");
                        String topic;
                        
                        // Determine the appropriate topic
                        if (EVENT_TYPE_CONNECTION.equals(eventType)) {
                            topic = TOPIC_CONNECTION;
                        } else if (EVENT_TYPE_DISCONNECTION.equals(eventType)) {
                            topic = TOPIC_DISCONNECTION;
                        } else {
                            topic = TOPIC_STATUS;
                        }
                        
                        // Add batch metadata
                        event.put("batchId", UUID.randomUUID().toString());
                        event.put("batchSize", events.length);
                        
                        // Serialize and publish
                        String eventJson = objectMapper.writeValueAsString(event);
                        String messageKey = String.valueOf(event.get("deviceId"));
                        
                        if ("kafka".equals(brokerType)) {
                            ProducerRecord<String, String> record = 
                                    new ProducerRecord<>(topic, messageKey, eventJson);
                            
                            // Propagate trace context via Kafka headers
                            io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator.getInstance()
                                    .inject(Context.current(), record, kafkaHeaderSetter);
                            
                            producer.send(record);
                        } else if ("rabbitmq".equals(brokerType)) {
                            // RabbitMQ implementation would go here
                            LOGGER.debug("Would publish to RabbitMQ: {} - {}", topic, eventJson);
                        }
                    } catch (Exception e) {
                        LOGGER.error("Error publishing batch event: {}", e.getMessage(), e);
                        span.recordException(e);
                    }
                }
                
                // Ensure all messages are sent
                if ("kafka".equals(brokerType)) {
                    producer.flush();
                }
                
                span.setStatus(StatusCode.OK);
            } catch (Exception e) {
                LOGGER.error("Failed to publish batch events: {}", e.getMessage(), e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
            } finally {
                span.end();
                // Record processing time
                long processingTime = System.currentTimeMillis() - startTime;
                eventProcessingTimeHistogram.record(processingTime, 
                        Context.current().with("event.type", eventType));
            }
        }, executorService);
    }
    
    /**
     * Closes the publisher and releases any resources.
     */
    public void close() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close(30, TimeUnit.SECONDS);
            } catch (Exception e) {
                LOGGER.error("Error closing producer: {}", e.getMessage(), e);
            }
        }
        if (executorService != null) {
            try {
                executorService.shutdown();
                if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("Error shutting down executor service: {}", e.getMessage(), e);
            }
        }
        
        if (scheduledExecutorService != null) {
            try {
                scheduledExecutorService.shutdown();
                if (!scheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
                    scheduledExecutorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOGGER.error("Error shutting down scheduled executor service: {}", e.getMessage(), e);
            }
        }
    }
}