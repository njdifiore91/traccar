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
package org.traccar.broadcast;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Kafka-based implementation of the BroadcastService.
 * Enables asynchronous communication between microservices using Kafka as a message broker.
 */
public class KafkaBroadcastService extends BaseBroadcastService {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaBroadcastService.class);

    private final ObjectMapper objectMapper;
    private final ExecutorService executorService;
    private final String topic;
    private final String groupId;
    private final String clientId;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final CircuitBreaker circuitBreaker;
    private final KafkaClientMetrics producerMetrics;
    private final KafkaClientMetrics consumerMetrics;
    private final Timer producerTimer;
    private final Timer consumerTimer;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * Constructs a new KafkaBroadcastService.
     *
     * @param config           Configuration for Kafka connection settings
     * @param executorService Executor service for running the consumer
     * @param objectMapper    Object mapper for serializing/deserializing messages
     * @param meterRegistry   Registry for collecting metrics
     * @param openTelemetry   OpenTelemetry instance for distributed tracing
     * @throws IOException If there is an error connecting to Kafka
     */
    public KafkaBroadcastService(
            Config config,
            ExecutorService executorService,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            OpenTelemetry openTelemetry) throws IOException {

        this.executorService = executorService;
        this.objectMapper = objectMapper;
        this.topic = config.getString(Keys.BROADCAST_TOPIC, "traccar-broadcast");
        this.groupId = config.getString(Keys.BROADCAST_GROUP_ID, "traccar-group");
        this.clientId = "traccar-" + UUID.randomUUID().toString();

        // Initialize OpenTelemetry
        this.tracer = openTelemetry.getTracer("org.traccar.broadcast.KafkaBroadcastService");
        this.propagator = openTelemetry.getPropagators().getTextMapPropagator();

        // Configure circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(10)
                .slidingWindowSize(100)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("kafka-broadcast");

        // Configure metrics
        Tags tags = Tags.of(
                Tag.of("component", "broadcast"),
                Tag.of("type", "kafka"),
                Tag.of("clientId", clientId));

        this.producerTimer = Timer.builder("kafka.broadcast.producer")
                .tags(tags)
                .description("Timer for Kafka broadcast producer operations")
                .register(meterRegistry);

        this.consumerTimer = Timer.builder("kafka.broadcast.consumer")
                .tags(tags)
                .description("Timer for Kafka broadcast consumer operations")
                .register(meterRegistry);

        try {
            // Initialize Kafka producer
            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                    config.getString(Keys.BROADCAST_ADDRESS));
            producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId + "-producer");
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.ACKS_CONFIG, "all");
            producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            producerProps.put(ProducerConfig.RETRIES_CONFIG, 3);
            producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
            producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);

            producer = new KafkaProducer<>(producerProps);
            producerMetrics = new KafkaClientMetrics(producer, tags);
            producerMetrics.bindTo(meterRegistry);

            // Initialize Kafka consumer
            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                    config.getString(Keys.BROADCAST_ADDRESS));
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId + "-consumer");
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            consumerProps.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 1000);
            consumerProps.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 10000);

            consumer = new KafkaConsumer<>(consumerProps);
            consumerMetrics = new KafkaClientMetrics(consumer, tags);
            consumerMetrics.bindTo(meterRegistry);

        } catch (Exception e) {
            LOGGER.error("Failed to initialize Kafka broadcast service", e);
            throw new IOException(e);
        }
    }

    @Override
    public boolean singleInstance() {
        return false; // Multiple instances can run in parallel with Kafka
    }

    @Override
    protected void sendMessage(BroadcastMessage message) {
        Span span = tracer.spanBuilder("kafka.broadcast.send")
                .setSpanKind(SpanKind.PRODUCER)
                .startSpan();

        try (Timer.Sample sample = Timer.start()) {
            Context context = Context.current().with(span);
            Context.current().makeCurrent();

            String payload = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, payload);

            // Inject tracing context into Kafka headers
            propagator.inject(context, record.headers(), this::injectIntoHeaders);

            // Send message with circuit breaker
            circuitBreaker.executeRunnable(() -> {
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.warn("Failed to send broadcast message", exception);
                        span.setStatus(StatusCode.ERROR, exception.getMessage());
                    } else {
                        span.setAttribute("kafka.topic", metadata.topic());
                        span.setAttribute("kafka.partition", metadata.partition());
                        span.setAttribute("kafka.offset", metadata.offset());
                    }
                });
            });

            sample.stop(producerTimer);
        } catch (Exception e) {
            LOGGER.warn("Error serializing broadcast message", e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }

    private void injectIntoHeaders(Headers headers, String key, String value) {
        headers.add(key, value.getBytes());
    }

    @Override
    public void start() throws IOException {
        if (running.compareAndSet(false, true)) {
            try {
                consumer.subscribe(Collections.singletonList(topic));
                executorService.submit(receiver);
                LOGGER.info("Kafka broadcast service started");
            } catch (Exception e) {
                running.set(false);
                LOGGER.error("Failed to start Kafka broadcast service", e);
                throw new IOException(e);
            }
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            try {
                if (consumer != null) {
                    consumer.wakeup();
                    consumer.close(Duration.ofSeconds(5));
                    consumerMetrics.close();
                }
            } catch (Exception e) {
                LOGGER.warn("Error closing Kafka consumer", e);
            }

            try {
                if (producer != null) {
                    producer.flush();
                    producer.close(Duration.ofSeconds(5));
                    producerMetrics.close();
                }
            } catch (Exception e) {
                LOGGER.warn("Error closing Kafka producer", e);
            }

            LOGGER.info("Kafka broadcast service stopped");
        }
    }

    private final Runnable receiver = new Runnable() {
        @Override
        public void run() {
            while (running.get()) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        Timer.Sample sample = Timer.start();
                        Span span = tracer.spanBuilder("kafka.broadcast.receive")
                                .setSpanKind(SpanKind.CONSUMER)
                                .startSpan();

                        try {
                            // Extract tracing context from Kafka headers
                            Context extractedContext = propagator.extract(Context.current(), record.headers(), 
                                    (headers, key) -> new String(headers.lastHeader(key).value()));
                            extractedContext.makeCurrent();

                            span.setAttribute("kafka.topic", record.topic());
                            span.setAttribute("kafka.partition", record.partition());
                            span.setAttribute("kafka.offset", record.offset());

                            BroadcastMessage message = objectMapper.readValue(record.value(), BroadcastMessage.class);
                            handleMessage(message);

                            sample.stop(consumerTimer);
                        } catch (Exception e) {
                            LOGGER.warn("Failed to process broadcast message", e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                        } finally {
                            span.end();
                        }
                    }
                } catch (Exception e) {
                    if (running.get()) {
                        LOGGER.warn("Error polling Kafka", e);
                    }
                }
            }
        }
    };
}