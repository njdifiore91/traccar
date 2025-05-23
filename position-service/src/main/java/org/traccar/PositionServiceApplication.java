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
package org.traccar;

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.endpoint.condition.ConditionalOnAvailableEndpoint;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties.AckMode;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Main application class for the Position Processing Service.
 * This service is responsible for processing and enriching position data from GPS devices.
 * It consumes raw position data from the message broker, processes it through various handlers,
 * and publishes enriched position data back to the message broker.
 * 
 * Key responsibilities:
 * - Process and enrich position data with geocoding, geolocation, distance calculation, and motion detection
 * - Implement a pipeline of position handlers for sequential data enrichment
 * - Store positions in the database via transactional outbox pattern
 * - Publish enriched positions to the message broker for consumption by other services
 */
@SpringBootApplication
@EnableDiscoveryClient // Enable service discovery registration
@EnableKafka // Enable Kafka message broker integration
@ComponentScan(basePackages = {"org.traccar"})
@ConfigurationPropertiesScan("org.traccar")
public class PositionServiceApplication {

    /**
     * Main method to start the Position Service application.
     * 
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(PositionServiceApplication.class, args);
    }
    
    /**
     * Configuration class for OpenTelemetry distributed tracing.
     */
    @Configuration
    static class OpenTelemetryConfig {
        
        @Value("${spring.application.name}")
        private String serviceName;
        
        @Value("${opentelemetry.exporter.otlp.endpoint:http://jaeger:4317}")
        private String jaegerEndpoint;
        
        /**
         * Configures OpenTelemetry for distributed tracing.
         * 
         * @return Configured OpenTelemetry instance
         */
        @Bean
        public OpenTelemetry openTelemetry() {
            Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                    ResourceAttributes.SERVICE_NAME, serviceName,
                    ResourceAttributes.SERVICE_VERSION, "1.0.0")));
            
            SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .setSampler(Sampler.alwaysOn())
                .build();
            
            return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(
                    W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();
        }
    }
    
    /**
     * Configuration class for Micrometer metrics collection.
     */
    @Configuration
    static class MetricsConfig {
        
        /**
         * Configures JVM garbage collection metrics.
         * 
         * @param registry Meter registry for metrics collection
         * @return JVM GC metrics
         */
        @Bean
        public JvmGcMetrics jvmGcMetrics(MeterRegistry registry) {
            JvmGcMetrics gcMetrics = new JvmGcMetrics();
            gcMetrics.bindTo(registry);
            return gcMetrics;
        }
        
        /**
         * Configures JVM memory metrics.
         * 
         * @param registry Meter registry for metrics collection
         * @return JVM memory metrics
         */
        @Bean
        public JvmMemoryMetrics jvmMemoryMetrics(MeterRegistry registry) {
            JvmMemoryMetrics memoryMetrics = new JvmMemoryMetrics();
            memoryMetrics.bindTo(registry);
            return memoryMetrics;
        }
        
        /**
         * Configures processor metrics.
         * 
         * @param registry Meter registry for metrics collection
         * @return Processor metrics
         */
        @Bean
        public ProcessorMetrics processorMetrics(MeterRegistry registry) {
            ProcessorMetrics processorMetrics = new ProcessorMetrics();
            processorMetrics.bindTo(registry);
            return processorMetrics;
        }
    }
    
    /**
     * Configuration class for Kafka message broker.
     */
    @Configuration
    static class KafkaConfig {
        
        @Value("${kafka.bootstrap-servers}")
        private String bootstrapServers;
        
        @Value("${kafka.consumer.group-id}")
        private String groupId;
        
        @Value("${kafka.consumer.auto-offset-reset:earliest}")
        private String autoOffsetReset;
        
        @Value("${kafka.consumer.max-poll-records:500}")
        private int maxPollRecords;
        
        @Value("${kafka.consumer.concurrency:10}")
        private int concurrency;
        
        /**
         * Configures Kafka consumer factory.
         * 
         * @return Kafka consumer factory
         */
        @Bean
        public ConsumerFactory<String, String> consumerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
            
            return new DefaultKafkaConsumerFactory<>(props);
        }
        
        /**
         * Configures Kafka listener container factory.
         * 
         * @param consumerFactory Kafka consumer factory
         * @return Kafka listener container factory
         */
        @Bean
        public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
                ConsumerFactory<String, String> consumerFactory) {
            
            ConcurrentKafkaListenerContainerFactory<String, String> factory = 
                new ConcurrentKafkaListenerContainerFactory<>();
            factory.setConsumerFactory(consumerFactory);
            factory.setConcurrency(concurrency);
            factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
            
            return factory;
        }
        
        /**
         * Configures Kafka producer factory.
         * 
         * @return Kafka producer factory
         */
        @Bean
        public ProducerFactory<String, String> producerFactory() {
            Map<String, Object> props = new HashMap<>();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
            props.put(ProducerConfig.ACKS_CONFIG, "all");
            props.put(ProducerConfig.RETRIES_CONFIG, 3);
            props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
            
            return new DefaultKafkaProducerFactory<>(props);
        }
        
        /**
         * Configures Kafka template for sending messages.
         * 
         * @param producerFactory Kafka producer factory
         * @return Kafka template
         */
        @Bean
        public KafkaTemplate<String, String> kafkaTemplate(
                ProducerFactory<String, String> producerFactory) {
            return new KafkaTemplate<>(producerFactory);
        }
    }
    
    /**
     * Configuration class for resilience patterns.
     */
    @Configuration
    static class ResilienceConfig {
        
        /**
         * Configures circuit breaker registry.
         * 
         * @return Circuit breaker registry
         */
        @Bean
        public CircuitBreakerRegistry circuitBreakerRegistry() {
            CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(10)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
            
            return CircuitBreakerRegistry.of(config);
        }
        
        /**
         * Configures retry registry.
         * 
         * @return Retry registry
         */
        @Bean
        public RetryRegistry retryRegistry() {
            RetryConfig config = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofSeconds(1))
                .enableExponentialBackoff(true)
                .exponentialBackoffMultiplier(2)
                .build();
            
            return RetryRegistry.of(config);
        }
    }
    
    /**
     * Custom health indicator for the Position Service.
     */
    @Bean
    public HealthIndicator positionServiceHealthIndicator() {
        return () -> {
            // In a real implementation, this would check critical dependencies
            // such as database connection, message broker connectivity, etc.
            return Health.up()
                .withDetail("service", "position-service")
                .withDetail("status", "operational")
                .build();
        };
    }
}