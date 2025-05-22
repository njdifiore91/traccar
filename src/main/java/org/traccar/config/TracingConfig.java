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
package org.traccar.config;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

/**
 * Configuration for distributed tracing using OpenTelemetry.
 * This class provides configuration options for OpenTelemetry instrumentation,
 * including trace sampling strategies, span exporters, and context propagation mechanisms.
 * It enables end-to-end visibility of request flows across service boundaries.
 */
@Singleton
public class TracingConfig {

    private final Config config;
    private final OpenTelemetry openTelemetry;

    /**
     * Configuration keys for tracing settings.
     */
    public static final class Keys {
        /**
         * Enable OpenTelemetry tracing.
         */
        public static final ConfigKey<Boolean> TRACING_ENABLED = new BooleanConfigKey(
                "tracing.enabled",
                java.util.List.of(KeyType.CONFIG),
                false);

        /**
         * OpenTelemetry exporter type. Available options: otlp-http, otlp-grpc, logging.
         */
        public static final ConfigKey<String> TRACING_EXPORTER = new StringConfigKey(
                "tracing.exporter",
                java.util.List.of(KeyType.CONFIG),
                "otlp-http");

        /**
         * OpenTelemetry exporter endpoint URL.
         */
        public static final ConfigKey<String> TRACING_ENDPOINT = new StringConfigKey(
                "tracing.endpoint",
                java.util.List.of(KeyType.CONFIG),
                "http://localhost:4318/v1/traces");

        /**
         * OpenTelemetry sampling ratio (0.0-1.0). Default is 0.1 (10% of traces).
         */
        public static final ConfigKey<Double> TRACING_SAMPLING_RATIO = new DoubleConfigKey(
                "tracing.sampling.ratio",
                java.util.List.of(KeyType.CONFIG),
                0.1);

        /**
         * OpenTelemetry service name.
         */
        public static final ConfigKey<String> TRACING_SERVICE_NAME = new StringConfigKey(
                "tracing.service.name",
                java.util.List.of(KeyType.CONFIG),
                "traccar");

        /**
         * OpenTelemetry service namespace.
         */
        public static final ConfigKey<String> TRACING_SERVICE_NAMESPACE = new StringConfigKey(
                "tracing.service.namespace",
                java.util.List.of(KeyType.CONFIG),
                "org.traccar");

        /**
         * OpenTelemetry service version.
         */
        public static final ConfigKey<String> TRACING_SERVICE_VERSION = new StringConfigKey(
                "tracing.service.version",
                java.util.List.of(KeyType.CONFIG),
                "1.0.0");

        /**
         * OpenTelemetry deployment environment.
         */
        public static final ConfigKey<String> TRACING_ENVIRONMENT = new StringConfigKey(
                "tracing.environment",
                java.util.List.of(KeyType.CONFIG),
                "production");
    }

    /**
     * Constructs a new TracingConfig instance.
     *
     * @param config The application configuration
     */
    @Inject
    public TracingConfig(Config config) {
        this.config = config;
        this.openTelemetry = initializeOpenTelemetry();
    }

    /**
     * Gets the configured OpenTelemetry instance.
     *
     * @return The OpenTelemetry instance
     */
    public OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }

    /**
     * Initializes the OpenTelemetry SDK with the configured settings.
     *
     * @return The configured OpenTelemetry instance
     */
    private OpenTelemetry initializeOpenTelemetry() {
        if (!config.getBoolean(Keys.TRACING_ENABLED)) {
            return OpenTelemetry.noop();
        }

        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        ResourceAttributes.SERVICE_NAME, config.getString(Keys.TRACING_SERVICE_NAME),
                        ResourceAttributes.SERVICE_NAMESPACE, config.getString(Keys.TRACING_SERVICE_NAMESPACE),
                        ResourceAttributes.SERVICE_VERSION, config.getString(Keys.TRACING_SERVICE_VERSION),
                        ResourceAttributes.DEPLOYMENT_ENVIRONMENT, config.getString(Keys.TRACING_ENVIRONMENT))));

        SpanExporter spanExporter = createSpanExporter();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .setSampler(Sampler.traceIdRatioBased(config.getDouble(Keys.TRACING_SAMPLING_RATIO)))
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .build();

        // Configure context propagation for distributed tracing
        TextMapPropagator propagator = W3CTraceContextPropagator.getInstance();
        ContextPropagators propagators = ContextPropagators.create(propagator);

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(propagators)
                .buildAndRegisterGlobal();
    }

    /**
     * Creates a span exporter based on the configured exporter type.
     *
     * @return The configured span exporter
     */
    private SpanExporter createSpanExporter() {
        String exporterType = config.getString(Keys.TRACING_EXPORTER);
        String endpoint = config.getString(Keys.TRACING_ENDPOINT);

        switch (exporterType) {
            case "otlp-http":
                return OtlpHttpSpanExporter.builder()
                        .setEndpoint(endpoint)
                        .build();
            case "otlp-grpc":
                return OtlpGrpcSpanExporter.builder()
                        .setEndpoint(endpoint)
                        .build();
            case "logging":
                return new io.opentelemetry.exporter.logging.LoggingSpanExporter();
            default:
                throw new IllegalArgumentException("Unsupported exporter type: " + exporterType);
        }
    }
}