/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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

import com.google.inject.AbstractModule;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;

import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configuration module for OpenTelemetry distributed tracing integration.
 * This module configures the OpenTelemetry SDK and provides a Tracer for creating spans.
 */
public class OpenTelemetryConfig extends AbstractModule {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenTelemetryConfig.class);

    @Override
    protected void configure() {
        bind(OpenTelemetry.class).toProvider(OpenTelemetryProvider.class).in(Singleton.class);
        bind(Tracer.class).toProvider(TracerProvider.class).in(Singleton.class);
    }

    /**
     * Provider for OpenTelemetry SDK configuration.
     */
    public static class OpenTelemetryProvider implements Provider<OpenTelemetry> {

        private final Config config;

        @Inject
        public OpenTelemetryProvider(Config config) {
            this.config = config;
        }

        @Override
        public OpenTelemetry get() {
            String serviceName = config.getString("opentelemetry.serviceName", "api-gateway");
            String endpoint = config.getString("opentelemetry.endpoint", "http://localhost:4317");
            
            LOGGER.info("Initializing OpenTelemetry with service name: {} and endpoint: {}", serviceName, endpoint);

            Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                    ResourceAttributes.SERVICE_NAME, serviceName)));

            OtlpGrpcSpanExporter spanExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(endpoint)
                .build();

            SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
                .setResource(resource)
                .build();

            return OpenTelemetrySdk.builder()
                .setTracerProvider(sdkTracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .buildAndRegisterGlobal();
        }
    }

    /**
     * Provider for Tracer instance.
     */
    public static class TracerProvider implements Provider<Tracer> {

        private final OpenTelemetry openTelemetry;

        @Inject
        public TracerProvider(OpenTelemetry openTelemetry) {
            this.openTelemetry = openTelemetry;
        }

        @Override
        public Tracer get() {
            return openTelemetry.getTracer("org.traccar");
        }
    }
}