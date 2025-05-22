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
package org.traccar.telemetry;

import io.opentelemetry.api.OpenTelemetry;
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
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

/**
 * Provider for OpenTelemetry tracing.
 */
@Singleton
public class TracingProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(TracingProvider.class);

    private final OpenTelemetry openTelemetry;

    @Inject
    public TracingProvider(Config config) {
        boolean tracingEnabled = config.getBoolean(Keys.TELEMETRY_TRACING_ENABLED.getKey(), false);
        String serviceName = config.getString(Keys.TELEMETRY_SERVICE_NAME.getKey(), "traccar");
        String endpoint = config.getString(Keys.TELEMETRY_TRACING_ENDPOINT.getKey(), "http://localhost:4317");

        if (tracingEnabled) {
            LOGGER.info("Initializing OpenTelemetry tracing with endpoint: {}", endpoint);

            Resource resource = Resource.getDefault()
                    .merge(Resource.builder()
                            .put(ResourceAttributes.SERVICE_NAME, serviceName)
                            .build());

            SdkTracerProvider sdkTracerProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(BatchSpanProcessor.builder(
                            OtlpGrpcSpanExporter.builder()
                                    .setEndpoint(endpoint)
                                    .build())
                            .build())
                    .setResource(resource)
                    .build();

            openTelemetry = OpenTelemetrySdk.builder()
                    .setTracerProvider(sdkTracerProvider)
                    .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                    .build();
        } else {
            LOGGER.info("OpenTelemetry tracing is disabled");
            openTelemetry = OpenTelemetry.noop();
        }
    }

    /**
     * Get a tracer for the specified instrumentation scope.
     *
     * @param instrumentationScope The instrumentation scope name
     * @return A tracer instance
     */
    public Tracer getTracer(String instrumentationScope) {
        return openTelemetry.getTracer(instrumentationScope);
    }

    /**
     * Get the OpenTelemetry instance.
     *
     * @return The OpenTelemetry instance
     */
    public OpenTelemetry getOpenTelemetry() {
        return openTelemetry;
    }
}