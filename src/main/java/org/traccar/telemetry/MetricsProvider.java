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
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.time.Duration;

/**
 * Provider for OpenTelemetry metrics.
 */
@Singleton
public class MetricsProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsProvider.class);

    private final OpenTelemetry openTelemetry;

    @Inject
    public MetricsProvider(Config config) {
        boolean metricsEnabled = config.getBoolean(Keys.TELEMETRY_METRICS_ENABLED.getKey(), false);
        String serviceName = config.getString(Keys.TELEMETRY_SERVICE_NAME.getKey(), "traccar");
        String endpoint = config.getString(Keys.TELEMETRY_METRICS_ENDPOINT.getKey(), "http://localhost:4317");
        long exportIntervalMillis = config.getLong(Keys.TELEMETRY_METRICS_EXPORT_INTERVAL.getKey(), 15000);

        if (metricsEnabled) {
            LOGGER.info("Initializing OpenTelemetry metrics with endpoint: {}", endpoint);

            Resource resource = Resource.getDefault()
                    .merge(Resource.builder()
                            .put(ResourceAttributes.SERVICE_NAME, serviceName)
                            .build());

            SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
                    .setResource(resource)
                    .registerMetricReader(PeriodicMetricReader.builder(
                            OtlpGrpcMetricExporter.builder()
                                    .setEndpoint(endpoint)
                                    .build())
                            .setInterval(Duration.ofMillis(exportIntervalMillis))
                            .build())
                    .build();

            openTelemetry = OpenTelemetrySdk.builder()
                    .setMeterProvider(sdkMeterProvider)
                    .build();
        } else {
            LOGGER.info("OpenTelemetry metrics are disabled");
            openTelemetry = OpenTelemetry.noop();
        }
    }

    /**
     * Get a meter for the specified instrumentation scope.
     *
     * @param instrumentationScope The instrumentation scope name
     * @return A meter instance
     */
    public Meter getMeter(String instrumentationScope) {
        return openTelemetry.getMeter(instrumentationScope);
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