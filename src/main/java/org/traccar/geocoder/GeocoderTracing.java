/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.geocoder;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.database.StatisticsManager;

import java.util.HashMap;
import java.util.Map;

/**
 * A decorator for Geocoder implementations that adds distributed tracing capabilities
 * using OpenTelemetry. This class wraps an existing Geocoder and adds spans for geocoding
 * operations, enabling end-to-end visibility of geocoding requests across service boundaries.
 */
public class GeocoderTracing implements Geocoder {

    private static final String SPAN_NAME = "geocoder.getAddress";
    private static final String INSTRUMENTATION_SCOPE = "org.traccar.geocoder";
    private static final String GEOCODER_PROVIDER_KEY = "geocoder.provider";
    private static final String GEOCODER_SERVICE_KEY = "geocoder.service";
    private static final String GEOCODER_LATITUDE_KEY = "geocoder.latitude";
    private static final String GEOCODER_LONGITUDE_KEY = "geocoder.longitude";
    private static final String GEOCODER_RESULT_KEY = "geocoder.result";
    private static final String GEOCODER_ERROR_KEY = "geocoder.error";

    private final Geocoder delegate;
    private final Tracer tracer;
    private String traceId;
    private String spanId;
    private boolean sampled;

    /**
     * Creates a new GeocoderTracing instance that wraps the provided Geocoder.
     *
     * @param delegate The Geocoder implementation to wrap
     */
    public GeocoderTracing(Geocoder delegate) {
        this.delegate = delegate;
        this.tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    }

    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        // Create a span for the geocoding operation
        SpanBuilder spanBuilder = tracer.spanBuilder(SPAN_NAME)
                .setSpanKind(SpanKind.CLIENT);

        // If we have trace context, set it as the parent
        if (traceId != null && spanId != null) {
            // Use the current context which should already have the trace context
            // If not explicitly set, it will use the current context by default
            spanBuilder.setParent(Context.current());
        }

        Span span = spanBuilder.startSpan();

        // Add attributes to the span
        span.setAttribute(GEOCODER_PROVIDER_KEY, delegate.getClass().getSimpleName());
        span.setAttribute(GEOCODER_SERVICE_KEY, "geocoding");
        span.setAttribute(GEOCODER_LATITUDE_KEY, latitude);
        span.setAttribute(GEOCODER_LONGITUDE_KEY, longitude);

        try (Scope scope = span.makeCurrent()) {
            // If callback is provided, wrap it to capture the result or error
            if (callback != null) {
                return delegate.getAddress(latitude, longitude, new ReverseGeocoderCallback() {
                    @Override
                    public void onSuccess(String address) {
                        span.setAttribute(GEOCODER_RESULT_KEY, address != null ? address : "null");
                        span.setStatus(StatusCode.OK);
                        span.end();
                        callback.onSuccess(address);
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        span.recordException(e);
                        span.setAttribute(GEOCODER_ERROR_KEY, e.getMessage());
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        span.end();
                        callback.onFailure(e);
                    }
                });
            } else {
                // Synchronous call
                try {
                    String address = delegate.getAddress(latitude, longitude, null);
                    span.setAttribute(GEOCODER_RESULT_KEY, address != null ? address : "null");
                    span.setStatus(StatusCode.OK);
                    return address;
                } catch (Exception e) {
                    span.recordException(e);
                    span.setAttribute(GEOCODER_ERROR_KEY, e.getMessage());
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                } finally {
                    span.end();
                }
            }
        }
    }

    @Override
    public void setStatisticsManager(StatisticsManager statisticsManager) {
        delegate.setStatisticsManager(statisticsManager);
    }

    @Override
    public void setTraceContext(String traceId, String spanId, boolean sampled) {
        this.traceId = traceId;
        this.spanId = spanId;
        this.sampled = sampled;
        
        // Also propagate to delegate if it supports tracing
        delegate.setTraceContext(traceId, spanId, sampled);
    }

    @Override
    public void registerWithServiceDiscovery(String serviceId, Map<String, String> metadata) {
        delegate.registerWithServiceDiscovery(serviceId, metadata);
    }

    @Override
    public void deregisterFromServiceDiscovery(String serviceId) {
        delegate.deregisterFromServiceDiscovery(serviceId);
    }

    @Override
    public Map<String, Double> getMetrics() {
        Map<String, Double> metrics = new HashMap<>(delegate.getMetrics());
        // Add any additional metrics specific to tracing if needed
        return metrics;
    }

    @Override
    public boolean isHealthy() {
        return delegate.isHealthy();
    }

    @Override
    public Map<String, Object> getHealthDetails() {
        Map<String, Object> details = new HashMap<>(delegate.getHealthDetails());
        // Add any additional health details specific to tracing if needed
        return details;
    }

    @Override
    public void configureFromEnvironment(String environmentPrefix) {
        delegate.configureFromEnvironment(environmentPrefix);
    }

    @Override
    public void initializeForContainer(Map<String, String> containerConfig) {
        delegate.initializeForContainer(containerConfig);
    }
}