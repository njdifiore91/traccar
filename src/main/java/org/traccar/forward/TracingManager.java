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
package org.traccar.forward;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages distributed tracing context propagation across service boundaries in the forwarding subsystem.
 * Integrates with OpenTelemetry to create, propagate, and extract trace contexts for both synchronous
 * and asynchronous operations, ensuring correlation IDs are maintained throughout the entire request lifecycle.
 */
@Singleton
public class TracingManager {

    private final Tracer tracer;
    private final OpenTelemetry openTelemetry;
    private final Map<String, Context> asyncContexts;
    private final double samplingRate;

    /**
     * Text map setter for injecting trace context into carriers like HTTP headers or message properties.
     */
    private static final TextMapSetter<Map<String, String>> SETTER = 
            (carrier, key, value) -> {
                if (carrier != null) {
                    carrier.put(key, value);
                }
            };

    /**
     * Text map getter for extracting trace context from carriers like HTTP headers or message properties.
     */
    private static final TextMapGetter<Map<String, String>> GETTER = 
            new TextMapGetter<>() {
                @Override
                public Iterable<String> keys(Map<String, String> carrier) {
                    return carrier.keySet();
                }

                @Override
                public String get(Map<String, String> carrier, String key) {
                    return carrier.get(key);
                }
            };

    /**
     * Constructs a new TracingManager with the provided OpenTelemetry instance.
     *
     * @param openTelemetry The OpenTelemetry instance for creating tracers and propagating context
     * @param samplingRate The sampling rate for tracing (0.0 to 1.0)
     */
    @Inject
    public TracingManager(OpenTelemetry openTelemetry, double samplingRate) {
        this.openTelemetry = openTelemetry;
        this.tracer = openTelemetry.getTracer("org.traccar.forward");
        this.asyncContexts = new ConcurrentHashMap<>();
        this.samplingRate = samplingRate;
    }

    /**
     * Creates a new span for a position forwarding operation.
     *
     * @param positionData The position data being forwarded
     * @param targetSystem The target system identifier
     * @return A span representing the forwarding operation
     */
    public Span createPositionForwardingSpan(PositionData positionData, String targetSystem) {
        SpanBuilder spanBuilder = tracer.spanBuilder("forward.position")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("forward.target", targetSystem)
                .setAttribute("forward.type", "position")
                .setAttribute("device.id", positionData.getDeviceId());

        if (positionData.getPositionId() != null) {
            spanBuilder.setAttribute("position.id", positionData.getPositionId());
        }

        return spanBuilder.startSpan();
    }

    /**
     * Creates a new span for an event forwarding operation.
     *
     * @param eventData The event data being forwarded
     * @param targetSystem The target system identifier
     * @return A span representing the forwarding operation
     */
    public Span createEventForwardingSpan(EventData eventData, String targetSystem) {
        SpanBuilder spanBuilder = tracer.spanBuilder("forward.event")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("forward.target", targetSystem)
                .setAttribute("forward.type", "event")
                .setAttribute("device.id", eventData.getDeviceId());

        if (eventData.getEventId() != null) {
            spanBuilder.setAttribute("event.id", eventData.getEventId());
        }
        if (eventData.getEventType() != null) {
            spanBuilder.setAttribute("event.type", eventData.getEventType());
        }

        return spanBuilder.startSpan();
    }

    /**
     * Injects the current trace context into a carrier for propagation across service boundaries.
     *
     * @param carrier The carrier to inject context into (e.g., HTTP headers, message properties)
     */
    public void injectContext(Map<String, String> carrier) {
        openTelemetry.getPropagators().getTextMapPropagator()
                .inject(Context.current(), carrier, SETTER);
    }

    /**
     * Extracts trace context from a carrier and returns it.
     *
     * @param carrier The carrier containing the trace context (e.g., HTTP headers, message properties)
     * @return The extracted context
     */
    public Context extractContext(Map<String, String> carrier) {
        return openTelemetry.getPropagators().getTextMapPropagator()
                .extract(Context.current(), carrier, GETTER);
    }

    /**
     * Stores the current context for later use in asynchronous operations.
     *
     * @return A correlation ID that can be used to retrieve the context later
     */
    public String storeCurrentContext() {
        String correlationId = generateCorrelationId();
        asyncContexts.put(correlationId, Context.current());
        return correlationId;
    }

    /**
     * Retrieves a previously stored context for an asynchronous operation.
     *
     * @param correlationId The correlation ID returned by storeCurrentContext()
     * @return The stored context, or null if not found
     */
    public Context retrieveContext(String correlationId) {
        return asyncContexts.get(correlationId);
    }

    /**
     * Removes a stored context after it's no longer needed.
     *
     * @param correlationId The correlation ID of the context to remove
     */
    public void removeContext(String correlationId) {
        asyncContexts.remove(correlationId);
    }

    /**
     * Executes a synchronous operation with the current trace context.
     *
     * @param span The span representing the operation
     * @param runnable The operation to execute
     */
    public void withSpan(Span span, Runnable runnable) {
        try (Scope scope = span.makeCurrent()) {
            runnable.run();
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Executes an asynchronous operation with the provided trace context.
     *
     * @param context The trace context to use
     * @param span The span representing the operation
     * @param runnable The operation to execute
     */
    public void withContext(Context context, Span span, Runnable runnable) {
        try (Scope scope = context.with(span).makeCurrent()) {
            runnable.run();
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Creates a result handler that completes the provided span based on the operation result.
     *
     * @param span The span to complete
     * @return A result handler that updates and ends the span
     */
    public ResultHandler createResultHandler(Span span) {
        return (success, throwable) -> {
            if (success) {
                span.setStatus(StatusCode.OK);
            } else {
                span.setStatus(StatusCode.ERROR, throwable != null ? throwable.getMessage() : "Unknown error");
                if (throwable != null) {
                    span.recordException(throwable);
                }
            }
            span.end();
        };
    }

    /**
     * Creates a result handler that completes the provided span and removes the stored context.
     *
     * @param span The span to complete
     * @param correlationId The correlation ID of the stored context to remove
     * @return A result handler that updates the span, ends it, and removes the stored context
     */
    public ResultHandler createAsyncResultHandler(Span span, String correlationId) {
        return (success, throwable) -> {
            if (success) {
                span.setStatus(StatusCode.OK);
            } else {
                span.setStatus(StatusCode.ERROR, throwable != null ? throwable.getMessage() : "Unknown error");
                if (throwable != null) {
                    span.recordException(throwable);
                }
            }
            span.end();
            removeContext(correlationId);
        };
    }

    /**
     * Generates a unique correlation ID for tracking asynchronous operations.
     *
     * @return A unique correlation ID
     */
    private String generateCorrelationId() {
        return UUID.randomUUID().toString();
    }

    /**
     * Gets the configured sampling rate for tracing.
     *
     * @return The sampling rate (0.0 to 1.0)
     */
    public double getSamplingRate() {
        return samplingRate;
    }

    /**
     * Creates attributes for a forwarding operation.
     *
     * @param targetSystem The target system identifier
     * @param forwardType The type of forwarding (position or event)
     * @return Attributes for the forwarding operation
     */
    public Attributes createForwardingAttributes(String targetSystem, String forwardType) {
        return Attributes.builder()
                .put("forward.target", targetSystem)
                .put("forward.type", forwardType)
                .build();
    }
}