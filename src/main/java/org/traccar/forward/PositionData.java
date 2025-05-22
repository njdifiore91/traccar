/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanId;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceId;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapPropagator;

import org.traccar.model.Device;
import org.traccar.model.Position;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * PositionData encapsulates position and device information for forwarding to external systems.
 * This class has been enhanced to support message broker compatibility, distributed tracing,
 * Protocol Buffer serialization, and schema versioning for the microservices architecture.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PositionData implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /**
     * Schema version for backward compatibility.
     * Increment this when making breaking changes to the structure.
     */
    @JsonProperty("schemaVersion")
    private int schemaVersion = 1;

    /**
     * Position information.
     */
    private Position position;

    /**
     * Device information.
     */
    private Device device;

    /**
     * OpenTelemetry trace ID for distributed tracing.
     */
    @JsonProperty("traceId")
    private String traceId;

    /**
     * OpenTelemetry span ID for distributed tracing.
     */
    @JsonProperty("spanId")
    private String spanId;

    /**
     * OpenTelemetry trace flags for distributed tracing.
     */
    @JsonProperty("traceFlags")
    private byte traceFlags;

    /**
     * OpenTelemetry trace state for distributed tracing.
     */
    @JsonProperty("traceState")
    private String traceState;

    /**
     * Additional metadata fields required by microservices.
     */
    @JsonProperty("metadata")
    private Map<String, String> metadata = new HashMap<>();

    /**
     * Timestamp when this message was created, used for message expiration and metrics.
     */
    @JsonProperty("timestamp")
    private long timestamp = System.currentTimeMillis();

    /**
     * Service that created this message, useful for routing and debugging.
     */
    @JsonProperty("sourceService")
    private String sourceService;

    /**
     * Default constructor for serialization frameworks.
     */
    public PositionData() {
    }

    /**
     * Constructor with position and device.
     *
     * @param position Position information
     * @param device   Device information
     */
    public PositionData(Position position, Device device) {
        this.position = position;
        this.device = device;
        this.timestamp = System.currentTimeMillis();
    }

    /**
     * Constructor with position, device, and OpenTelemetry context.
     *
     * @param position Position information
     * @param device   Device information
     * @param context  OpenTelemetry context for distributed tracing
     */
    public PositionData(Position position, Device device, Context context) {
        this(position, device);
        if (context != null) {
            SpanContext spanContext = io.opentelemetry.api.trace.Span.fromContext(context).getSpanContext();
            if (spanContext.isValid()) {
                this.traceId = spanContext.getTraceId();
                this.spanId = spanContext.getSpanId();
                this.traceFlags = spanContext.getTraceFlags().asByte();
                this.traceState = spanContext.getTraceState().asString();
            }
        }
    }

    public Position getPosition() {
        return position;
    }

    public void setPosition(Position position) {
        this.position = position;
    }

    public Device getDevice() {
        return device;
    }

    public void setDevice(Device device) {
        this.device = device;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    public byte getTraceFlags() {
        return traceFlags;
    }

    public void setTraceFlags(byte traceFlags) {
        this.traceFlags = traceFlags;
    }

    public String getTraceState() {
        return traceState;
    }

    public void setTraceState(String traceState) {
        this.traceState = traceState;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    /**
     * Add a metadata entry.
     *
     * @param key   Metadata key
     * @param value Metadata value
     * @return this instance for method chaining
     */
    public PositionData addMetadata(String key, String value) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(key, value);
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSourceService() {
        return sourceService;
    }

    public void setSourceService(String sourceService) {
        this.sourceService = sourceService;
    }

    /**
     * Extract OpenTelemetry context from this object.
     *
     * @return OpenTelemetry context or null if tracing information is not available
     */
    @JsonIgnore
    public Context extractOpenTelemetryContext() {
        if (traceId == null || spanId == null) {
            return null;
        }

        SpanContext spanContext = SpanContext.create(
                traceId,
                spanId,
                TraceFlags.fromByte(traceFlags),
                traceState != null ? TraceState.fromString(traceState) : TraceState.getDefault());

        return Context.current().with(io.opentelemetry.api.trace.Span.wrap(spanContext));
    }

    /**
     * Injects OpenTelemetry context into this object.
     *
     * @param context OpenTelemetry context
     * @return this instance for method chaining
     */
    public PositionData injectOpenTelemetryContext(Context context) {
        if (context != null) {
            SpanContext spanContext = io.opentelemetry.api.trace.Span.fromContext(context).getSpanContext();
            if (spanContext.isValid()) {
                this.traceId = spanContext.getTraceId();
                this.spanId = spanContext.getSpanId();
                this.traceFlags = spanContext.getTraceFlags().asByte();
                this.traceState = spanContext.getTraceState().asString();
            }
        }
        return this;
    }

    /**
     * Helper class for injecting OpenTelemetry context into PositionData.
     */
    public static class PositionDataTextMapSetter implements TextMapPropagator.Setter<PositionData> {
        @Override
        public void set(PositionData carrier, String key, String value) {
            carrier.addMetadata(key, value);
        }
    }

    /**
     * Helper class for extracting OpenTelemetry context from PositionData.
     */
    public static class PositionDataTextMapGetter implements TextMapPropagator.Getter<PositionData> {
        @Override
        public String get(PositionData carrier, String key) {
            if (carrier.getMetadata() != null) {
                return carrier.getMetadata().get(key);
            }
            return null;
        }

        @Override
        public Iterable<String> keys(PositionData carrier) {
            if (carrier.getMetadata() != null) {
                return carrier.getMetadata().keySet();
            }
            return java.util.Collections.emptyList();
        }
    }

    /**
     * Convert this object to a Protocol Buffer message.
     * This method should be implemented when the Protocol Buffer schema is defined.
     *
     * @return Protocol Buffer message bytes
     */
    @JsonIgnore
    public byte[] toProtobuf() {
        // This would be implemented using the generated Protocol Buffer classes
        // For example:
        // return PositionDataProto.PositionData.newBuilder()
        //     .setSchemaVersion(schemaVersion)
        //     .setPosition(position.toProtobuf())
        //     .setDevice(device.toProtobuf())
        //     .setTraceId(traceId)
        //     .setSpanId(spanId)
        //     .setTraceFlags(traceFlags)
        //     .setTraceState(traceState)
        //     .putAllMetadata(metadata)
        //     .setTimestamp(timestamp)
        //     .setSourceService(sourceService)
        //     .build()
        //     .toByteArray();
        throw new UnsupportedOperationException("Protocol Buffer serialization not implemented yet");
    }

    /**
     * Create a PositionData object from a Protocol Buffer message.
     * This method should be implemented when the Protocol Buffer schema is defined.
     *
     * @param bytes Protocol Buffer message bytes
     * @return PositionData object
     */
    public static PositionData fromProtobuf(byte[] bytes) {
        // This would be implemented using the generated Protocol Buffer classes
        // For example:
        // try {
        //     PositionDataProto.PositionData proto = PositionDataProto.PositionData.parseFrom(bytes);
        //     PositionData data = new PositionData();
        //     data.setSchemaVersion(proto.getSchemaVersion());
        //     data.setPosition(Position.fromProtobuf(proto.getPosition()));
        //     data.setDevice(Device.fromProtobuf(proto.getDevice()));
        //     data.setTraceId(proto.getTraceId());
        //     data.setSpanId(proto.getSpanId());
        //     data.setTraceFlags(proto.getTraceFlags());
        //     data.setTraceState(proto.getTraceState());
        //     data.setMetadata(new HashMap<>(proto.getMetadataMap()));
        //     data.setTimestamp(proto.getTimestamp());
        //     data.setSourceService(proto.getSourceService());
        //     return data;
        // } catch (Exception e) {
        //     throw new RuntimeException("Failed to parse Protocol Buffer message", e);
        // }
        throw new UnsupportedOperationException("Protocol Buffer deserialization not implemented yet");
    }
}