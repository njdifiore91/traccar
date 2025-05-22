/*
 * Copyright 2013 - 2016 Anton Tananaev (anton@traccar.org)
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
package org.traccar.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Base message class for inter-service communication.
 * Provides support for message broker serialization, routing metadata,
 * and distributed tracing context propagation.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Message extends ExtendedModel implements MessageEnvelope {

    private long deviceId;

    public long getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(long deviceId) {
        this.deviceId = deviceId;
    }

    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    // Message routing metadata fields
    private String topic;
    private Integer partition;
    private String routingKey;
    private String exchange;
    private Map<String, String> headers = new HashMap<>();

    // Tracing context fields
    private String traceId;
    private String spanId;
    private String parentSpanId;
    private Map<String, String> baggage = new HashMap<>();

    // Message envelope fields
    private String messageId = UUID.randomUUID().toString();
    private long timestamp = System.currentTimeMillis();
    private Integer ttl;

    /**
     * Default constructor for serialization frameworks.
     */
    public Message() {
    }

    /**
     * Constructor with device ID and type.
     *
     * @param deviceId The device ID
     * @param type The message type
     */
    public Message(long deviceId, String type) {
        this.deviceId = deviceId;
        this.type = type;
    }

    // Message routing metadata getters and setters

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public Integer getPartition() {
        return partition;
    }

    @Override
    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    @Override
    public String getRoutingKey() {
        return routingKey;
    }

    @Override
    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    @Override
    public String getExchange() {
        return exchange;
    }

    @Override
    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    @Override
    public Map<String, String> getHeaders() {
        return headers;
    }

    @Override
    public void setHeaders(Map<String, String> headers) {
        this.headers = headers != null ? headers : new HashMap<>();
    }

    @Override
    public void addHeader(String key, String value) {
        if (headers == null) {
            headers = new HashMap<>();
        }
        headers.put(key, value);
    }

    // Tracing context getters and setters

    @Override
    public String getTraceId() {
        return traceId;
    }

    @Override
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    @Override
    public String getSpanId() {
        return spanId;
    }

    @Override
    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    @Override
    public String getParentSpanId() {
        return parentSpanId;
    }

    @Override
    public void setParentSpanId(String parentSpanId) {
        this.parentSpanId = parentSpanId;
    }

    @Override
    public Map<String, String> getBaggage() {
        return baggage;
    }

    @Override
    public void setBaggage(Map<String, String> baggage) {
        this.baggage = baggage != null ? baggage : new HashMap<>();
    }

    @Override
    public void addBaggageItem(String key, String value) {
        if (baggage == null) {
            baggage = new HashMap<>();
        }
        baggage.put(key, value);
    }

    // Message envelope getters and setters

    @Override
    @JsonProperty("id")
    public String getMessageId() {
        return messageId;
    }

    @Override
    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public Integer getTtl() {
        return ttl;
    }

    @Override
    public void setTtl(Integer ttl) {
        this.ttl = ttl;
    }

    /**
     * Creates a copy of this message with a new message ID and timestamp.
     * Useful for message forwarding and redelivery scenarios.
     *
     * @return A new message instance with copied content but new ID and timestamp
     */
    @JsonIgnore
    public Message copy() {
        Message copy = new Message(deviceId, type);
        copy.setAttributes(getAttributes());
        
        // Copy routing metadata
        copy.setTopic(topic);
        copy.setPartition(partition);
        copy.setRoutingKey(routingKey);
        copy.setExchange(exchange);
        copy.setHeaders(new HashMap<>(headers));
        
        // Copy tracing context
        copy.setTraceId(traceId);
        copy.setSpanId(spanId);
        copy.setParentSpanId(parentSpanId);
        copy.setBaggage(new HashMap<>(baggage));
        
        // Set new message ID and timestamp
        copy.setMessageId(UUID.randomUUID().toString());
        copy.setTimestamp(System.currentTimeMillis());
        copy.setTtl(ttl);
        
        return copy;
    }

    /**
     * Creates a response message with the same tracing context.
     *
     * @param type The response message type
     * @return A new message instance configured as a response to this message
     */
    @JsonIgnore
    public Message createResponse(String type) {
        Message response = new Message(deviceId, type);
        
        // Copy tracing context for correlation
        response.setTraceId(traceId);
        response.setParentSpanId(spanId);
        response.setBaggage(new HashMap<>(baggage));
        
        return response;
    }
}