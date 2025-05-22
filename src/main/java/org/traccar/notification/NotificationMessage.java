/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.notification;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a notification message that can be sent through various channels.
 * This class is designed to be serialized and deserialized for message broker compatibility
 * in a distributed microservices architecture.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
public class NotificationMessage implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /**
     * Current schema version for backward compatibility
     */
    private static final int CURRENT_VERSION = 1;

    @JsonProperty("subject")
    private final String subject;
    
    @JsonProperty("body")
    private final String body;
    
    /**
     * Unique identifier for distributed tracing across services
     */
    @JsonProperty("correlationId")
    private final String correlationId;
    
    /**
     * Schema version for backward compatibility
     */
    @JsonProperty("version")
    private final int version;
    
    /**
     * Target service or component for message routing
     */
    @JsonProperty("target")
    private final String target;
    
    /**
     * Message type for routing and processing
     */
    @JsonProperty("messageType")
    private final String messageType;
    
    /**
     * Additional metadata for message routing and processing
     */
    @JsonProperty("metadata")
    private final Map<String, String> metadata;

    /**
     * Creates a new notification message with minimal information.
     *
     * @param subject The notification subject
     * @param body The notification body
     */
    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public NotificationMessage(
            @JsonProperty("subject") String subject, 
            @JsonProperty("body") String body) {
        this(subject, body, UUID.randomUUID().toString(), null, null);
    }
    
    /**
     * Creates a new notification message with routing information.
     *
     * @param subject The notification subject
     * @param body The notification body
     * @param correlationId Unique identifier for tracing (if null, a new UUID will be generated)
     * @param target Target service or component
     * @param messageType Type of message for routing
     */
    public NotificationMessage(
            String subject, 
            String body, 
            String correlationId,
            String target,
            String messageType) {
        this.subject = subject;
        this.body = body;
        this.correlationId = correlationId != null ? correlationId : UUID.randomUUID().toString();
        this.version = CURRENT_VERSION;
        this.target = target;
        this.messageType = messageType;
        this.metadata = new HashMap<>();
    }
    
    /**
     * Full constructor with all fields.
     *
     * @param subject The notification subject
     * @param body The notification body
     * @param correlationId Unique identifier for tracing
     * @param version Schema version
     * @param target Target service or component
     * @param messageType Type of message for routing
     * @param metadata Additional metadata
     */
    public NotificationMessage(
            String subject, 
            String body, 
            String correlationId,
            int version,
            String target,
            String messageType,
            Map<String, String> metadata) {
        this.subject = subject;
        this.body = body;
        this.correlationId = correlationId;
        this.version = version;
        this.target = target;
        this.messageType = messageType;
        this.metadata = metadata != null ? new HashMap<>(metadata) : new HashMap<>();
    }

    /**
     * Returns the notification subject.
     *
     * @return The subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Returns the notification body.
     *
     * @return The body
     */
    public String getBody() {
        return body;
    }
    
    /**
     * Returns the correlation ID for distributed tracing.
     *
     * @return The correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }
    
    /**
     * Returns the schema version.
     *
     * @return The version
     */
    public int getVersion() {
        return version;
    }
    
    /**
     * Returns the target service or component.
     *
     * @return The target
     */
    public String getTarget() {
        return target;
    }
    
    /**
     * Returns the message type.
     *
     * @return The message type
     */
    public String getMessageType() {
        return messageType;
    }
    
    /**
     * Returns the metadata map.
     *
     * @return The metadata
     */
    public Map<String, String> getMetadata() {
        return new HashMap<>(metadata);
    }
    
    /**
     * Adds a metadata entry.
     *
     * @param key The metadata key
     * @param value The metadata value
     * @return A new NotificationMessage with the added metadata
     */
    public NotificationMessage withMetadata(String key, String value) {
        Map<String, String> newMetadata = new HashMap<>(this.metadata);
        newMetadata.put(key, value);
        return new NotificationMessage(
                this.subject,
                this.body,
                this.correlationId,
                this.version,
                this.target,
                this.messageType,
                newMetadata);
    }
    
    /**
     * Creates a new NotificationMessage with the specified target.
     *
     * @param target The target service or component
     * @return A new NotificationMessage with the updated target
     */
    public NotificationMessage withTarget(String target) {
        return new NotificationMessage(
                this.subject,
                this.body,
                this.correlationId,
                this.version,
                target,
                this.messageType,
                this.metadata);
    }
    
    /**
     * Creates a new NotificationMessage with the specified message type.
     *
     * @param messageType The message type
     * @return A new NotificationMessage with the updated message type
     */
    public NotificationMessage withMessageType(String messageType) {
        return new NotificationMessage(
                this.subject,
                this.body,
                this.correlationId,
                this.version,
                this.target,
                messageType,
                this.metadata);
    }
    
    /**
     * Creates a builder for NotificationMessage.
     *
     * @return A new builder
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder class for NotificationMessage.
     */
    public static class Builder {
        private String subject;
        private String body;
        private String correlationId;
        private String target;
        private String messageType;
        private Map<String, String> metadata = new HashMap<>();
        
        public Builder subject(String subject) {
            this.subject = subject;
            return this;
        }
        
        public Builder body(String body) {
            this.body = body;
            return this;
        }
        
        public Builder correlationId(String correlationId) {
            this.correlationId = correlationId;
            return this;
        }
        
        public Builder target(String target) {
            this.target = target;
            return this;
        }
        
        public Builder messageType(String messageType) {
            this.messageType = messageType;
            return this;
        }
        
        public Builder metadata(String key, String value) {
            this.metadata.put(key, value);
            return this;
        }
        
        public Builder metadata(Map<String, String> metadata) {
            if (metadata != null) {
                this.metadata.putAll(metadata);
            }
            return this;
        }
        
        public NotificationMessage build() {
            return new NotificationMessage(
                    subject,
                    body,
                    correlationId,
                    CURRENT_VERSION,
                    target,
                    messageType,
                    metadata);
        }
    }
}