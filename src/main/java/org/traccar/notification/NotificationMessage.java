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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a formatted notification message with content and metadata.
 * Enhanced with tracing context for distributed tracing across services.
 */
public class NotificationMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String subject;
    private final String body;
    private String correlationId;
    private Map<String, String> tracingContext;
    private Map<String, Object> metadata;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public NotificationMessage(
            @JsonProperty("subject") String subject, 
            @JsonProperty("body") String body) {
        this.subject = subject;
        this.body = body;
        this.metadata = new HashMap<>();
        this.tracingContext = new HashMap<>();
    }

    public String getSubject() {
        return subject;
    }

    public String getBody() {
        return body;
    }
    
    /**
     * Gets the correlation ID for this notification message.
     * This is typically the trace ID from OpenTelemetry for distributed tracing.
     * 
     * @return The correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Sets the correlation ID for this notification message.
     * 
     * @param correlationId The correlation ID to set
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Gets the tracing context for distributed tracing.
     * 
     * @return Map containing tracing context information
     */
    public Map<String, String> getTracingContext() {
        return tracingContext;
    }

    /**
     * Sets the tracing context for distributed tracing.
     * 
     * @param tracingContext Map containing tracing context information
     */
    public void setTracingContext(Map<String, String> tracingContext) {
        this.tracingContext = tracingContext;
    }

    /**
     * Gets additional metadata associated with this notification.
     * 
     * @return Map containing metadata
     */
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    /**
     * Sets additional metadata for this notification.
     * 
     * @param metadata Map containing metadata
     */
    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    /**
     * Adds a single metadata entry to this notification.
     * 
     * @param key Metadata key
     * @param value Metadata value
     */
    public void addMetadata(String key, Object value) {
        if (this.metadata == null) {
            this.metadata = new HashMap<>();
        }
        this.metadata.put(key, value);
    }
}