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
package org.traccar.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.traccar.storage.StorageName;

import java.util.Map;

/**
 * Outbox message entity for implementing the Transaction Outbox pattern.
 * This class represents a message that needs to be published to a message broker.
 * It is stored in the database as part of a transaction to ensure reliable message delivery.
 */
@StorageName("tc_outbox_messages")
@JsonIgnoreProperties(ignoreUnknown = true)
public class OutboxMessage extends BaseModel {

    /**
     * Status of the outbox message
     */
    public enum Status {
        /**
         * Message is pending delivery to the message broker
         */
        PENDING,
        
        /**
         * Message has been successfully processed and delivered
         */
        PROCESSED,
        
        /**
         * Message delivery has failed
         */
        FAILED
    }
    
    private String id;
    private String topic;
    private Object payload;
    private Map<String, String> headers;
    private long createdAt;
    private long processedAt;
    private long failedAt;
    private String status;
    private String errorMessage;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public Object getPayload() {
        return payload;
    }

    public void setPayload(Object payload) {
        this.payload = payload;
    }

    public Map<String, String> getHeaders() {
        return headers;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public long getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(long processedAt) {
        this.processedAt = processedAt;
    }

    public long getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(long failedAt) {
        this.failedAt = failedAt;
    }

    public Status getStatus() {
        return status != null ? Status.valueOf(status) : null;
    }

    public void setStatus(Status status) {
        this.status = status != null ? status.name() : null;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}