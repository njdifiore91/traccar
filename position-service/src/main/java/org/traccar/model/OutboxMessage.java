/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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

import java.util.Date;

/**
 * Represents an outbox message for the transaction outbox pattern.
 * 
 * This class is used to store messages that need to be published to the message broker
 * as part of a database transaction. The outbox pattern ensures that messages are reliably
 * delivered to the broker even if the service fails after committing the transaction but
 * before publishing the message.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class OutboxMessage extends BaseModel {

    /**
     * Status of an outbox message.
     */
    public enum Status {
        /**
         * Message is pending and needs to be published to the broker.
         */
        PENDING,
        
        /**
         * Message has been successfully published to the broker.
         */
        PROCESSED,
        
        /**
         * Message failed to be published to the broker after maximum retries.
         */
        FAILED
    }

    private String topic;
    private String payload;
    private String correlationId;
    private Status status;
    private int retryCount;
    private Date createdAt;
    private Date processedAt;
    private Date failedAt;

    /**
     * Get the topic to publish the message to.
     *
     * @return Topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Set the topic to publish the message to.
     *
     * @param topic Topic name
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * Get the message payload as a JSON string.
     *
     * @return Message payload
     */
    public String getPayload() {
        return payload;
    }

    /**
     * Set the message payload as a JSON string.
     *
     * @param payload Message payload
     */
    public void setPayload(String payload) {
        this.payload = payload;
    }

    /**
     * Get the correlation ID for distributed tracing.
     *
     * @return Correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Set the correlation ID for distributed tracing.
     *
     * @param correlationId Correlation ID
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Get the status of the outbox message.
     *
     * @return Message status
     */
    public Status getStatus() {
        return status;
    }

    /**
     * Set the status of the outbox message.
     *
     * @param status Message status
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Get the number of retry attempts for publishing the message.
     *
     * @return Retry count
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Set the number of retry attempts for publishing the message.
     *
     * @param retryCount Retry count
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Get the date and time when the outbox message was created.
     *
     * @return Creation date
     */
    public Date getCreatedAt() {
        return createdAt;
    }

    /**
     * Set the date and time when the outbox message was created.
     *
     * @param createdAt Creation date
     */
    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Get the date and time when the outbox message was successfully processed.
     *
     * @return Processing date
     */
    public Date getProcessedAt() {
        return processedAt;
    }

    /**
     * Set the date and time when the outbox message was successfully processed.
     *
     * @param processedAt Processing date
     */
    public void setProcessedAt(Date processedAt) {
        this.processedAt = processedAt;
    }

    /**
     * Get the date and time when the outbox message failed to be processed after maximum retries.
     *
     * @return Failure date
     */
    public Date getFailedAt() {
        return failedAt;
    }

    /**
     * Set the date and time when the outbox message failed to be processed after maximum retries.
     *
     * @param failedAt Failure date
     */
    public void setFailedAt(Date failedAt) {
        this.failedAt = failedAt;
    }
}