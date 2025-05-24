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
package org.traccar.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Date;

/**
 * Represents the delivery status of a notification across various channels.
 * This model tracks the success, failure, and attempt details for notification delivery.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DeliveryStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private long notificationId;
    private String channel;
    private String recipient;
    private String status;
    private Date timestamp;
    private Date sentTime;
    private Date deliveredTime;
    private String errorMessage;
    private int attemptCount;
    private String correlationId;

    /**
     * Status constants for delivery tracking
     */
    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_SENDING = "SENDING";
    public static final String STATUS_DELIVERED = "DELIVERED";
    public static final String STATUS_FAILED = "FAILED";
    public static final String STATUS_RETRYING = "RETRYING";
    public static final String STATUS_CANCELLED = "CANCELLED";

    public DeliveryStatus() {
    }

    /**
     * Constructor with essential fields
     *
     * @param notificationId The ID of the notification being delivered
     * @param channel The delivery channel (email, sms, push, etc.)
     * @param recipient The recipient address
     */
    public DeliveryStatus(long notificationId, String channel, String recipient) {
        this.notificationId = notificationId;
        this.channel = channel;
        this.recipient = recipient;
        this.status = STATUS_PENDING;
        this.timestamp = new Date();
        this.attemptCount = 0;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getNotificationId() {
        return notificationId;
    }

    public void setNotificationId(long notificationId) {
        this.notificationId = notificationId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getRecipient() {
        return recipient;
    }

    public void setRecipient(String recipient) {
        this.recipient = recipient;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public Date getSentTime() {
        return sentTime;
    }

    public void setSentTime(Date sentTime) {
        this.sentTime = sentTime;
    }

    public Date getDeliveredTime() {
        return deliveredTime;
    }

    public void setDeliveredTime(Date deliveredTime) {
        this.deliveredTime = deliveredTime;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public void setAttemptCount(int attemptCount) {
        this.attemptCount = attemptCount;
    }

    public void incrementAttemptCount() {
        this.attemptCount++;
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Marks this delivery as sending
     * 
     * @return this instance for method chaining
     */
    public DeliveryStatus markSending() {
        this.status = STATUS_SENDING;
        this.sentTime = new Date();
        this.incrementAttemptCount();
        return this;
    }

    /**
     * Marks this delivery as delivered successfully
     * 
     * @return this instance for method chaining
     */
    public DeliveryStatus markDelivered() {
        this.status = STATUS_DELIVERED;
        this.deliveredTime = new Date();
        return this;
    }

    /**
     * Marks this delivery as failed
     * 
     * @param errorMessage The error message describing the failure
     * @return this instance for method chaining
     */
    public DeliveryStatus markFailed(String errorMessage) {
        this.status = STATUS_FAILED;
        this.errorMessage = errorMessage;
        return this;
    }

    /**
     * Marks this delivery for retry
     * 
     * @param errorMessage The error message describing the failure that triggered the retry
     * @return this instance for method chaining
     */
    public DeliveryStatus markRetrying(String errorMessage) {
        this.status = STATUS_RETRYING;
        this.errorMessage = errorMessage;
        return this;
    }

    /**
     * Marks this delivery as cancelled
     * 
     * @param reason The reason for cancellation
     * @return this instance for method chaining
     */
    public DeliveryStatus markCancelled(String reason) {
        this.status = STATUS_CANCELLED;
        this.errorMessage = reason;
        return this;
    }
}