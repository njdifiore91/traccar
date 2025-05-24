package org.traccar.notification;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents a notification message that can be sent through various channels.
 * This class is designed to be serialized and transmitted via message brokers.
 */
@JsonPropertyOrder({
    "id", 
    "correlationId", 
    "timestamp", 
    "priority", 
    "subject", 
    "body", 
    "userId", 
    "deviceId", 
    "type", 
    "attributes"
})
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class NotificationMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("id")
    private String id;

    @JsonProperty("correlationId")
    private String correlationId;

    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant timestamp;

    @JsonProperty("priority")
    private Priority priority;

    @JsonProperty("subject")
    @NotBlank
    private String subject;

    @JsonProperty("body")
    @NotBlank
    private String body;

    @JsonProperty("userId")
    private Long userId;

    @JsonProperty("deviceId")
    private Long deviceId;

    @JsonProperty("type")
    @NotNull
    private String type;

    @JsonProperty("attributes")
    private Map<String, Object> attributes;

    /**
     * Priority levels for notification messages.
     */
    public enum Priority {
        HIGH,
        MEDIUM,
        LOW
    }

    /**
     * Default constructor for serialization frameworks.
     */
    public NotificationMessage() {
        this.id = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
        this.priority = Priority.MEDIUM;
        this.attributes = new HashMap<>();
    }

    /**
     * Creates a new notification message with the specified parameters.
     *
     * @param subject   The notification subject
     * @param body      The notification body
     * @param userId    The user ID to whom the notification is addressed
     * @param deviceId  The device ID associated with the notification
     * @param type      The notification type
     */
    public NotificationMessage(
            String subject,
            String body,
            Long userId,
            Long deviceId,
            String type) {
        this();
        this.subject = subject;
        this.body = body;
        this.userId = userId;
        this.deviceId = deviceId;
        this.type = type;
    }

    /**
     * Gets the notification message ID.
     *
     * @return The notification ID
     */
    public String getId() {
        return id;
    }

    /**
     * Sets the notification message ID.
     *
     * @param id The notification ID
     */
    public void setId(String id) {
        this.id = id;
    }

    /**
     * Gets the correlation ID for distributed tracing.
     *
     * @return The correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }

    /**
     * Sets the correlation ID for distributed tracing.
     *
     * @param correlationId The correlation ID
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    /**
     * Gets the timestamp when the notification was created.
     *
     * @return The timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * Sets the timestamp when the notification was created.
     *
     * @param timestamp The timestamp
     */
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Gets the notification priority.
     *
     * @return The priority
     */
    public Priority getPriority() {
        return priority;
    }

    /**
     * Sets the notification priority.
     *
     * @param priority The priority
     */
    public void setPriority(Priority priority) {
        this.priority = priority;
    }

    /**
     * Gets the notification subject.
     *
     * @return The subject
     */
    public String getSubject() {
        return subject;
    }

    /**
     * Sets the notification subject.
     *
     * @param subject The subject
     */
    public void setSubject(String subject) {
        this.subject = subject;
    }

    /**
     * Gets the notification body.
     *
     * @return The body
     */
    public String getBody() {
        return body;
    }

    /**
     * Sets the notification body.
     *
     * @param body The body
     */
    public void setBody(String body) {
        this.body = body;
    }

    /**
     * Gets the user ID to whom the notification is addressed.
     *
     * @return The user ID
     */
    public Long getUserId() {
        return userId;
    }

    /**
     * Sets the user ID to whom the notification is addressed.
     *
     * @param userId The user ID
     */
    public void setUserId(Long userId) {
        this.userId = userId;
    }

    /**
     * Gets the device ID associated with the notification.
     *
     * @return The device ID
     */
    public Long getDeviceId() {
        return deviceId;
    }

    /**
     * Sets the device ID associated with the notification.
     *
     * @param deviceId The device ID
     */
    public void setDeviceId(Long deviceId) {
        this.deviceId = deviceId;
    }

    /**
     * Gets the notification type.
     *
     * @return The type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the notification type.
     *
     * @param type The type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets the additional attributes associated with the notification.
     *
     * @return The attributes map
     */
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    /**
     * Sets the additional attributes associated with the notification.
     *
     * @param attributes The attributes map
     */
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    /**
     * Adds an attribute to the notification.
     *
     * @param key   The attribute key
     * @param value The attribute value
     * @return This notification message for method chaining
     */
    public NotificationMessage addAttribute(String key, Object value) {
        if (this.attributes == null) {
            this.attributes = new HashMap<>();
        }
        this.attributes.put(key, value);
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NotificationMessage that = (NotificationMessage) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(correlationId, that.correlationId) &&
                Objects.equals(subject, that.subject) &&
                Objects.equals(body, that.body) &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(deviceId, that.deviceId) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, correlationId, subject, body, userId, deviceId, type);
    }

    @Override
    public String toString() {
        return "NotificationMessage{" +
                "id='" + id + '\'' +
                ", correlationId='" + correlationId + '\'' +
                ", timestamp=" + timestamp +
                ", priority=" + priority +
                ", subject='" + subject + '\'' +
                ", body='" + body + '\'' +
                ", userId=" + userId +
                ", deviceId=" + deviceId +
                ", type='" + type + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    /**
     * Builder for creating NotificationMessage instances.
     */
    public static class Builder {
        private final NotificationMessage message;

        /**
         * Creates a new builder instance.
         */
        public Builder() {
            message = new NotificationMessage();
        }

        /**
         * Sets the notification subject.
         *
         * @param subject The subject
         * @return This builder for method chaining
         */
        public Builder withSubject(String subject) {
            message.setSubject(subject);
            return this;
        }

        /**
         * Sets the notification body.
         *
         * @param body The body
         * @return This builder for method chaining
         */
        public Builder withBody(String body) {
            message.setBody(body);
            return this;
        }

        /**
         * Sets the user ID to whom the notification is addressed.
         *
         * @param userId The user ID
         * @return This builder for method chaining
         */
        public Builder withUserId(Long userId) {
            message.setUserId(userId);
            return this;
        }

        /**
         * Sets the device ID associated with the notification.
         *
         * @param deviceId The device ID
         * @return This builder for method chaining
         */
        public Builder withDeviceId(Long deviceId) {
            message.setDeviceId(deviceId);
            return this;
        }

        /**
         * Sets the notification type.
         *
         * @param type The type
         * @return This builder for method chaining
         */
        public Builder withType(String type) {
            message.setType(type);
            return this;
        }

        /**
         * Sets the notification priority.
         *
         * @param priority The priority
         * @return This builder for method chaining
         */
        public Builder withPriority(Priority priority) {
            message.setPriority(priority);
            return this;
        }

        /**
         * Sets the correlation ID for distributed tracing.
         *
         * @param correlationId The correlation ID
         * @return This builder for method chaining
         */
        public Builder withCorrelationId(String correlationId) {
            message.setCorrelationId(correlationId);
            return this;
        }

        /**
         * Adds an attribute to the notification.
         *
         * @param key   The attribute key
         * @param value The attribute value
         * @return This builder for method chaining
         */
        public Builder withAttribute(String key, Object value) {
            message.addAttribute(key, value);
            return this;
        }

        /**
         * Sets all attributes for the notification.
         *
         * @param attributes The attributes map
         * @return This builder for method chaining
         */
        public Builder withAttributes(Map<String, Object> attributes) {
            message.setAttributes(attributes);
            return this;
        }

        /**
         * Builds the notification message.
         *
         * @return The built notification message
         */
        public NotificationMessage build() {
            return message;
        }
    }
}