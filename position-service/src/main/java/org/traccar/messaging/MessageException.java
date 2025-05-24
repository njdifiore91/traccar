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
package org.traccar.messaging;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown for messaging-related errors in the Position Processing Service.
 * This exception provides detailed information about the error type, context, and
 * whether the error is transient (retryable) or permanent.
 */
public class MessageException extends Exception {

    /**
     * Types of messaging errors that can occur.
     */
    public enum ErrorType {
        /**
         * Error establishing or maintaining a connection to the message broker.
         * Usually transient and can be retried.
         */
        CONNECTION(true),
        
        /**
         * Error serializing or deserializing a message.
         * Usually permanent and indicates a message format issue.
         */
        SERIALIZATION(false),
        
        /**
         * Error due to authentication failure with the message broker.
         * Usually permanent and requires configuration correction.
         */
        AUTHENTICATION(false),
        
        /**
         * Error due to authorization failure with the message broker.
         * Usually permanent and requires permission correction.
         */
        AUTHORIZATION(false),
        
        /**
         * Error due to a timeout while communicating with the message broker.
         * Usually transient and can be retried.
         */
        TIMEOUT(true),
        
        /**
         * Error due to the message broker being unavailable.
         * Usually transient and can be retried after the broker recovers.
         */
        BROKER_UNAVAILABLE(true),
        
        /**
         * Error due to an invalid message format or content.
         * Usually permanent and requires message correction.
         */
        INVALID_MESSAGE(false),
        
        /**
         * Error due to a message delivery failure.
         * May be transient or permanent depending on the cause.
         */
        DELIVERY_FAILURE(true),
        
        /**
         * Error due to a message processing failure.
         * May be transient or permanent depending on the cause.
         */
        PROCESSING_FAILURE(true),
        
        /**
         * Unknown or unspecified error.
         * Assumed to be transient by default.
         */
        UNKNOWN(true);
        
        private final boolean transient;
        
        ErrorType(boolean isTransient) {
            this.transient = isTransient;
        }
        
        /**
         * Determines if this error type is typically transient and can be retried.
         *
         * @return true if the error is typically transient, false otherwise
         */
        public boolean isTransient() {
            return transient;
        }
    }
    
    private final ErrorType errorType;
    private final boolean transient;
    private final String messageId;
    private final Instant timestamp;
    private final Map<String, Object> context;
    
    /**
     * Constructs a new MessageException with the specified detail message.
     * The error type is set to UNKNOWN.
     *
     * @param message the detail message
     */
    public MessageException(String message) {
        this(ErrorType.UNKNOWN, message, null, null, null, true);
    }
    
    /**
     * Constructs a new MessageException with the specified detail message and cause.
     * The error type is set to UNKNOWN.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public MessageException(String message, Throwable cause) {
        this(ErrorType.UNKNOWN, message, cause, null, null, true);
    }
    
    /**
     * Constructs a new MessageException with the specified error type and detail message.
     *
     * @param errorType the type of messaging error
     * @param message the detail message
     */
    public MessageException(ErrorType errorType, String message) {
        this(errorType, message, null, null, null, errorType.isTransient());
    }
    
    /**
     * Constructs a new MessageException with the specified error type, detail message, and cause.
     *
     * @param errorType the type of messaging error
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public MessageException(ErrorType errorType, String message, Throwable cause) {
        this(errorType, message, cause, null, null, errorType.isTransient());
    }
    
    /**
     * Constructs a new MessageException with the specified error type, detail message,
     * cause, and message ID.
     *
     * @param errorType the type of messaging error
     * @param message the detail message
     * @param cause the cause of the exception
     * @param messageId the ID of the message that caused the error
     */
    public MessageException(ErrorType errorType, String message, Throwable cause, String messageId) {
        this(errorType, message, cause, messageId, null, errorType.isTransient());
    }
    
    /**
     * Constructs a new MessageException with the specified error type, detail message,
     * cause, message ID, and context.
     *
     * @param errorType the type of messaging error
     * @param message the detail message
     * @param cause the cause of the exception
     * @param messageId the ID of the message that caused the error
     * @param context additional context information about the error
     */
    public MessageException(ErrorType errorType, String message, Throwable cause, String messageId, Map<String, Object> context) {
        this(errorType, message, cause, messageId, context, errorType.isTransient());
    }
    
    /**
     * Constructs a new MessageException with the specified error type, detail message,
     * cause, message ID, context, and transient flag.
     *
     * @param errorType the type of messaging error
     * @param message the detail message
     * @param cause the cause of the exception
     * @param messageId the ID of the message that caused the error
     * @param context additional context information about the error
     * @param isTransient whether the error is transient and can be retried
     */
    public MessageException(ErrorType errorType, String message, Throwable cause, String messageId, Map<String, Object> context, boolean isTransient) {
        super(message, cause);
        this.errorType = errorType;
        this.transient = isTransient;
        this.messageId = messageId;
        this.timestamp = Instant.now();
        this.context = context != null ? new HashMap<>(context) : new HashMap<>();
    }
    
    /**
     * Gets the type of messaging error.
     *
     * @return the error type
     */
    public ErrorType getErrorType() {
        return errorType;
    }
    
    /**
     * Determines if this error is transient and can be retried.
     *
     * @return true if the error is transient, false otherwise
     */
    public boolean isTransient() {
        return transient;
    }
    
    /**
     * Gets the ID of the message that caused the error, if available.
     *
     * @return the message ID, or null if not available
     */
    public String getMessageId() {
        return messageId;
    }
    
    /**
     * Gets the timestamp when the exception was created.
     *
     * @return the timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
    }
    
    /**
     * Gets the additional context information about the error.
     *
     * @return an unmodifiable map of context information
     */
    public Map<String, Object> getContext() {
        return Collections.unmodifiableMap(context);
    }
    
    /**
     * Adds a context value to the exception.
     *
     * @param key the context key
     * @param value the context value
     * @return this exception instance for method chaining
     */
    public MessageException addContext(String key, Object value) {
        context.put(key, value);
        return this;
    }
    
    /**
     * Gets a formatted error message that includes the error type, message ID,
     * timestamp, and context information.
     *
     * @return a formatted error message
     */
    public String getFormattedMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append("[")
          .append(errorType)
          .append("] ")
          .append(getMessage());
        
        if (messageId != null) {
            sb.append(" (Message ID: ")
              .append(messageId)
              .append(")");
        }
        
        sb.append(" [")
          .append(timestamp)
          .append("]");
        
        if (!context.isEmpty()) {
            sb.append(" Context: ")
              .append(context);
        }
        
        return sb.toString();
    }
    
    /**
     * Determines if this exception should be retried based on its error type and
     * the number of previous retry attempts.
     *
     * @param retryAttempts the number of previous retry attempts
     * @param maxRetries the maximum number of retries allowed
     * @return true if the exception should be retried, false otherwise
     */
    public boolean shouldRetry(int retryAttempts, int maxRetries) {
        return isTransient() && retryAttempts < maxRetries;
    }
}