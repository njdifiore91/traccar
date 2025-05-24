/*
 * Copyright 2022 - 2025 Anton Tananaev (anton@traccar.org)
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

import java.io.Serializable;

/**
 * Exception thrown during message processing in the notification service.
 * This exception is designed to be serializable for transmission across service boundaries
 * via message brokers and includes support for correlation IDs to facilitate distributed tracing.
 */
public class MessageException extends Exception implements Serializable {

    /**
     * Serialization version UID for cross-service compatibility.
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * Correlation ID for distributed tracing across services.
     */
    private String correlationId;
    
    /**
     * Error code for categorizing the exception type.
     */
    private String errorCode;
    
    /**
     * Constructs a new MessageException with the specified detail message.
     *
     * @param message the detail message
     */
    public MessageException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new MessageException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public MessageException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new MessageException with the specified detail message and error code.
     *
     * @param message the detail message
     * @param errorCode the error code for categorizing the exception
     */
    public MessageException(String message, String errorCode) {
        super(message);
        this.errorCode = errorCode;
    }
    
    /**
     * Constructs a new MessageException with the specified detail message, error code, and cause.
     *
     * @param message the detail message
     * @param errorCode the error code for categorizing the exception
     * @param cause the cause of the exception
     */
    public MessageException(String message, String errorCode, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }
    
    /**
     * Constructs a new MessageException with the specified detail message, error code, and correlation ID.
     *
     * @param message the detail message
     * @param errorCode the error code for categorizing the exception
     * @param correlationId the correlation ID for distributed tracing
     */
    public MessageException(String message, String errorCode, String correlationId) {
        super(message);
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }
    
    /**
     * Constructs a new MessageException with the specified detail message, error code, correlation ID, and cause.
     *
     * @param message the detail message
     * @param errorCode the error code for categorizing the exception
     * @param correlationId the correlation ID for distributed tracing
     * @param cause the cause of the exception
     */
    public MessageException(String message, String errorCode, String correlationId, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }
    
    /**
     * Gets the correlation ID for distributed tracing.
     *
     * @return the correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }
    
    /**
     * Sets the correlation ID for distributed tracing.
     *
     * @param correlationId the correlation ID to set
     */
    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }
    
    /**
     * Gets the error code for categorizing the exception.
     *
     * @return the error code
     */
    public String getErrorCode() {
        return errorCode;
    }
    
    /**
     * Sets the error code for categorizing the exception.
     *
     * @param errorCode the error code to set
     */
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
}