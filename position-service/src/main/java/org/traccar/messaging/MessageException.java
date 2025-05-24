/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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

/**
 * Custom exception class for messaging-related errors in the Position Processing Service,
 * providing specific error types and context for message processing failures.
 */
public class MessageException extends Exception {

    /**
     * Enum defining different types of messaging errors.
     */
    public enum ErrorType {
        /**
         * Error connecting to the message broker.
         */
        CONNECTION,
        
        /**
         * Error serializing or deserializing messages.
         */
        SERIALIZATION,
        
        /**
         * Error during message transmission.
         */
        TRANSMISSION,
        
        /**
         * Error with message acknowledgment.
         */
        ACKNOWLEDGMENT,
        
        /**
         * Timeout waiting for a response.
         */
        TIMEOUT,
        
        /**
         * Unknown or unspecified error.
         */
        UNKNOWN
    }

    private final ErrorType errorType;
    private final boolean retryable;

    /**
     * Constructs a new MessageException with the specified detail message.
     *
     * @param message The detail message
     */
    public MessageException(String message) {
        super(message);
        this.errorType = ErrorType.UNKNOWN;
        this.retryable = false;
    }

    /**
     * Constructs a new MessageException with the specified detail message and cause.
     *
     * @param message The detail message
     * @param cause The cause of the exception
     */
    public MessageException(String message, Throwable cause) {
        super(message, cause);
        this.errorType = determineErrorType(cause);
        this.retryable = determineRetryable(this.errorType, cause);
    }

    /**
     * Constructs a new MessageException with the specified detail message and error type.
     *
     * @param message The detail message
     * @param errorType The type of messaging error
     */
    public MessageException(String message, ErrorType errorType) {
        super(message);
        this.errorType = errorType;
        this.retryable = determineRetryable(errorType, null);
    }

    /**
     * Constructs a new MessageException with the specified detail message, cause, and error type.
     *
     * @param message The detail message
     * @param cause The cause of the exception
     * @param errorType The type of messaging error
     */
    public MessageException(String message, Throwable cause, ErrorType errorType) {
        super(message, cause);
        this.errorType = errorType;
        this.retryable = determineRetryable(errorType, cause);
    }

    /**
     * Constructs a new MessageException with the specified detail message, error type, and retryable flag.
     *
     * @param message The detail message
     * @param errorType The type of messaging error
     * @param retryable Whether the operation can be retried
     */
    public MessageException(String message, ErrorType errorType, boolean retryable) {
        super(message);
        this.errorType = errorType;
        this.retryable = retryable;
    }

    /**
     * Gets the type of messaging error.
     *
     * @return The error type
     */
    public ErrorType getErrorType() {
        return errorType;
    }

    /**
     * Indicates whether the operation can be retried.
     *
     * @return true if the operation can be retried, false otherwise
     */
    public boolean isRetryable() {
        return retryable;
    }

    /**
     * Determines the error type based on the cause of the exception.
     *
     * @param cause The cause of the exception
     * @return The determined error type
     */
    private ErrorType determineErrorType(Throwable cause) {
        if (cause == null) {
            return ErrorType.UNKNOWN;
        }

        String className = cause.getClass().getName();

        if (className.contains("ConnectException") || className.contains("ConnectionException")) {
            return ErrorType.CONNECTION;
        } else if (className.contains("SerializationException") || className.contains("InvalidProtocolBufferException")) {
            return ErrorType.SERIALIZATION;
        } else if (className.contains("TimeoutException")) {
            return ErrorType.TIMEOUT;
        } else if (className.contains("SendException") || className.contains("ProducerFencedException")) {
            return ErrorType.TRANSMISSION;
        } else if (className.contains("AcknowledgmentException")) {
            return ErrorType.ACKNOWLEDGMENT;
        }

        return ErrorType.UNKNOWN;
    }

    /**
     * Determines whether the operation can be retried based on the error type and cause.
     *
     * @param errorType The type of messaging error
     * @param cause The cause of the exception
     * @return true if the operation can be retried, false otherwise
     */
    private boolean determineRetryable(ErrorType errorType, Throwable cause) {
        switch (errorType) {
            case CONNECTION:
            case TIMEOUT:
            case TRANSMISSION:
                return true;
            case SERIALIZATION:
                return false;
            case ACKNOWLEDGMENT:
                return cause != null && !cause.getMessage().contains("permanent");
            case UNKNOWN:
            default:
                return false;
        }
    }
}