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
package org.traccar.geocoder;

/**
 * Exception thrown when geocoding operations fail.
 * This exception is used to distinguish geocoding-specific errors from other exceptions.
 */
public class GeocoderException extends RuntimeException {

    private final String errorCode;
    private final boolean retryable;

    /**
     * Creates a new GeocoderException with a message.
     *
     * @param message Error message
     */
    public GeocoderException(String message) {
        this(message, null, null, false);
    }

    /**
     * Creates a new GeocoderException with a message and cause.
     *
     * @param message Error message
     * @param cause Underlying cause
     */
    public GeocoderException(String message, Throwable cause) {
        this(message, cause, null, false);
    }

    /**
     * Creates a new GeocoderException with a message, cause, and error code.
     *
     * @param message Error message
     * @param cause Underlying cause
     * @param errorCode Error code from the geocoding service
     */
    public GeocoderException(String message, Throwable cause, String errorCode) {
        this(message, cause, errorCode, false);
    }

    /**
     * Creates a new GeocoderException with a message, cause, error code, and retryable flag.
     *
     * @param message Error message
     * @param cause Underlying cause
     * @param errorCode Error code from the geocoding service
     * @param retryable Whether the operation can be retried
     */
    public GeocoderException(String message, Throwable cause, String errorCode, boolean retryable) {
        super(message, cause);
        this.errorCode = errorCode;
        this.retryable = retryable;
    }

    /**
     * Gets the error code from the geocoding service.
     *
     * @return Error code or null if not available
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Checks if the operation can be retried.
     *
     * @return true if the operation can be retried, false otherwise
     */
    public boolean isRetryable() {
        return retryable;
    }
}