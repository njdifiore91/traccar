/*
 * Copyright 2016 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.geolocation;

import java.io.Serializable;

/**
 * Exception thrown during geolocation operations.
 * Enhanced for microservices environment with correlation ID support for distributed tracing
 * and service instance information for better error context.
 */
public class GeolocationException extends RuntimeException implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private String correlationId;
    private String serviceInstance;
    private String errorCode;
    
    /**
     * Constructs a new GeolocationException with the specified detail message.
     *
     * @param message the detail message
     */
    public GeolocationException(String message) {
        super(message);
    }

    /**
     * Constructs a new GeolocationException with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public GeolocationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new GeolocationException with the specified detail message, correlation ID,
     * and service instance information.
     *
     * @param message the detail message
     * @param correlationId the correlation ID for distributed tracing
     * @param serviceInstance the service instance identifier
     */
    public GeolocationException(String message, String correlationId, String serviceInstance) {
        super(message);
        this.correlationId = correlationId;
        this.serviceInstance = serviceInstance;
    }
    
    /**
     * Constructs a new GeolocationException with the specified detail message, cause, correlation ID,
     * service instance information, and error code.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     * @param correlationId the correlation ID for distributed tracing
     * @param serviceInstance the service instance identifier
     * @param errorCode the error code for categorization
     */
    public GeolocationException(String message, Throwable cause, String correlationId, 
                               String serviceInstance, String errorCode) {
        super(message, cause);
        this.correlationId = correlationId;
        this.serviceInstance = serviceInstance;
        this.errorCode = errorCode;
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
     * Gets the service instance identifier.
     *
     * @return the service instance identifier
     */
    public String getServiceInstance() {
        return serviceInstance;
    }

    /**
     * Sets the service instance identifier.
     *
     * @param serviceInstance the service instance identifier to set
     */
    public void setServiceInstance(String serviceInstance) {
        this.serviceInstance = serviceInstance;
    }

    /**
     * Gets the error code for categorization.
     *
     * @return the error code
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Sets the error code for categorization.
     *
     * @param errorCode the error code to set
     */
    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }
    
    /**
     * Returns a string representation of this exception including the message,
     * correlation ID, service instance, and error code if available.
     *
     * @return a string representation of this exception
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getName());
        sb.append(": ").append(getMessage());
        
        if (correlationId != null) {
            sb.append(" [correlationId=").append(correlationId).append("]");
        }
        
        if (serviceInstance != null) {
            sb.append(" [serviceInstance=").append(serviceInstance).append("]");
        }
        
        if (errorCode != null) {
            sb.append(" [errorCode=").append(errorCode).append("]");
        }
        
        return sb.toString();
    }
}