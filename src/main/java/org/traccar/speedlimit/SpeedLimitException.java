/*
 * Copyright 2020 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.speedlimit;

import java.util.HashMap;
import java.util.Map;

/**
 * Exception thrown when speed limit service encounters an error.
 * Enhanced with support for circuit breaker scenarios, distributed tracing,
 * exception categorization for metrics, and service-specific information.
 */
public class SpeedLimitException extends RuntimeException {

    /**
     * Categorizes the type of exception for metrics collection and handling.
     */
    public enum Category {
        /**
         * Connection-related issues (network problems, unreachable service)
         */
        CONNECTION,
        
        /**
         * Timeout-related issues (service took too long to respond)
         */
        TIMEOUT,
        
        /**
         * Authentication or authorization failures
         */
        SECURITY,
        
        /**
         * Input validation failures
         */
        VALIDATION,
        
        /**
         * Service-side errors (internal server errors)
         */
        SERVICE,
        
        /**
         * Client-side errors (bad requests)
         */
        CLIENT,
        
        /**
         * Rate limiting or quota exceeded
         */
        RATE_LIMIT,
        
        /**
         * Unknown or unclassified errors
         */
        UNKNOWN
    }
    
    /**
     * Indicates the severity level of the exception.
     */
    public enum Severity {
        /**
         * Critical errors that require immediate attention
         */
        CRITICAL,
        
        /**
         * Serious errors that should be addressed soon
         */
        ERROR,
        
        /**
         * Less severe issues that should be monitored
         */
        WARNING,
        
        /**
         * Informational exceptions that don't indicate a problem
         */
        INFO
    }
    
    /**
     * Indicates whether the exception is transient (temporary) or permanent.
     * This is useful for circuit breaker scenarios to determine retry strategies.
     */
    public enum FailureType {
        /**
         * Temporary failure that might succeed on retry
         */
        TRANSIENT,
        
        /**
         * Permanent failure that won't succeed on retry
         */
        PERMANENT
    }

    private final Category category;
    private final Severity severity;
    private final FailureType failureType;
    private final boolean shouldTriggerCircuitBreaker;
    private final String serviceName;
    private final String instanceId;
    private final String errorCode;
    
    // Distributed tracing context
    private String traceId;
    private String spanId;
    private Map<String, String> tracingAttributes;
    
    // Additional metadata for metrics and troubleshooting
    private Map<String, String> metadata;

    /**
     * Creates a basic SpeedLimitException with a message.
     * Maintains backward compatibility with existing code.
     * 
     * @param message the detail message
     */
    public SpeedLimitException(String message) {
        super(message);
        this.category = Category.UNKNOWN;
        this.severity = Severity.ERROR;
        this.failureType = FailureType.PERMANENT;
        this.shouldTriggerCircuitBreaker = true;
        this.serviceName = "speed-limit-service";
        this.instanceId = null;
        this.errorCode = null;
        this.tracingAttributes = new HashMap<>();
        this.metadata = new HashMap<>();
    }

    /**
     * Creates a SpeedLimitException with detailed information.
     * 
     * @param message the detail message
     * @param cause the cause of the exception
     * @param category the exception category
     * @param severity the severity level
     * @param failureType whether the failure is transient or permanent
     * @param shouldTriggerCircuitBreaker whether this exception should trigger circuit breaker
     * @param serviceName the name of the service where the exception occurred
     * @param instanceId the instance ID of the service
     * @param errorCode a specific error code for this exception
     */
    public SpeedLimitException(String message, Throwable cause, 
                              Category category, 
                              Severity severity,
                              FailureType failureType,
                              boolean shouldTriggerCircuitBreaker,
                              String serviceName,
                              String instanceId,
                              String errorCode) {
        super(message, cause);
        this.category = category;
        this.severity = severity;
        this.failureType = failureType;
        this.shouldTriggerCircuitBreaker = shouldTriggerCircuitBreaker;
        this.serviceName = serviceName;
        this.instanceId = instanceId;
        this.errorCode = errorCode;
        this.tracingAttributes = new HashMap<>();
        this.metadata = new HashMap<>();
    }
    
    /**
     * Builder class for creating SpeedLimitException instances with a fluent API.
     */
    public static class Builder {
        private String message;
        private Throwable cause;
        private Category category = Category.UNKNOWN;
        private Severity severity = Severity.ERROR;
        private FailureType failureType = FailureType.PERMANENT;
        private boolean shouldTriggerCircuitBreaker = true;
        private String serviceName = "speed-limit-service";
        private String instanceId;
        private String errorCode;
        private String traceId;
        private String spanId;
        private Map<String, String> tracingAttributes = new HashMap<>();
        private Map<String, String> metadata = new HashMap<>();
        
        public Builder(String message) {
            this.message = message;
        }
        
        public Builder withCause(Throwable cause) {
            this.cause = cause;
            return this;
        }
        
        public Builder withCategory(Category category) {
            this.category = category;
            return this;
        }
        
        public Builder withSeverity(Severity severity) {
            this.severity = severity;
            return this;
        }
        
        public Builder withFailureType(FailureType failureType) {
            this.failureType = failureType;
            return this;
        }
        
        public Builder withCircuitBreakerTrigger(boolean shouldTrigger) {
            this.shouldTriggerCircuitBreaker = shouldTrigger;
            return this;
        }
        
        public Builder withServiceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }
        
        public Builder withInstanceId(String instanceId) {
            this.instanceId = instanceId;
            return this;
        }
        
        public Builder withErrorCode(String errorCode) {
            this.errorCode = errorCode;
            return this;
        }
        
        public Builder withTraceId(String traceId) {
            this.traceId = traceId;
            return this;
        }
        
        public Builder withSpanId(String spanId) {
            this.spanId = spanId;
            return this;
        }
        
        public Builder withTracingAttribute(String key, String value) {
            this.tracingAttributes.put(key, value);
            return this;
        }
        
        public Builder withMetadata(String key, String value) {
            this.metadata.put(key, value);
            return this;
        }
        
        public SpeedLimitException build() {
            SpeedLimitException exception = new SpeedLimitException(
                message, cause, category, severity, failureType,
                shouldTriggerCircuitBreaker, serviceName, instanceId, errorCode);
            
            exception.setTraceId(traceId);
            exception.setSpanId(spanId);
            exception.setTracingAttributes(tracingAttributes);
            exception.setMetadata(metadata);
            
            return exception;
        }
    }

    /**
     * Gets the exception category.
     * 
     * @return the category of this exception
     */
    public Category getCategory() {
        return category;
    }

    /**
     * Gets the severity level.
     * 
     * @return the severity of this exception
     */
    public Severity getSeverity() {
        return severity;
    }

    /**
     * Gets the failure type (transient or permanent).
     * 
     * @return the failure type of this exception
     */
    public FailureType getFailureType() {
        return failureType;
    }

    /**
     * Checks if this exception should trigger a circuit breaker.
     * 
     * @return true if this exception should trigger a circuit breaker
     */
    public boolean shouldTriggerCircuitBreaker() {
        return shouldTriggerCircuitBreaker;
    }

    /**
     * Gets the service name where the exception occurred.
     * 
     * @return the service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Gets the instance ID of the service where the exception occurred.
     * 
     * @return the instance ID
     */
    public String getInstanceId() {
        return instanceId;
    }

    /**
     * Gets the error code for this exception.
     * 
     * @return the error code
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * Gets the trace ID for distributed tracing.
     * 
     * @return the trace ID
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * Sets the trace ID for distributed tracing.
     * 
     * @param traceId the trace ID to set
     */
    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * Gets the span ID for distributed tracing.
     * 
     * @return the span ID
     */
    public String getSpanId() {
        return spanId;
    }

    /**
     * Sets the span ID for distributed tracing.
     * 
     * @param spanId the span ID to set
     */
    public void setSpanId(String spanId) {
        this.spanId = spanId;
    }

    /**
     * Gets the tracing attributes for distributed tracing.
     * 
     * @return the tracing attributes
     */
    public Map<String, String> getTracingAttributes() {
        return tracingAttributes;
    }

    /**
     * Sets the tracing attributes for distributed tracing.
     * 
     * @param tracingAttributes the tracing attributes to set
     */
    public void setTracingAttributes(Map<String, String> tracingAttributes) {
        this.tracingAttributes = tracingAttributes;
    }

    /**
     * Adds a tracing attribute for distributed tracing.
     * 
     * @param key the attribute key
     * @param value the attribute value
     */
    public void addTracingAttribute(String key, String value) {
        this.tracingAttributes.put(key, value);
    }

    /**
     * Gets the metadata for metrics collection and troubleshooting.
     * 
     * @return the metadata
     */
    public Map<String, String> getMetadata() {
        return metadata;
    }

    /**
     * Sets the metadata for metrics collection and troubleshooting.
     * 
     * @param metadata the metadata to set
     */
    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }

    /**
     * Adds a metadata entry for metrics collection and troubleshooting.
     * 
     * @param key the metadata key
     * @param value the metadata value
     */
    public void addMetadata(String key, String value) {
        this.metadata.put(key, value);
    }
    
    /**
     * Creates a builder for SpeedLimitException.
     * 
     * @param message the detail message
     * @return a new builder instance
     */
    public static Builder builder(String message) {
        return new Builder(message);
    }
    
    /**
     * Creates a formatted string representation of this exception including all details.
     * 
     * @return a detailed string representation
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("\nDetails: {");
        sb.append("\n  category: ").append(category);
        sb.append("\n  severity: ").append(severity);
        sb.append("\n  failureType: ").append(failureType);
        sb.append("\n  shouldTriggerCircuitBreaker: ").append(shouldTriggerCircuitBreaker);
        sb.append("\n  serviceName: ").append(serviceName);
        
        if (instanceId != null) {
            sb.append("\n  instanceId: ").append(instanceId);
        }
        
        if (errorCode != null) {
            sb.append("\n  errorCode: ").append(errorCode);
        }
        
        if (traceId != null) {
            sb.append("\n  traceId: ").append(traceId);
        }
        
        if (spanId != null) {
            sb.append("\n  spanId: ").append(spanId);
        }
        
        if (!tracingAttributes.isEmpty()) {
            sb.append("\n  tracingAttributes: ").append(tracingAttributes);
        }
        
        if (!metadata.isEmpty()) {
            sb.append("\n  metadata: ").append(metadata);
        }
        
        sb.append("\n}");
        return sb.toString();
    }
}