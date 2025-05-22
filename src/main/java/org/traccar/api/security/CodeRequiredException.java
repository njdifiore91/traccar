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
package org.traccar.api.security;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

/**
 * Exception thrown when a required authentication code is not provided.
 * This exception is integrated with OpenTelemetry for distributed tracing
 * and supports API Gateway error standardization.
 */
public class CodeRequiredException extends SecurityException {
    
    private static final String ERROR_CODE = "AUTH_CODE_REQUIRED";
    private String correlationId;
    
    /**
     * Constructs a new CodeRequiredException with default message.
     * Records the current trace context for distributed tracing.
     */
    public CodeRequiredException() {
        this("Code not provided", null);
    }
    
    /**
     * Constructs a new CodeRequiredException with a specified message.
     * Records the current trace context for distributed tracing.
     * 
     * @param message the detail message
     * @param correlationId the correlation ID for distributed tracing, or null to use the current span's trace ID
     */
    public CodeRequiredException(String message, String correlationId) {
        super(message);
        this.correlationId = correlationId;
        
        // Record exception in the current span if available
        Span currentSpan = Span.current();
        if (currentSpan != null && !currentSpan.equals(Span.getInvalid())) {
            currentSpan.recordException(this);
            currentSpan.setStatus(StatusCode.ERROR, message);
            
            // If no correlation ID was provided, use the trace ID as correlation ID
            if (this.correlationId == null) {
                this.correlationId = currentSpan.getSpanContext().getTraceId();
            }
            
            // Add error attributes for standardized error handling
            currentSpan.setAttribute("error.type", this.getClass().getName());
            currentSpan.setAttribute("error.code", ERROR_CODE);
            currentSpan.setAttribute("error.correlation_id", this.correlationId);
        }
    }
    
    /**
     * Gets the error code for this exception.
     * Used for API Gateway error standardization.
     * 
     * @return the standardized error code
     */
    public String getErrorCode() {
        return ERROR_CODE;
    }
    
    /**
     * Gets the correlation ID for this exception.
     * Used for distributed tracing across services.
     * 
     * @return the correlation ID
     */
    public String getCorrelationId() {
        return correlationId;
    }
}