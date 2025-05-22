/*
 * Copyright 2021 - 2022 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.apache.velocity.tools.generic.DateTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.signature.TokenManager;
import org.traccar.helper.model.UserUtil;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.storage.StorageException;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.Map;

@Singleton
public class TextTemplateFormatter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TextTemplateFormatter.class);

    private final VelocityEngine velocityEngine;
    private final TokenManager tokenManager;
    private final Tracer tracer;
    private final Meter meter;
    
    // Metrics for template rendering
    private final LongCounter templateRenderCounter;
    private final LongCounter templateRenderErrorCounter;
    
    private static final AttributeKey<String> TEMPLATE_NAME_KEY = AttributeKey.stringKey("template.name");
    private static final AttributeKey<String> TEMPLATE_PATH_KEY = AttributeKey.stringKey("template.path");
    private static final AttributeKey<String> ERROR_TYPE_KEY = AttributeKey.stringKey("error.type");
    private static final AttributeKey<String> CORRELATION_ID_KEY = AttributeKey.stringKey("correlation.id");

    @Inject
    public TextTemplateFormatter(
            VelocityEngine velocityEngine, 
            TokenManager tokenManager,
            Tracer tracer,
            Meter meter) {
        this.velocityEngine = velocityEngine;
        this.tokenManager = tokenManager;
        this.tracer = tracer;
        this.meter = meter;
        
        // Initialize metrics
        this.templateRenderCounter = meter.counterBuilder("template.render.count")
                .setDescription("Number of template render operations")
                .build();
        
        this.templateRenderErrorCounter = meter.counterBuilder("template.render.error.count")
                .setDescription("Number of template render errors")
                .build();
    }

    public VelocityContext prepareContext(Server server, User user) {
        return prepareContext(server, user, null, null);
    }
    
    public VelocityContext prepareContext(Server server, User user, String correlationId, Map<String, Object> brokerMetadata) {
        VelocityContext velocityContext = new VelocityContext();

        if (user != null) {
            velocityContext.put("user", user);
            velocityContext.put("timezone", UserUtil.getTimezone(server, user));
            try {
                velocityContext.put("token", tokenManager.generateToken(user.getId()));
            } catch (IOException | GeneralSecurityException | StorageException e) {
                LOGGER.warn("Token generation failed", e);
            }
        }

        velocityContext.put("webUrl", velocityEngine.getProperty("web.url"));
        velocityContext.put("dateTool", new DateTool());
        velocityContext.put("numberTool", new NumberTool());
        velocityContext.put("locale", Locale.getDefault());
        
        // Add correlation ID if available
        if (correlationId != null) {
            velocityContext.put("correlationId", correlationId);
        } else {
            // Use current span's trace ID as correlation ID if available
            Span currentSpan = Span.current();
            if (currentSpan != null && !currentSpan.equals(Span.getInvalid())) {
                String traceId = currentSpan.getSpanContext().getTraceId();
                velocityContext.put("correlationId", traceId);
            }
        }
        
        // Add message broker metadata if available
        if (brokerMetadata != null && !brokerMetadata.isEmpty()) {
            velocityContext.put("brokerMetadata", brokerMetadata);
        }

        return velocityContext;
    }

    public Template getTemplate(String name, String path) {
        String templateFilePath = Paths.get(path, name + ".vm").toString();
        try {
            return velocityEngine.getTemplate(templateFilePath, StandardCharsets.UTF_8.name());
        } catch (ResourceNotFoundException e) {
            LOGGER.error("Template not found: {}", templateFilePath, e);
            throw e;
        } catch (ParseErrorException e) {
            LOGGER.error("Template parse error: {}", templateFilePath, e);
            throw e;
        } catch (Exception e) {
            LOGGER.error("Error loading template: {}", templateFilePath, e);
            throw e;
        }
    }

    public NotificationMessage formatMessage(VelocityContext velocityContext, String name, String templatePath) {
        // Create a span for template rendering
        Span span = tracer.spanBuilder("template.render")
                .setAttribute(TEMPLATE_NAME_KEY, name)
                .setAttribute(TEMPLATE_PATH_KEY, templatePath)
                .startSpan();
        
        // Get correlation ID from context if available
        String correlationId = (String) velocityContext.get("correlationId");
        if (correlationId != null) {
            span.setAttribute(CORRELATION_ID_KEY, correlationId);
        }
        
        try (io.opentelemetry.context.Scope scope = span.makeCurrent()) {
            long startTime = System.currentTimeMillis();
            StringWriter writer = new StringWriter();
            
            try {
                Template template = getTemplate(name, templatePath);
                template.merge(velocityContext, writer);
                
                // Record successful template rendering
                templateRenderCounter.add(1, Attributes.of(
                        TEMPLATE_NAME_KEY, name,
                        TEMPLATE_PATH_KEY, templatePath));
                
                // Record rendering time as a span attribute
                long renderTime = System.currentTimeMillis() - startTime;
                span.setAttribute("template.render.time_ms", renderTime);
                
                return new NotificationMessage((String) velocityContext.get("subject"), writer.toString());
            } catch (ResourceNotFoundException e) {
                recordTemplateError(span, "resource_not_found", e, name, templatePath);
                throw new TemplateRenderingException("Template not found: " + name, e, correlationId);
            } catch (ParseErrorException e) {
                recordTemplateError(span, "parse_error", e, name, templatePath);
                throw new TemplateRenderingException("Template parse error: " + name, e, correlationId);
            } catch (MethodInvocationException e) {
                recordTemplateError(span, "method_invocation_error", e, name, templatePath);
                throw new TemplateRenderingException("Template method invocation error: " + name, e, correlationId);
            } catch (Exception e) {
                recordTemplateError(span, "general_error", e, name, templatePath);
                throw new TemplateRenderingException("Template rendering error: " + name, e, correlationId);
            }
        } finally {
            span.end();
        }
    }
    
    private void recordTemplateError(Span span, String errorType, Exception e, String name, String templatePath) {
        span.setStatus(StatusCode.ERROR, e.getMessage());
        span.recordException(e);
        span.setAttribute(ERROR_TYPE_KEY, errorType);
        
        // Record error metric
        templateRenderErrorCounter.add(1, Attributes.of(
                TEMPLATE_NAME_KEY, name,
                TEMPLATE_PATH_KEY, templatePath,
                ERROR_TYPE_KEY, errorType));
        
        LOGGER.error("Template rendering error [type={}, template={}, path={}]: {}", 
                errorType, name, templatePath, e.getMessage(), e);
    }
    
    /**
     * Custom exception for template rendering errors that includes correlation ID
     */
    public static class TemplateRenderingException extends RuntimeException {
        private final String correlationId;
        
        public TemplateRenderingException(String message, Throwable cause, String correlationId) {
            super(message, cause);
            this.correlationId = correlationId;
        }
        
        public String getCorrelationId() {
            return correlationId;
        }
    }
}