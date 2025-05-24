package org.traccar.template;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.exception.MethodInvocationException;
import org.apache.velocity.exception.ParseErrorException;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Renders notification templates with provided context data, transforming template
 * definitions into formatted notification messages. This component encapsulates the
 * Apache Velocity rendering engine, providing a clean interface for template rendering
 * with comprehensive error handling and performance monitoring.
 */
@Component
public class TemplateRenderer {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateRenderer.class);

    private final VelocityEngine velocityEngine;
    private final TemplateCache templateCache;
    private final MeterRegistry meterRegistry;
    private final long renderTimeoutMs;

    /**
     * Output format types supported by the template renderer.
     */
    public enum OutputFormat {
        TEXT,
        HTML,
        JSON
    }

    /**
     * Exception thrown when a template rendering operation fails.
     */
    public static class TemplateRenderException extends Exception {
        private final String templateName;

        public TemplateRenderException(String templateName, String message) {
            super(message);
            this.templateName = templateName;
        }

        public TemplateRenderException(String templateName, String message, Throwable cause) {
            super(message, cause);
            this.templateName = templateName;
        }

        public String getTemplateName() {
            return templateName;
        }
    }

    /**
     * Creates a new TemplateRenderer with the specified dependencies.
     *
     * @param velocityEngine The Velocity engine used for template rendering
     * @param templateCache The cache for template storage and retrieval
     * @param meterRegistry The registry for performance metrics
     * @param renderTimeoutMs The maximum time in milliseconds allowed for rendering a template
     */
    @Autowired
    public TemplateRenderer(
            VelocityEngine velocityEngine,
            TemplateCache templateCache,
            MeterRegistry meterRegistry,
            @Value("${notification.template.render.timeout:5000}") long renderTimeoutMs) {
        this.velocityEngine = velocityEngine;
        this.templateCache = templateCache;
        this.meterRegistry = meterRegistry;
        this.renderTimeoutMs = renderTimeoutMs;
    }

    /**
     * Asynchronously renders a template with the provided context data.
     *
     * @param templateName The name of the template to render
     * @param contextData The data to use for template rendering
     * @return A CompletableFuture that will complete with the rendered template content
     */
    public CompletableFuture<String> renderAsync(String templateName, Map<String, Object> contextData) {
        return renderAsync(templateName, contextData, OutputFormat.TEXT);
    }

    /**
     * Asynchronously renders a template with the provided context data in the specified output format.
     *
     * @param templateName The name of the template to render
     * @param contextData The data to use for template rendering
     * @param outputFormat The desired output format
     * @return A CompletableFuture that will complete with the rendered template content
     */
    public CompletableFuture<String> renderAsync(String templateName, Map<String, Object> contextData, OutputFormat outputFormat) {
        Timer.Sample sample = Timer.start(meterRegistry);
        
        return CompletableFuture.supplyAsync(() -> {
            try {
                String result = renderTemplate(templateName, contextData, outputFormat);
                sample.stop(meterRegistry.timer("template.render.time", "template", templateName, "format", outputFormat.name()));
                return result;
            } catch (Exception e) {
                sample.stop(meterRegistry.timer("template.render.error", "template", templateName, "format", outputFormat.name()));
                throw new RuntimeException(e);
            }
        }).orTimeout(renderTimeoutMs, TimeUnit.MILLISECONDS)
          .exceptionally(ex -> {
              if (ex instanceof TimeoutException) {
                  LOGGER.error("Template rendering timed out after {} ms: {}", renderTimeoutMs, templateName);
                  meterRegistry.counter("template.render.timeout", "template", templateName).increment();
                  throw new RuntimeException(new TemplateRenderException(templateName, 
                          "Template rendering timed out after " + renderTimeoutMs + " ms", ex));
              } else {
                  Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
                  LOGGER.error("Failed to render template: {}", templateName, cause);
                  meterRegistry.counter("template.render.failure", "template", templateName, 
                          "error", cause.getClass().getSimpleName()).increment();
                  throw new RuntimeException(new TemplateRenderException(templateName, 
                          "Failed to render template: " + cause.getMessage(), cause));
              }
          });
    }

    /**
     * Synchronously renders a template with the provided context data.
     *
     * @param templateName The name of the template to render
     * @param contextData The data to use for template rendering
     * @return The rendered template content
     * @throws TemplateRenderException If an error occurs during template rendering
     */
    public String render(String templateName, Map<String, Object> contextData) throws TemplateRenderException {
        return render(templateName, contextData, OutputFormat.TEXT);
    }

    /**
     * Synchronously renders a template with the provided context data in the specified output format.
     *
     * @param templateName The name of the template to render
     * @param contextData The data to use for template rendering
     * @param outputFormat The desired output format
     * @return The rendered template content
     * @throws TemplateRenderException If an error occurs during template rendering
     */
    public String render(String templateName, Map<String, Object> contextData, OutputFormat outputFormat) throws TemplateRenderException {
        Timer.Sample sample = Timer.start(meterRegistry);
        try {
            String result = renderTemplate(templateName, contextData, outputFormat);
            sample.stop(meterRegistry.timer("template.render.time", "template", templateName, "format", outputFormat.name()));
            return result;
        } catch (Exception e) {
            sample.stop(meterRegistry.timer("template.render.error", "template", templateName, "format", outputFormat.name()));
            meterRegistry.counter("template.render.failure", "template", templateName, 
                    "error", e.getClass().getSimpleName()).increment();
            throw new TemplateRenderException(templateName, "Failed to render template: " + e.getMessage(), e);
        }
    }

    /**
     * Internal method to perform the actual template rendering.
     *
     * @param templateName The name of the template to render
     * @param contextData The data to use for template rendering
     * @param outputFormat The desired output format
     * @return The rendered template content
     * @throws TemplateRenderException If an error occurs during template rendering
     */
    private String renderTemplate(String templateName, Map<String, Object> contextData, OutputFormat outputFormat) 
            throws TemplateRenderException {
        try {
            // Prepare the template name based on the output format
            String formattedTemplateName = formatTemplateName(templateName, outputFormat);
            
            // Create a Velocity context with the provided data
            VelocityContext context = new VelocityContext();
            contextData.forEach(context::put);
            
            // Add format-specific context variables if needed
            switch (outputFormat) {
                case HTML:
                    context.put("escapeHtml", true);
                    break;
                case JSON:
                    context.put("escapeJson", true);
                    break;
                default:
                    // No special handling for TEXT format
                    break;
            }
            
            // Render the template
            StringWriter writer = new StringWriter();
            boolean templateExists = velocityEngine.mergeTemplate(formattedTemplateName, "UTF-8", context, writer);
            
            if (!templateExists) {
                throw new TemplateRenderException(formattedTemplateName, "Template not found: " + formattedTemplateName);
            }
            
            return writer.toString();
        } catch (ResourceNotFoundException e) {
            throw new TemplateRenderException(templateName, "Template not found: " + e.getMessage(), e);
        } catch (ParseErrorException e) {
            throw new TemplateRenderException(templateName, "Template parsing error: " + e.getMessage(), e);
        } catch (MethodInvocationException e) {
            throw new TemplateRenderException(templateName, "Method invocation error in template: " + e.getMessage(), e);
        } catch (Exception e) {
            throw new TemplateRenderException(templateName, "Unexpected error rendering template: " + e.getMessage(), e);
        }
    }

    /**
     * Formats the template name based on the output format.
     *
     * @param templateName The base template name
     * @param outputFormat The desired output format
     * @return The formatted template name
     */
    private String formatTemplateName(String templateName, OutputFormat outputFormat) {
        // If the template name already has an extension, use it as is
        if (templateName.endsWith(".vm")) {
            return templateName;
        }
        
        // Add format-specific extension if needed
        switch (outputFormat) {
            case HTML:
                return templateName + ".html.vm";
            case JSON:
                return templateName + ".json.vm";
            case TEXT:
            default:
                return templateName + ".vm";
        }
    }
}