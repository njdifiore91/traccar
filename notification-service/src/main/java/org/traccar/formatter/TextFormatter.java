/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.formatter;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.tools.generic.DateTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.traccar.api.signature.TokenManager;
import org.traccar.helper.model.UserUtil;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.notification.NotificationMessage;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Formats plain text notifications using Apache Velocity templates.
 * This component prepares context data, merges templates, and produces formatted text content
 * for email and other text-based notification channels.
 */
@Service
public class TextFormatter {

    private static final Logger LOGGER = LoggerFactory.getLogger(TextFormatter.class);
    private static final String TEMPLATE_CACHE_PREFIX = "template:";
    private static final String CIRCUIT_BREAKER_NAME = "templateLoader";

    private final VelocityEngine velocityEngine;
    private final TokenManager tokenManager;
    private final RedisTemplate<String, Template> redisTemplate;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    /**
     * Creates a new TextFormatter with the specified dependencies.
     *
     * @param velocityEngine The Velocity template engine
     * @param tokenManager The token manager for generating authentication tokens
     * @param redisTemplate Redis template for distributed template caching
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     */
    @Autowired
    public TextFormatter(
            VelocityEngine velocityEngine,
            TokenManager tokenManager,
            @Qualifier("templateRedisTemplate") RedisTemplate<String, Template> redisTemplate,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.velocityEngine = velocityEngine;
        this.tokenManager = tokenManager;
        this.redisTemplate = redisTemplate;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
    }

    /**
     * Prepares a Velocity context with server and user information.
     *
     * @param server The server configuration
     * @param user The user for whom the context is prepared
     * @return A VelocityContext with user and server information
     */
    public VelocityContext prepareContext(Server server, User user) {
        Span span = tracer.spanBuilder("prepareContext").startSpan();
        try (Scope scope = span.makeCurrent()) {
            VelocityContext velocityContext = new VelocityContext();

            if (user != null) {
                velocityContext.put("user", user);
                velocityContext.put("timezone", UserUtil.getTimezone(server, user));
                try {
                    velocityContext.put("token", tokenManager.generateToken(user.getId()));
                } catch (IOException | GeneralSecurityException e) {
                    LOGGER.warn("Token generation failed", e);
                    span.recordException(e);
                }
            }

            velocityContext.put("webUrl", velocityEngine.getProperty("web.url"));
            velocityContext.put("dateTool", new DateTool());
            velocityContext.put("numberTool", new NumberTool());
            velocityContext.put("locale", Locale.getDefault());

            return velocityContext;
        } finally {
            span.end();
        }
    }

    /**
     * Gets a template from the cache or loads it if not cached.
     * Protected by a circuit breaker to prevent cascading failures.
     *
     * @param name The template name
     * @param path The template path
     * @return The loaded template
     */
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "getTemplateFallback")
    public Template getTemplate(String name, String path) {
        Span span = tracer.spanBuilder("getTemplate").startSpan();
        span.setAttribute("template.name", name);
        span.setAttribute("template.path", path);
        
        try (Scope scope = span.makeCurrent()) {
            String cacheKey = TEMPLATE_CACHE_PREFIX + path + ":" + name;
            Template template = redisTemplate.opsForValue().get(cacheKey);
            
            if (template == null) {
                LOGGER.debug("Template cache miss for {}", name);
                String templateFilePath = Paths.get(path, name + ".vm").toString();
                template = velocityEngine.getTemplate(templateFilePath, StandardCharsets.UTF_8.name());
                redisTemplate.opsForValue().set(cacheKey, template);
                meterRegistry.counter("template.cache.miss").increment();
            } else {
                LOGGER.debug("Template cache hit for {}", name);
                meterRegistry.counter("template.cache.hit").increment();
            }
            
            return template;
        } finally {
            span.end();
        }
    }

    /**
     * Fallback method for template loading when the circuit breaker is open.
     * Directly loads the template without caching.
     *
     * @param name The template name
     * @param path The template path
     * @param e The exception that triggered the fallback
     * @return The loaded template
     */
    public Template getTemplateFallback(String name, String path, Exception e) {
        LOGGER.warn("Using fallback for template loading: {}", name, e);
        meterRegistry.counter("template.fallback").increment();
        String templateFilePath = Paths.get(path, name + ".vm").toString();
        return velocityEngine.getTemplate(templateFilePath, StandardCharsets.UTF_8.name());
    }

    /**
     * Formats a message synchronously using the specified template.
     *
     * @param velocityContext The Velocity context with data for the template
     * @param name The template name
     * @param templatePath The template path
     * @return The formatted notification message
     */
    public NotificationMessage formatMessage(VelocityContext velocityContext, String name, String templatePath) {
        Span span = tracer.spanBuilder("formatMessage").startSpan();
        span.setAttribute("template.name", name);
        
        Timer.Sample sample = Timer.start(meterRegistry);
        try (Scope scope = span.makeCurrent()) {
            StringWriter writer = new StringWriter();
            getTemplate(name, templatePath).merge(velocityContext, writer);
            return new NotificationMessage((String) velocityContext.get("subject"), writer.toString());
        } finally {
            sample.stop(meterRegistry.timer("template.render.time", "template", name));
            span.end();
        }
    }

    /**
     * Formats a message asynchronously using the specified template.
     *
     * @param velocityContext The Velocity context with data for the template
     * @param name The template name
     * @param templatePath The template path
     * @return A CompletableFuture that will contain the formatted notification message
     */
    @Async("templateExecutor")
    public CompletableFuture<NotificationMessage> formatMessageAsync(
            VelocityContext velocityContext, String name, String templatePath) {
        try {
            NotificationMessage message = formatMessage(velocityContext, name, templatePath);
            return CompletableFuture.completedFuture(message);
        } catch (Exception e) {
            LOGGER.error("Error formatting message asynchronously", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Formats a message with a timeout to prevent long-running template rendering.
     *
     * @param velocityContext The Velocity context with data for the template
     * @param name The template name
     * @param templatePath The template path
     * @param timeout The maximum time to wait for rendering
     * @param unit The time unit for the timeout
     * @return The formatted notification message or null if timeout occurs
     */
    public NotificationMessage formatMessageWithTimeout(
            VelocityContext velocityContext, String name, String templatePath, long timeout, TimeUnit unit) {
        try {
            return formatMessageAsync(velocityContext, name, templatePath).get(timeout, unit);
        } catch (Exception e) {
            LOGGER.error("Template rendering timed out or failed", e);
            meterRegistry.counter("template.render.timeout").increment();
            return null;
        }
    }
}