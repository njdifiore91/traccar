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
package org.traccar.web;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.Tracer;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.util.DateCache;
import org.eclipse.jetty.util.component.ContainerLifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.helper.SessionHelper;

import java.time.Instant;
import java.util.Locale;
import java.util.TimeZone;

/**
 * Enhanced request logger that provides structured JSON logging with OpenTelemetry integration
 * for distributed tracing and metrics collection.
 */
public class WebRequestLog extends ContainerLifeCycle implements RequestLog {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebRequestLog.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    private final Writer writer;
    private final DateCache dateCache;
    
    // OpenTelemetry components
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter requestCounter;
    private final LongCounter errorCounter;
    private final LongHistogram requestDurationHistogram;

    public WebRequestLog(Writer writer) {
        this.writer = writer;
        addBean(writer);
        
        // Initialize date formatter for timestamps
        this.dateCache = new DateCache(
                "dd/MMM/yyyy:HH:mm:ss ZZZ", Locale.getDefault(), TimeZone.getTimeZone("GMT"));
        
        // Initialize OpenTelemetry components
        tracer = GlobalOpenTelemetry.getTracer("org.traccar.web.WebRequestLog");
        meter = GlobalOpenTelemetry.getMeter("org.traccar.web.WebRequestLog");
        
        // Create metrics for request logging performance
        requestCounter = meter.counterBuilder("http.server.requests")
                .setDescription("Total number of HTTP requests processed")
                .build();
        
        errorCounter = meter.counterBuilder("http.server.errors")
                .setDescription("Total number of HTTP request errors")
                .build();
                
        requestDurationHistogram = meter.histogramBuilder("http.server.duration")
                .setDescription("HTTP request duration in milliseconds")
                .setUnit("ms")
                .build();
    }

    @Override
    public void log(Request request, Response response) {
        try {
            // Extract trace context from the current request if available
            Span currentSpan = Span.current();
            SpanContext spanContext = currentSpan.getSpanContext();
            String traceId = spanContext.isValid() ? spanContext.getTraceId() : "";
            String spanId = spanContext.isValid() ? spanContext.getSpanId() : "";
            
            // Extract user ID from session
            Long userId = (Long) request.getSession().getAttribute(SessionHelper.USER_ID_KEY);
            
            // Get HTTP status code
            int statusCode = response.getCommittedMetaData().getStatus();
            
            // Calculate request duration in milliseconds
            long requestDuration = System.currentTimeMillis() - request.getTimeStamp();
            
            // Common attributes for metrics
            Attributes attributes = Attributes.of(
                    AttributeKey.stringKey("method"), request.getMethod(),
                    AttributeKey.stringKey("path"), request.getOriginalURI(),
                    AttributeKey.longKey("status"), statusCode);
            
            // Record metrics
            requestCounter.add(1, attributes);
            requestDurationHistogram.record(requestDuration, attributes);
            
            if (statusCode >= 400) {
                errorCounter.add(1, attributes);
            }
            
            // Create structured JSON log entry
            ObjectNode jsonLog = OBJECT_MAPPER.createObjectNode()
                    .put("timestamp", Instant.now().toString())
                    .put("level", statusCode >= 400 ? "ERROR" : "INFO")
                    .put("service", "traccar-web")
                    .put("remoteHost", request.getRemoteHost())
                    .put("userId", userId != null ? userId.toString() : "-")
                    .put("requestTime", dateCache.format(request.getTimeStamp()))
                    .put("method", request.getMethod())
                    .put("uri", request.getOriginalURI())
                    .put("protocol", request.getProtocol())
                    .put("status", statusCode)
                    .put("bytes", response.getHttpChannel().getBytesWritten())
                    .put("duration_ms", requestDuration)
                    .put("traceId", traceId)
                    .put("spanId", spanId);
            
            // Write the JSON log entry
            writer.write(jsonLog.toString());
            
        } catch (Throwable t) {
            LOGGER.warn("Failed to log request", t);
        }
    }
}