/*
 * Copyright 2019 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.handler.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.NetworkMessage;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.BufferUtil;
import org.traccar.helper.NetworkUtil;
import org.traccar.model.LogRecord;
import org.traccar.session.ConnectionManager;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * A Netty channel handler that logs network messages with OpenTelemetry integration.
 * Supports distributed tracing, metrics collection, and structured JSON logging.
 */
public class StandardLoggingHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardLoggingHandler.class);
    
    // OpenTelemetry components
    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer("org.traccar.handler.network");
    private static final Meter METER = GlobalOpenTelemetry.getMeter("org.traccar.handler.network");
    
    // Metrics
    private static final LongCounter INBOUND_MESSAGES = METER.counterBuilder("traccar.network.messages.received")
            .setDescription("Number of network messages received")
            .build();
    
    private static final LongCounter OUTBOUND_MESSAGES = METER.counterBuilder("traccar.network.messages.sent")
            .setDescription("Number of network messages sent")
            .build();
    
    // Attribute keys for spans and metrics
    private static final AttributeKey<String> PROTOCOL_KEY = AttributeKey.stringKey("protocol");
    private static final AttributeKey<String> DIRECTION_KEY = AttributeKey.stringKey("direction");
    private static final AttributeKey<String> REMOTE_ADDRESS_KEY = AttributeKey.stringKey("remote.address");
    private static final AttributeKey<String> LOCAL_ADDRESS_KEY = AttributeKey.stringKey("local.address");
    private static final AttributeKey<String> SESSION_ID_KEY = AttributeKey.stringKey("session.id");
    private static final AttributeKey<Long> MESSAGE_SIZE_KEY = AttributeKey.longKey("message.size");
    private static final AttributeKey<String> CONTAINER_ID_KEY = AttributeKey.stringKey("container.id");
    private static final AttributeKey<String> POD_NAME_KEY = AttributeKey.stringKey("k8s.pod.name");
    
    private final String protocol;
    private ConnectionManager connectionManager;
    private boolean decodeTextData;
    private String containerId;
    private String podName;

    public StandardLoggingHandler(String protocol) {
        this.protocol = protocol;
        // Try to get container-specific information for containerized environments
        this.containerId = System.getenv("CONTAINER_ID");
        this.podName = System.getenv("POD_NAME");
    }

    @Inject
    public void setConfig(Config config) {
        decodeTextData = config.getBoolean(Keys.LOGGER_TEXT_PROTOCOL);
    }

    @Inject
    public void setConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Create a span for this inbound message
        Span span = TRACER.spanBuilder("network.receive")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(PROTOCOL_KEY, protocol)
                .setAttribute(DIRECTION_KEY, "inbound")
                .setAttribute(SESSION_ID_KEY, NetworkUtil.session(ctx.channel()))
                .startSpan();
        
        // Use the span as the current context
        try (Scope scope = span.makeCurrent()) {
            LogRecord record = createLogRecord(ctx, msg, span);
            log(ctx, false, record, span);
            super.channelRead(ctx, msg);
            if (record != null) {
                connectionManager.updateLog(record);
            }
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        // Create a span for this outbound message
        Span span = TRACER.spanBuilder("network.send")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(PROTOCOL_KEY, protocol)
                .setAttribute(DIRECTION_KEY, "outbound")
                .setAttribute(SESSION_ID_KEY, NetworkUtil.session(ctx.channel()))
                .startSpan();
        
        // Use the span as the current context
        try (Scope scope = span.makeCurrent()) {
            LogRecord record = createLogRecord(ctx, msg, span);
            log(ctx, true, record, span);
            super.write(ctx, msg, promise);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    private LogRecord createLogRecord(ChannelHandlerContext ctx, Object msg, Span span) {
        if (msg instanceof NetworkMessage networkMessage) {
            if (networkMessage.getMessage() instanceof ByteBuf data) {
                LogRecord record = new LogRecord(ctx.channel().localAddress(), networkMessage.getRemoteAddress());
                record.setProtocol(protocol);
                
                // Note: We don't modify the LogRecord with trace context
                // as the current LogRecord class doesn't support these fields
                
                // Set the data content based on configuration
                if (decodeTextData && BufferUtil.isPrintable(data, data.readableBytes())) {
                    record.setData(data.getCharSequence(
                            data.readerIndex(), data.readableBytes(), StandardCharsets.US_ASCII).toString()
                            .replace("\r", "\\r").replace("\n", "\\n"));
                } else {
                    record.setData(ByteBufUtil.hexDump(data));
                }
                
                // Record metrics
                boolean isInbound = span.getAttribute(DIRECTION_KEY).equals("inbound");
                Attributes attributes = Attributes.builder()
                        .put(PROTOCOL_KEY, protocol)
                        .put(DIRECTION_KEY, isInbound ? "inbound" : "outbound")
                        .put(MESSAGE_SIZE_KEY, (long) data.readableBytes())
                        .build();
                
                if (isInbound) {
                    INBOUND_MESSAGES.add(1, attributes);
                } else {
                    OUTBOUND_MESSAGES.add(1, attributes);
                }
                
                // Add additional attributes to the span
                span.setAttribute(MESSAGE_SIZE_KEY, data.readableBytes());
                span.setAttribute(REMOTE_ADDRESS_KEY, networkMessage.getRemoteAddress().toString());
                span.setAttribute(LOCAL_ADDRESS_KEY, ctx.channel().localAddress().toString());
                
                // Add container-specific attributes if available
                if (containerId != null) {
                    span.setAttribute(CONTAINER_ID_KEY, containerId);
                }
                if (podName != null) {
                    span.setAttribute(POD_NAME_KEY, podName);
                }
                
                return record;
            }
        }
        return null;
    }

    private void log(ChannelHandlerContext ctx, boolean downstream, LogRecord record, Span span) {
        if (record != null) {
            // Create structured JSON log entry
            Map<String, Object> logEntry = new HashMap<>();
            logEntry.put("timestamp", System.currentTimeMillis());
            logEntry.put("level", "INFO");
            logEntry.put("protocol", protocol);
            logEntry.put("session", NetworkUtil.session(ctx.channel()));
            logEntry.put("direction", downstream ? "outbound" : "inbound");
            
            InetSocketAddress address = record.getAddress();
            logEntry.put("remoteHost", address.getHostString());
            logEntry.put("remotePort", address.getPort());
            
            InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
            logEntry.put("localHost", localAddress.getHostString());
            logEntry.put("localPort", localAddress.getPort());
            
            logEntry.put("data", record.getData());
            
            // Add trace context for correlation
            logEntry.put("traceId", span.getSpanContext().getTraceId());
            logEntry.put("spanId", span.getSpanContext().getSpanId());
            
            // Add container information if available
            if (containerId != null) {
                logEntry.put("containerId", containerId);
            }
            if (podName != null) {
                logEntry.put("podName", podName);
            }
            
            // Log as JSON string
            LOGGER.info(formatAsJson(logEntry));
        }
    }
    
    /**
     * Simple JSON formatter for the log entry.
     * In a production environment, you would use a proper JSON library like Jackson or Gson.
     */
    private String formatAsJson(Map<String, Object> logEntry) {
        StringBuilder json = new StringBuilder();
        json.append("{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : logEntry.entrySet()) {
            if (!first) {
                json.append(",");
            }
            first = false;
            json.append("\"").append(entry.getKey()).append("\"");
            json.append(":");
            if (entry.getValue() instanceof String) {
                json.append("\"").append(escapeJsonString((String) entry.getValue())).append("\"");
            } else if (entry.getValue() instanceof Number) {
                json.append(entry.getValue());
            } else if (entry.getValue() == null) {
                json.append("null");
            } else {
                json.append("\"").append(escapeJsonString(entry.getValue().toString())).append("\"");
            }
        }
        json.append("}");
        return json.toString();
    }
    
    /**
     * Escapes special characters in JSON strings.
     */
    private String escapeJsonString(String input) {
        return input.replace("\\", "\\\\").replace("\"", "\\\"").replace("\r", "\\r").replace("\n", "\\n");
    }
}