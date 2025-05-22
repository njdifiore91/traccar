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
package org.traccar.observability;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * Netty channel handler that adds OpenTelemetry distributed tracing capabilities.
 * Creates spans for inbound and outbound messages to track request flow across services.
 */
public class OpenTelemetryTracingHandler extends ChannelDuplexHandler {

    private static final AttributeKey<String> PROTOCOL_KEY = AttributeKey.stringKey("protocol");
    private static final AttributeKey<String> REMOTE_ADDRESS_KEY = AttributeKey.stringKey("remote.address");
    private static final AttributeKey<String> MESSAGE_TYPE_KEY = AttributeKey.stringKey("message.type");
    private static final AttributeKey<Long> MESSAGE_SIZE_KEY = AttributeKey.longKey("message.size");

    private final Tracer tracer;
    private final String protocol;

    public OpenTelemetryTracingHandler(Tracer tracer, String protocol) {
        this.tracer = tracer;
        this.protocol = protocol;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        Span span = tracer.spanBuilder("channel.read")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute(PROTOCOL_KEY, protocol)
                .setAttribute(REMOTE_ADDRESS_KEY, ctx.channel().remoteAddress().toString())
                .setAttribute(MESSAGE_TYPE_KEY, msg.getClass().getSimpleName())
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // Add message size if available
            if (msg instanceof io.netty.buffer.ByteBuf byteBuf) {
                span.setAttribute(MESSAGE_SIZE_KEY, byteBuf.readableBytes());
            }
            
            // Propagate to next handler
            ctx.fireChannelRead(msg);
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
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        Span span = tracer.spanBuilder("channel.write")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute(PROTOCOL_KEY, protocol)
                .setAttribute(REMOTE_ADDRESS_KEY, ctx.channel().remoteAddress().toString())
                .setAttribute(MESSAGE_TYPE_KEY, msg.getClass().getSimpleName())
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            // Add message size if available
            if (msg instanceof io.netty.buffer.ByteBuf byteBuf) {
                span.setAttribute(MESSAGE_SIZE_KEY, byteBuf.readableBytes());
            }
            
            // Add promise completion listener
            promise.addListener(future -> {
                if (future.isSuccess()) {
                    span.setStatus(StatusCode.OK);
                } else {
                    span.recordException(future.cause());
                    span.setStatus(StatusCode.ERROR, future.cause().getMessage());
                }
                span.end();
            });
            
            // Propagate to next handler
            ctx.write(msg, promise);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.end();
            throw e;
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        Span span = tracer.spanBuilder("channel.exception")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute(PROTOCOL_KEY, protocol)
                .setAttribute(REMOTE_ADDRESS_KEY, ctx.channel().remoteAddress().toString())
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            span.recordException(cause);
            span.setStatus(StatusCode.ERROR, cause.getMessage());
            
            // Propagate to next handler
            ctx.fireExceptionCaught(cause);
        } finally {
            span.end();
        }
    }
}