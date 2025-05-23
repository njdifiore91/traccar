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
package org.traccar;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.Position;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Processing handler for decoded positions.
 * Publishes positions to the message broker for further processing by the Position Service.
 */
@ChannelHandler.Sharable
public class ProcessingHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingHandler.class);

    private final MessageProducer messageProducer;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final String positionsTopic;

    private final Timer publishTimer;
    private final Counter publishedCounter;
    private final Counter failedCounter;

    /**
     * Constructs a new ProcessingHandler.
     *
     * @param messageProducer The message producer for publishing positions to the message broker
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param config The service configuration
     */
    @Inject
    public ProcessingHandler(
            MessageProducer messageProducer,
            MeterRegistry meterRegistry,
            Tracer tracer,
            ProtocolServiceConfig config) {
        this.messageProducer = messageProducer;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.positionsTopic = config.getString("message.topic.positions", "positions");

        this.publishTimer = Timer.builder("protocol.positions.publish.time")
                .description("Time taken to publish positions to the message broker")
                .register(meterRegistry);
        this.publishedCounter = Counter.builder("protocol.positions.published")
                .description("Number of positions published to the message broker")
                .register(meterRegistry);
        this.failedCounter = Counter.builder("protocol.positions.failed")
                .description("Number of positions that failed to publish to the message broker")
                .register(meterRegistry);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof Position) {
            Position position = (Position) msg;
            publishPosition(position, ctx);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    /**
     * Publishes a position to the message broker.
     *
     * @param position The position to publish
     * @param ctx The channel handler context
     */
    private void publishPosition(Position position, ChannelHandlerContext ctx) {
        Span span = tracer.spanBuilder("PublishPosition")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("deviceId", String.valueOf(position.getDeviceId()))
                .setAttribute("protocol", position.getProtocol())
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            String correlationId = UUID.randomUUID().toString();
            span.setAttribute("correlationId", correlationId);

            Map<String, Object> headers = new HashMap<>();
            headers.put(MessageHeaders.CORRELATION_ID, correlationId);
            headers.put(MessageHeaders.SOURCE_SERVICE, "protocol-service");
            headers.put(MessageHeaders.MESSAGE_TYPE, "position");
            headers.put(MessageHeaders.DEVICE_ID, String.valueOf(position.getDeviceId()));

            MessageEnvelope<Position> envelope = new MessageEnvelope<>(position, headers);

            Timer.Sample sample = Timer.start(meterRegistry);

            CompletableFuture<Void> future = messageProducer.sendAsync(positionsTopic, envelope);
            future.whenComplete((result, exception) -> {
                sample.stop(publishTimer);
                if (exception != null) {
                    failedCounter.increment();
                    span.recordException(exception);
                    span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, exception.getMessage());
                    LOGGER.warn("Failed to publish position for device {}: {}", 
                            position.getDeviceId(), exception.getMessage());
                } else {
                    publishedCounter.increment();
                    LOGGER.debug("Published position for device {}", position.getDeviceId());
                }
                span.end();
            });

        } catch (Exception e) {
            failedCounter.increment();
            span.recordException(e);
            span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
            span.end();
            LOGGER.error("Error publishing position for device {}", position.getDeviceId(), e);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        LOGGER.warn("Processing exception: {}", cause.getMessage());
        ctx.fireExceptionCaught(cause);
    }
}