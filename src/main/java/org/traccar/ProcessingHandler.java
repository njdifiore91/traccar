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
package org.traccar;

import com.google.inject.Injector;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
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
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.database.BufferingManager;
import org.traccar.database.NotificationManager;
import org.traccar.handler.BasePositionHandler;
import org.traccar.handler.ComputedAttributesHandler;
import org.traccar.handler.CopyAttributesHandler;
import org.traccar.handler.DatabaseHandler;
import org.traccar.handler.DistanceHandler;
import org.traccar.handler.DriverHandler;
import org.traccar.handler.EngineHoursHandler;
import org.traccar.handler.FilterHandler;
import org.traccar.handler.GeocoderHandler;
import org.traccar.handler.GeofenceHandler;
import org.traccar.handler.GeolocationHandler;
import org.traccar.handler.HemisphereHandler;
import org.traccar.handler.MotionHandler;
import org.traccar.handler.OutdatedHandler;
import org.traccar.handler.PositionForwardingHandler;
import org.traccar.handler.PostProcessHandler;
import org.traccar.handler.SpeedLimitHandler;
import org.traccar.handler.TimeHandler;
import org.traccar.handler.events.AlarmEventHandler;
import org.traccar.handler.events.BaseEventHandler;
import org.traccar.handler.events.BehaviorEventHandler;
import org.traccar.handler.events.CommandResultEventHandler;
import org.traccar.handler.events.DriverEventHandler;
import org.traccar.handler.events.FuelEventHandler;
import org.traccar.handler.events.GeofenceEventHandler;
import org.traccar.handler.events.IgnitionEventHandler;
import org.traccar.handler.events.MaintenanceEventHandler;
import org.traccar.handler.events.MediaEventHandler;
import org.traccar.handler.events.MotionEventHandler;
import org.traccar.handler.events.OverspeedEventHandler;
import org.traccar.handler.network.AcknowledgementHandler;
import org.traccar.helper.PositionLogger;
import org.traccar.messaging.MessageBrokerClient;
import org.traccar.messaging.MessageBrokerClientFactory;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.telemetry.MetricsProvider;
import org.traccar.telemetry.TracingProvider;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

@Singleton
@ChannelHandler.Sharable
public class ProcessingHandler extends ChannelInboundHandlerAdapter implements BufferingManager.Callback {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingHandler.class);

    private final CacheManager cacheManager;
    private final NotificationManager notificationManager;
    private final PositionLogger positionLogger;
    private final BufferingManager bufferingManager;
    private final List<BasePositionHandler> positionHandlers;
    private final List<BaseEventHandler> eventHandlers;
    private final PostProcessHandler postProcessHandler;
    
    // Message broker integration
    private final MessageBrokerClient messageBrokerClient;
    private final boolean useMessageBroker;
    private final String positionsTopic;
    
    // Distributed tracing
    private final Tracer tracer;
    
    // Metrics collection
    private final LongCounter positionsProcessedCounter;
    private final LongCounter positionsFilteredCounter;
    private final LongCounter positionsPublishedCounter;
    private final LongCounter processingErrorsCounter;
    
    // Circuit breakers
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final CircuitBreaker geocoderCircuitBreaker;
    private final CircuitBreaker forwardingCircuitBreaker;
    private final CircuitBreaker messageBrokerCircuitBreaker;

    private final Map<Long, Queue<Position>> queues = new HashMap<>();

    private synchronized Queue<Position> getQueue(long deviceId) {
        return queues.computeIfAbsent(deviceId, k -> new LinkedList<>());
    }

    @Inject
    public ProcessingHandler(
            Injector injector, Config config,
            CacheManager cacheManager, NotificationManager notificationManager, 
            PositionLogger positionLogger, TracingProvider tracingProvider,
            MetricsProvider metricsProvider, MessageBrokerClientFactory messageBrokerFactory) {
        this.cacheManager = cacheManager;
        this.notificationManager = notificationManager;
        this.positionLogger = positionLogger;
        bufferingManager = new BufferingManager(config, this);
        
        // Initialize distributed tracing
        this.tracer = tracingProvider.getTracer("org.traccar.processing");
        
        // Initialize metrics
        Meter meter = metricsProvider.getMeter("org.traccar.processing");
        this.positionsProcessedCounter = meter.counterBuilder("positions.processed")
                .setDescription("Number of positions processed")
                .build();
        this.positionsFilteredCounter = meter.counterBuilder("positions.filtered")
                .setDescription("Number of positions filtered out")
                .build();
        this.positionsPublishedCounter = meter.counterBuilder("positions.published")
                .setDescription("Number of positions published to message broker")
                .build();
        this.processingErrorsCounter = meter.counterBuilder("processing.errors")
                .setDescription("Number of errors during position processing")
                .build();
        
        // Initialize circuit breakers
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slowCallRateThreshold(50)
                .slowCallDurationThreshold(Duration.ofSeconds(2))
                .permittedNumberOfCallsInHalfOpenState(10)
                .minimumNumberOfCalls(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .recordExceptions(Exception.class)
                .build();
        
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.geocoderCircuitBreaker = circuitBreakerRegistry.circuitBreaker("geocoder");
        this.forwardingCircuitBreaker = circuitBreakerRegistry.circuitBreaker("forwarding");
        this.messageBrokerCircuitBreaker = circuitBreakerRegistry.circuitBreaker("messageBroker");
        
        // Initialize message broker client
        this.useMessageBroker = config.getBoolean(Keys.PROCESSING_REMOTE_ENABLED.getKey());
        this.positionsTopic = config.getString(Keys.PROCESSING_REMOTE_POSITIONS_TOPIC.getKey(), "positions");
        
        if (useMessageBroker) {
            this.messageBrokerClient = messageBrokerFactory.create();
            LOGGER.info("Message broker integration enabled, publishing to topic: {}", positionsTopic);
        } else {
            this.messageBrokerClient = null;
            LOGGER.info("Message broker integration disabled, using direct processing");
        }

        positionHandlers = Stream.of(
                ComputedAttributesHandler.Early.class,
                OutdatedHandler.class,
                TimeHandler.class,
                GeolocationHandler.class,
                HemisphereHandler.class,
                DistanceHandler.class,
                FilterHandler.class,
                GeofenceHandler.class,
                GeocoderHandler.class,
                SpeedLimitHandler.class,
                MotionHandler.class,
                ComputedAttributesHandler.Late.class,
                EngineHoursHandler.class,
                DriverHandler.class,
                CopyAttributesHandler.class,
                PositionForwardingHandler.class,
                DatabaseHandler.class)
                .map((clazz) -> (BasePositionHandler) injector.getInstance(clazz))
                .filter(Objects::nonNull)
                .toList();

        eventHandlers = Stream.of(
                MediaEventHandler.class,
                CommandResultEventHandler.class,
                OverspeedEventHandler.class,
                BehaviorEventHandler.class,
                FuelEventHandler.class,
                MotionEventHandler.class,
                GeofenceEventHandler.class,
                AlarmEventHandler.class,
                IgnitionEventHandler.class,
                MaintenanceEventHandler.class,
                DriverEventHandler.class)
                .map((clazz) -> (BaseEventHandler) injector.getInstance(clazz))
                .filter(Objects::nonNull)
                .toList();

        postProcessHandler = injector.getInstance(PostProcessHandler.class);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Position position) {
            // Create a new span for position processing
            Span span = tracer.spanBuilder("process_position")
                    .setSpanKind(SpanKind.CONSUMER)
                    .setAttribute("deviceId", position.getDeviceId())
                    .setAttribute("protocol", position.getProtocol())
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                bufferingManager.accept(ctx, position);
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                processingErrorsCounter.add(1);
                throw e;
            } finally {
                span.end();
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void onReleased(ChannelHandlerContext context, Position position) {
        // Create a span for position handling
        Span span = tracer.spanBuilder("handle_position")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("protocol", position.getProtocol())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Queue<Position> queue = getQueue(position.getDeviceId());
            boolean queued;
            synchronized (queue) {
                queued = !queue.isEmpty();
                queue.offer(position);
            }
            if (!queued) {
                try {
                    cacheManager.addDevice(position.getDeviceId(), position.getDeviceId());
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    processingErrorsCounter.add(1);
                    throw new RuntimeException(e);
                }
                
                // Determine if we should use message broker or direct processing
                if (useMessageBroker && messageBrokerClient != null) {
                    publishPositionToMessageBroker(context, position, span);
                } else {
                    processPositionHandlers(context, position, span);
                }
            }
        } finally {
            span.end();
        }
    }
    
    private void publishPositionToMessageBroker(ChannelHandlerContext ctx, Position position, Span parentSpan) {
        Span span = tracer.spanBuilder("publish_position_to_broker")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("topic", positionsTopic)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Use circuit breaker for message broker publishing
            Supplier<CompletableFuture<Void>> publishSupplier = () -> {
                return messageBrokerClient.publish(positionsTopic, position)
                        .thenApply(result -> {
                            positionsPublishedCounter.add(1);
                            ctx.writeAndFlush(new AcknowledgementHandler.EventHandled(position));
                            processNextPosition(ctx, position.getDeviceId());
                            return null;
                        });
            };
            
            // Execute with circuit breaker
            try {
                messageBrokerCircuitBreaker.executeCompletionStage(publishSupplier)
                        .exceptionally(e -> {
                            LOGGER.warn("Failed to publish position to message broker, falling back to direct processing", e);
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, "Message broker publishing failed: " + e.getMessage());
                            processingErrorsCounter.add(1);
                            
                            // Fallback to direct processing
                            processPositionHandlers(ctx, position, parentSpan);
                            return null;
                        }).toCompletableFuture().orTimeout(5, TimeUnit.SECONDS)
                        .exceptionally(e -> {
                            LOGGER.error("Timeout publishing position to message broker", e);
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, "Message broker publishing timed out: " + e.getMessage());
                            processingErrorsCounter.add(1);
                            
                            // Fallback to direct processing
                            processPositionHandlers(ctx, position, parentSpan);
                            return null;
                        });
            } catch (Exception e) {
                LOGGER.error("Error executing circuit breaker for message broker", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                processingErrorsCounter.add(1);
                
                // Fallback to direct processing
                processPositionHandlers(ctx, position, parentSpan);
            }
        } finally {
            span.end();
        }
    }

    private void processPositionHandlers(ChannelHandlerContext ctx, Position position, Span parentSpan) {
        Span span = tracer.spanBuilder("process_position_handlers")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", position.getDeviceId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            var iterator = positionHandlers.iterator();
            processNextHandler(ctx, position, iterator, span);
        } finally {
            span.end();
        }
    }
    
    private void processNextHandler(ChannelHandlerContext ctx, Position position, 
                                   java.util.Iterator<BasePositionHandler> iterator, Span parentSpan) {
        if (!iterator.hasNext()) {
            processEventHandlers(ctx, position, parentSpan);
            return;
        }
        
        BasePositionHandler handler = iterator.next();
        String handlerName = handler.getClass().getSimpleName();
        
        Span span = tracer.spanBuilder("handler_" + handlerName)
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("handler", handlerName)
                .setAttribute("deviceId", position.getDeviceId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Apply circuit breaker for specific handlers that make external calls
            if (handler instanceof GeocoderHandler) {
                processHandlerWithCircuitBreaker(geocoderCircuitBreaker, handler, position, new BasePositionHandler.Callback() {
                    @Override
                    public void processed(boolean filtered) {
                        span.setAttribute("filtered", filtered);
                        if (filtered) {
                            positionsFilteredCounter.add(1);
                            finishedProcessing(ctx, position, true, parentSpan);
                        } else {
                            processNextHandler(ctx, position, iterator, parentSpan);
                        }
                    }
                });
            } else if (handler instanceof PositionForwardingHandler) {
                processHandlerWithCircuitBreaker(forwardingCircuitBreaker, handler, position, new BasePositionHandler.Callback() {
                    @Override
                    public void processed(boolean filtered) {
                        span.setAttribute("filtered", filtered);
                        if (filtered) {
                            positionsFilteredCounter.add(1);
                            finishedProcessing(ctx, position, true, parentSpan);
                        } else {
                            processNextHandler(ctx, position, iterator, parentSpan);
                        }
                    }
                });
            } else {
                // Regular handler processing without circuit breaker
                handler.handlePosition(position, new BasePositionHandler.Callback() {
                    @Override
                    public void processed(boolean filtered) {
                        span.setAttribute("filtered", filtered);
                        if (filtered) {
                            positionsFilteredCounter.add(1);
                            finishedProcessing(ctx, position, true, parentSpan);
                        } else {
                            processNextHandler(ctx, position, iterator, parentSpan);
                        }
                    }
                });
            }
        } catch (Exception e) {
            LOGGER.error("Error processing handler {}", handlerName, e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            processingErrorsCounter.add(1);
            
            // Continue with next handler despite the error for graceful degradation
            processNextHandler(ctx, position, iterator, parentSpan);
        } finally {
            span.end();
        }
    }
    
    private void processHandlerWithCircuitBreaker(CircuitBreaker circuitBreaker, 
                                                BasePositionHandler handler, 
                                                Position position, 
                                                BasePositionHandler.Callback callback) {
        try {
            circuitBreaker.executeSupplier(() -> {
                handler.handlePosition(position, callback);
                return true;
            });
        } catch (Exception e) {
            LOGGER.warn("Circuit breaker prevented call to handler {}, using fallback", 
                    handler.getClass().getSimpleName(), e);
            // Fallback: continue processing without this handler
            callback.processed(false);
        }
    }

    private void processEventHandlers(ChannelHandlerContext ctx, Position position, Span parentSpan) {
        Span span = tracer.spanBuilder("process_event_handlers")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", position.getDeviceId())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            eventHandlers.forEach(handler -> {
                String handlerName = handler.getClass().getSimpleName();
                Span eventSpan = tracer.spanBuilder("event_" + handlerName)
                        .setParent(Context.current())
                        .setSpanKind(SpanKind.INTERNAL)
                        .setAttribute("handler", handlerName)
                        .setAttribute("deviceId", position.getDeviceId())
                        .startSpan();
                
                try (Scope eventScope = eventSpan.makeCurrent()) {
                    handler.analyzePosition(position, (event) -> {
                        eventSpan.setAttribute("eventType", event.getType());
                        notificationManager.updateEvents(Map.of(event, position));
                    });
                } catch (Exception e) {
                    LOGGER.error("Error in event handler {}", handlerName, e);
                    eventSpan.recordException(e);
                    eventSpan.setStatus(StatusCode.ERROR, e.getMessage());
                    processingErrorsCounter.add(1);
                } finally {
                    eventSpan.end();
                }
            });
            
            finishedProcessing(ctx, position, false, parentSpan);
        } finally {
            span.end();
        }
    }

    private void finishedProcessing(ChannelHandlerContext ctx, Position position, boolean filtered, Span parentSpan) {
        Span span = tracer.spanBuilder("finish_processing")
                .setParent(Context.current().with(parentSpan))
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("filtered", filtered)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            if (!filtered) {
                positionsProcessedCounter.add(1);
                postProcessHandler.handlePosition(position, ignore -> {
                    positionLogger.log(ctx, position);
                    ctx.writeAndFlush(new AcknowledgementHandler.EventHandled(position));
                    processNextPosition(ctx, position.getDeviceId());
                });
            } else {
                ctx.writeAndFlush(new AcknowledgementHandler.EventHandled(position));
                processNextPosition(ctx, position.getDeviceId());
            }
        } finally {
            span.end();
        }
    }

    private void processNextPosition(ChannelHandlerContext ctx, long deviceId) {
        Queue<Position> queue = getQueue(deviceId);
        Position nextPosition;
        synchronized (queue) {
            queue.poll(); // remove current position
            nextPosition = queue.peek();
        }
        if (nextPosition != null) {
            // Create a new span for the next position
            Span span = tracer.spanBuilder("process_next_position")
                    .setSpanKind(SpanKind.INTERNAL)
                    .setAttribute("deviceId", deviceId)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                if (useMessageBroker && messageBrokerClient != null) {
                    publishPositionToMessageBroker(ctx, nextPosition, span);
                } else {
                    processPositionHandlers(ctx, nextPosition, span);
                }
            } finally {
                span.end();
            }
        } else {
            cacheManager.removeDevice(deviceId, deviceId);
        }
    }
}