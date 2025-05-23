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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
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
import org.traccar.model.Event;
import org.traccar.model.Position;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Orchestrates the position processing pipeline in the microservice context,
 * configuring the handler chain and integrating with the message broker for input and output.
 */
@Component
public class PositionProcessingPipeline {

    private static final Logger LOGGER = LoggerFactory.getLogger(PositionProcessingPipeline.class);

    private final List<BasePositionHandler> positionHandlers;
    private final List<BaseEventHandler> eventHandlers;
    private final PostProcessHandler postProcessHandler;
    private final EnrichedPositionProducer enrichedPositionProducer;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final Tracer tracer;
    private final MetricsCollector metricsCollector;
    
    private final Map<Long, Queue<Position>> deviceQueues = new ConcurrentHashMap<>();
    private final AtomicLong totalProcessedPositions = new AtomicLong(0);

    @Autowired
    public PositionProcessingPipeline(
            List<BasePositionHandler> positionHandlers,
            List<BaseEventHandler> eventHandlers,
            PostProcessHandler postProcessHandler,
            EnrichedPositionProducer enrichedPositionProducer,
            CircuitBreakerRegistry circuitBreakerRegistry,
            Tracer tracer,
            MetricsCollector metricsCollector) {
        this.positionHandlers = positionHandlers;
        this.eventHandlers = eventHandlers;
        this.postProcessHandler = postProcessHandler;
        this.enrichedPositionProducer = enrichedPositionProducer;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.tracer = tracer;
        this.metricsCollector = metricsCollector;
    }

    /**
     * Initializes the position processing pipeline by ordering the handlers.
     */
    @PostConstruct
    public void init() {
        // Log the configured handlers for debugging
        LOGGER.info("Initializing position processing pipeline with {} position handlers and {} event handlers",
                positionHandlers.size(), eventHandlers.size());
        
        // Verify that all required handlers are present
        verifyRequiredHandlers();
        
        // Sort handlers in the correct order
        positionHandlers.sort((h1, h2) -> {
            int index1 = getHandlerOrder(h1.getClass());
            int index2 = getHandlerOrder(h2.getClass());
            return Integer.compare(index1, index2);
        });
        
        // Log the ordered handlers for debugging
        LOGGER.info("Position processing pipeline initialized with the following handlers:");
        positionHandlers.forEach(handler -> 
            LOGGER.info("Position handler: {} (order: {})", 
                    handler.getClass().getSimpleName(), getHandlerOrder(handler.getClass())));
        eventHandlers.forEach(handler -> 
            LOGGER.info("Event handler: {}", handler.getClass().getSimpleName()));
        
        // Register health indicators for circuit breakers
        LOGGER.info("Registering circuit breakers for external service handlers");
        positionHandlers.stream()
                .filter(this::isExternalServiceHandler)
                .forEach(handler -> {
                    String handlerName = handler.getClass().getSimpleName();
                    LOGGER.info("Registered circuit breaker for {}", handlerName);
                });
    }

    /**
     * Verifies that all required handlers are present in the pipeline.
     */
    private void verifyRequiredHandlers() {
        Class<?>[] requiredHandlers = {
            ComputedAttributesHandler.Early.class,
            DatabaseHandler.class,
            PostProcessHandler.class
        };
        
        for (Class<?> handlerClass : requiredHandlers) {
            boolean found = positionHandlers.stream()
                    .anyMatch(handler -> handlerClass.isAssignableFrom(handler.getClass()));
            if (!found && !PostProcessHandler.class.equals(handlerClass)) {
                LOGGER.warn("Required handler {} not found in the pipeline", handlerClass.getSimpleName());
            }
        }
    }

    /**
     * Returns the order index for a handler class.
     */
    private int getHandlerOrder(Class<?> handlerClass) {
        Class<?>[] orderedHandlers = {
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
                DatabaseHandler.class
        };
        
        for (int i = 0; i < orderedHandlers.length; i++) {
            if (orderedHandlers[i].isAssignableFrom(handlerClass)) {
                return i;
            }
        }
        return Integer.MAX_VALUE; // Put unknown handlers at the end
    }

    /**
     * Processes a position through the handler chain.
     * 
     * @param position The position to process
     * @param correlationId The correlation ID for distributed tracing
     */
    public void processPosition(Position position, String correlationId) {
        Span span = tracer.spanBuilder("process_position")
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            metricsCollector.incrementPositionsReceived();
            long startTime = System.currentTimeMillis();
            
            // Log position receipt for debugging
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Received position from device {}: lat={}, lon={}, time={}, correlationId={}",
                        position.getDeviceId(), position.getLatitude(), position.getLongitude(), 
                        position.getFixTime(), correlationId);
            }
            
            Queue<Position> queue = getDeviceQueue(position.getDeviceId());
            boolean queued;
            synchronized (queue) {
                queued = !queue.isEmpty();
                queue.offer(position);
            }
            
            if (!queued) {
                processPositionHandlers(position, correlationId);
            }
            
            metricsCollector.recordProcessingTime(System.currentTimeMillis() - startTime);
            totalProcessedPositions.incrementAndGet();
            
            // Log processing statistics periodically
            if (totalProcessedPositions.get() % 10000 == 0) {
                LOGGER.info("Total positions processed: {}, current queue sizes: {}", 
                        totalProcessedPositions.get(), getQueueSizeInfo());
            }
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error processing position: {}", e.getMessage(), e);
            metricsCollector.incrementProcessingErrors();
        } finally {
            span.end();
        }
    }

    /**
     * Gets the queue for a device, creating it if it doesn't exist.
     */
    private Queue<Position> getDeviceQueue(long deviceId) {
        return deviceQueues.computeIfAbsent(deviceId, k -> new LinkedList<>());
    }
    
    /**
     * Gets information about the current queue sizes for logging.
     */
    private String getQueueSizeInfo() {
        StringBuilder sb = new StringBuilder("{");
        deviceQueues.forEach((deviceId, queue) -> {
            synchronized (queue) {
                if (queue.size() > 0) {
                    sb.append(deviceId).append(":")
                      .append(queue.size()).append(", ");
                }
            }
        });
        if (sb.length() > 1) {
            sb.setLength(sb.length() - 2); // Remove trailing comma and space
        }
        sb.append("}");
        return sb.toString();
    }

    /**
     * Processes a position through the position handlers chain.
     */
    private void processPositionHandlers(Position position, String correlationId) {
        Span span = tracer.spanBuilder("process_position_handlers")
                .setParent(Context.current())
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            var iterator = positionHandlers.iterator();
            if (iterator.hasNext()) {
                processNextHandler(iterator, position, correlationId, filtered -> {
                    if (filtered) {
                        span.setAttribute("filtered", true);
                        finishProcessing(position, true, correlationId);
                    }
                });
            } else {
                processEventHandlers(position, correlationId);
            }
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error in position handler chain: {}", e.getMessage(), e);
            metricsCollector.incrementHandlerErrors();
            // Continue processing even on error
            finishProcessing(position, false, correlationId);
        } finally {
            span.end();
        }
    }

    /**
     * Recursively processes the next handler in the chain.
     */
    private void processNextHandler(
            java.util.Iterator<BasePositionHandler> iterator, 
            Position position, 
            String correlationId,
            Consumer<Boolean> filterCallback) {
        
        BasePositionHandler handler = iterator.next();
        String handlerName = handler.getClass().getSimpleName();
        
        Span span = tracer.spanBuilder("handler_" + handlerName)
                .setParent(Context.current())
                .setAttribute("handler", handlerName)
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            long startTime = System.currentTimeMillis();
            
            // Apply circuit breaker if the handler interacts with external services
            if (isExternalServiceHandler(handler)) {
                CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker(handlerName);
                circuitBreaker.executeRunnable(() -> {
                    handler.handlePosition(position, filtered -> {
                        metricsCollector.recordHandlerTime(handlerName, System.currentTimeMillis() - startTime);
                        if (filtered) {
                            span.setAttribute("filtered", true);
                            filterCallback.accept(true);
                        } else if (iterator.hasNext()) {
                            processNextHandler(iterator, position, correlationId, filterCallback);
                        } else {
                            processEventHandlers(position, correlationId);
                        }
                    });
                });
            } else {
                handler.handlePosition(position, filtered -> {
                    metricsCollector.recordHandlerTime(handlerName, System.currentTimeMillis() - startTime);
                    if (filtered) {
                        span.setAttribute("filtered", true);
                        filterCallback.accept(true);
                    } else if (iterator.hasNext()) {
                        processNextHandler(iterator, position, correlationId, filterCallback);
                    } else {
                        processEventHandlers(position, correlationId);
                    }
                });
            }
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error in handler {}: {}", handlerName, e.getMessage(), e);
            metricsCollector.incrementHandlerErrors();
            
            // Continue with next handler on error
            if (iterator.hasNext()) {
                processNextHandler(iterator, position, correlationId, filterCallback);
            } else {
                processEventHandlers(position, correlationId);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Determines if a handler interacts with external services and needs circuit breaker protection.
     */
    private boolean isExternalServiceHandler(BasePositionHandler handler) {
        return handler instanceof GeolocationHandler
                || handler instanceof GeocoderHandler
                || handler instanceof SpeedLimitHandler
                || handler instanceof PositionForwardingHandler;
    }

    /**
     * Processes a position through the event handlers.
     */
    private void processEventHandlers(Position position, String correlationId) {
        Span span = tracer.spanBuilder("process_event_handlers")
                .setParent(Context.current())
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Map<Event, Position> events = new HashMap<>();
            
            for (BaseEventHandler handler : eventHandlers) {
                String handlerName = handler.getClass().getSimpleName();
                Span handlerSpan = tracer.spanBuilder("event_handler_" + handlerName)
                        .setParent(Context.current())
                        .setAttribute("handler", handlerName)
                        .setAttribute("deviceId", position.getDeviceId())
                        .setAttribute("correlationId", correlationId)
                        .startSpan();
                
                try (Scope handlerScope = handlerSpan.makeCurrent()) {
                    long startTime = System.currentTimeMillis();
                    handler.analyzePosition(position, event -> {
                        events.put(event, position);
                        handlerSpan.setAttribute("eventType", event.getType());
                    });
                    metricsCollector.recordHandlerTime(handlerName, System.currentTimeMillis() - startTime);
                } catch (Exception e) {
                    handlerSpan.recordException(e);
                    LOGGER.error("Error in event handler {}: {}", handlerName, e.getMessage(), e);
                    metricsCollector.incrementHandlerErrors();
                } finally {
                    handlerSpan.end();
                }
            }
            
            if (!events.isEmpty()) {
                span.setAttribute("eventsGenerated", events.size());
                metricsCollector.incrementEventsGenerated(events.size());
                // In microservice architecture, events are published to the message broker
                // by the Event Service, so we don't need to handle them here
            }
            
            finishProcessing(position, false, correlationId);
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error processing events: {}", e.getMessage(), e);
            metricsCollector.incrementProcessingErrors();
            finishProcessing(position, false, correlationId);
        } finally {
            span.end();
        }
    }

    /**
     * Finishes processing a position by handling post-processing and publishing to the message broker.
     */
    private void finishProcessing(Position position, boolean filtered, String correlationId) {
        Span span = tracer.spanBuilder("finish_processing")
                .setParent(Context.current())
                .setAttribute("deviceId", position.getDeviceId())
                .setAttribute("filtered", filtered)
                .setAttribute("correlationId", correlationId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            if (!filtered) {
                // Apply post-processing with circuit breaker protection for database operations
                CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("databaseOperations");
                circuitBreaker.executeRunnable(() -> {
                    postProcessHandler.handlePosition(position, ignore -> {
                        // Log successful processing
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("Successfully processed position from device {}: id={}, correlationId={}",
                                    position.getDeviceId(), position.getId(), correlationId);
                        }
                        
                        // Publish enriched position to the message broker
                        enrichedPositionProducer.publishPosition(position, correlationId);
                        metricsCollector.incrementPositionsPublished();
                        processNextPosition(position.getDeviceId(), correlationId);
                    });
                });
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Position filtered for device {}: correlationId={}",
                            position.getDeviceId(), correlationId);
                }
                metricsCollector.incrementPositionsFiltered();
                processNextPosition(position.getDeviceId(), correlationId);
            }
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error in post-processing: {}", e.getMessage(), e);
            metricsCollector.incrementProcessingErrors();
            processNextPosition(position.getDeviceId(), correlationId);
        } finally {
            span.end();
        }
    }

    /**
     * Processes the next position in the queue for a device.
     */
    private void processNextPosition(long deviceId, String correlationId) {
        Queue<Position> queue = getDeviceQueue(deviceId);
        Position nextPosition;
        synchronized (queue) {
            queue.poll(); // remove current position
            nextPosition = queue.peek();
        }
        
        if (nextPosition != null) {
            processPositionHandlers(nextPosition, correlationId);
        } else {
            // Clean up the queue if it's empty
            deviceQueues.remove(deviceId);
        }
    }
}