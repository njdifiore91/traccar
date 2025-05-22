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
package org.traccar.reports.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageConsumerFactory;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageHeaders;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageProducerFactory;
import org.traccar.messaging.MessageSerializer;
import org.traccar.messaging.ProtobufMessageSerializer;
import org.traccar.model.User;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributeKey;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Client for interacting with the message broker (Kafka/RabbitMQ) to publish and consume messages
 * related to report generation and notification. This component enables asynchronous report processing
 * and inter-service communication.
 */
@Singleton
public class MessageBrokerClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageBrokerClient.class);

    private static final String TOPIC_REPORT_REQUESTS = "report-requests";
    private static final String TOPIC_REPORT_COMPLETIONS = "report-completions";
    private static final String TOPIC_REPORT_NOTIFICATIONS = "report-notifications";
    private static final String CONSUMER_GROUP_REPORTS = "reporting-service";
    
    // OpenTelemetry instrumentation
    private static final String INSTRUMENTATION_SCOPE = "org.traccar.reports.common.MessageBrokerClient";
    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_SCOPE);
    private static final AttributeKey<String> REPORT_ID_KEY = AttributeKey.stringKey("report.id");
    private static final AttributeKey<String> REPORT_TYPE_KEY = AttributeKey.stringKey("report.type");
    private static final AttributeKey<Long> USER_ID_KEY = AttributeKey.longKey("user.id");
    private static final AttributeKey<String> TOPIC_KEY = AttributeKey.stringKey("messaging.topic");

    private final MessageProducer messageProducer;
    private final MessageConsumer messageConsumer;
    private final MessageSerializer messageSerializer;
    private final ExecutorService executorService;

    /**
     * Constructs a new MessageBrokerClient with the necessary dependencies.
     *
     * @param messageProducerFactory Factory for creating message producers
     * @param messageConsumerFactory Factory for creating message consumers
     */
    @Inject
    public MessageBrokerClient(
            MessageProducerFactory messageProducerFactory,
            MessageConsumerFactory messageConsumerFactory) {
        this.messageProducer = messageProducerFactory.createProducer();
        this.messageConsumer = messageConsumerFactory.createConsumer(CONSUMER_GROUP_REPORTS);
        this.messageSerializer = new ProtobufMessageSerializer();
        this.executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        
        initializeConsumers();
    }

    /**
     * Initializes message consumers for handling report generation requests.
     */
    private void initializeConsumers() {
        Span span = TRACER.spanBuilder("initialize_message_consumers")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            messageConsumer.subscribe(TOPIC_REPORT_REQUESTS, new ReportRequestHandler());
            LOGGER.info("Subscribed to {} topic", TOPIC_REPORT_REQUESTS);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize message consumers", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }

    /**
     * Publishes a report completion event to the message broker.
     *
     * @param reportId The unique identifier of the completed report
     * @param userId The user ID associated with the report
     * @param reportType The type of report that was generated
     * @param success Whether the report generation was successful
     * @param errorMessage Error message if the report generation failed, null otherwise
     * @return A CompletableFuture that completes when the message is acknowledged by the broker
     */
    public CompletableFuture<Void> publishReportCompletion(
            String reportId, long userId, String reportType, boolean success, String errorMessage) {
        
        // Create span for tracing this operation
        Span span = TRACER.spanBuilder("publish_report_completion")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(REPORT_ID_KEY, reportId)
                .setAttribute(USER_ID_KEY, userId)
                .setAttribute(REPORT_TYPE_KEY, reportType)
                .setAttribute(TOPIC_KEY, TOPIC_REPORT_COMPLETIONS)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Map<String, Object> payload = new HashMap<>();
            payload.put("reportId", reportId);
            payload.put("userId", userId);
            payload.put("reportType", reportType);
            payload.put("success", success);
            if (errorMessage != null) {
                payload.put("errorMessage", errorMessage);
                span.setAttribute("error.message", errorMessage);
            }
            
            Map<String, String> headers = new HashMap<>();
            headers.put(MessageHeaders.CORRELATION_ID, reportId);
            headers.put(MessageHeaders.MESSAGE_TYPE, "report.completion");
            headers.put(MessageHeaders.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
            
            // Add trace context to message headers for distributed tracing
            // This would typically use W3C Trace Context propagation
            // OpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), headers, (carrier, key, value) -> carrier.put(key, value));
            
            MessageEnvelope envelope = new MessageEnvelope(payload, headers);
            
            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
                messageProducer.send(TOPIC_REPORT_COMPLETIONS, envelope, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to publish report completion for report {}", reportId, exception);
                        span.recordException(exception);
                        span.setStatus(StatusCode.ERROR, exception.getMessage());
                        future.completeExceptionally(exception);
                    } else {
                        LOGGER.debug("Published report completion for report {}", reportId);
                        span.setStatus(StatusCode.OK);
                        future.complete(null);
                    }
                    span.end();
                });
            } catch (Exception e) {
                LOGGER.error("Error sending report completion message", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.end();
                future.completeExceptionally(e);
            }
            
            return future;
        }
    }

    /**
     * Publishes a notification that a report is ready to be delivered to a user.
     *
     * @param reportId The unique identifier of the completed report
     * @param userId The user ID to notify
     * @param reportType The type of report that was generated
     * @param reportData The report data as a byte array (if included in the notification)
     * @return A CompletableFuture that completes when the message is acknowledged by the broker
     */
    public CompletableFuture<Void> publishReportNotification(
            String reportId, long userId, String reportType, byte[] reportData) {
        
        // Create span for tracing this operation
        Span span = TRACER.spanBuilder("publish_report_notification")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(REPORT_ID_KEY, reportId)
                .setAttribute(USER_ID_KEY, userId)
                .setAttribute(REPORT_TYPE_KEY, reportType)
                .setAttribute(TOPIC_KEY, TOPIC_REPORT_NOTIFICATIONS)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Map<String, Object> payload = new HashMap<>();
            payload.put("reportId", reportId);
            payload.put("userId", userId);
            payload.put("reportType", reportType);
            payload.put("timestamp", System.currentTimeMillis());
            
            // Only include report data if it's small enough, otherwise just send a reference
            if (reportData != null && reportData.length <= 1024 * 1024) { // 1MB limit
                payload.put("reportData", reportData);
                payload.put("includesData", true);
                span.setAttribute("report.data.included", true);
                span.setAttribute("report.data.size", reportData.length);
            } else {
                payload.put("includesData", false);
                span.setAttribute("report.data.included", false);
                if (reportData != null) {
                    span.setAttribute("report.data.size", reportData.length);
                }
            }
            
            Map<String, String> headers = new HashMap<>();
            headers.put(MessageHeaders.CORRELATION_ID, reportId);
            headers.put(MessageHeaders.MESSAGE_TYPE, "report.notification");
            headers.put(MessageHeaders.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
            
            // Add trace context to message headers for distributed tracing
            // This would typically use W3C Trace Context propagation
            // OpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), headers, (carrier, key, value) -> carrier.put(key, value));
            
            MessageEnvelope envelope = new MessageEnvelope(payload, headers);
            
            CompletableFuture<Void> future = new CompletableFuture<>();
            try {
                messageProducer.send(TOPIC_REPORT_NOTIFICATIONS, envelope, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to publish report notification for report {}", reportId, exception);
                        span.recordException(exception);
                        span.setStatus(StatusCode.ERROR, exception.getMessage());
                        future.completeExceptionally(exception);
                    } else {
                        LOGGER.debug("Published report notification for report {}", reportId);
                        span.setStatus(StatusCode.OK);
                        future.complete(null);
                    }
                    span.end();
                });
            } catch (Exception e) {
                LOGGER.error("Error sending report notification message", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.end();
                future.completeExceptionally(e);
            }
            
            return future;
        }
    }

    /**
     * Generates a report asynchronously via the message broker and notifies the user upon completion.
     *
     * @param userId The user ID requesting the report
     * @param reportType The type of report to generate
     * @param parameters Report generation parameters
     * @return The report ID that can be used to track the request
     */
    public String generateReportAsync(long userId, String reportType, Map<String, Object> parameters) {
        String reportId = UUID.randomUUID().toString();
        
        // Create span for tracing this operation
        Span span = TRACER.spanBuilder("generate_report_async")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute(REPORT_ID_KEY, reportId)
                .setAttribute(USER_ID_KEY, userId)
                .setAttribute(REPORT_TYPE_KEY, reportType)
                .setAttribute(TOPIC_KEY, TOPIC_REPORT_REQUESTS)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Map<String, Object> payload = new HashMap<>(parameters);
            payload.put("reportId", reportId);
            payload.put("userId", userId);
            payload.put("reportType", reportType);
            payload.put("timestamp", System.currentTimeMillis());
            
            // Add parameters as span attributes for better observability
            for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                if (entry.getValue() instanceof String) {
                    span.setAttribute("report.param." + entry.getKey(), (String) entry.getValue());
                } else if (entry.getValue() instanceof Long) {
                    span.setAttribute("report.param." + entry.getKey(), (Long) entry.getValue());
                } else if (entry.getValue() instanceof Boolean) {
                    span.setAttribute("report.param." + entry.getKey(), (Boolean) entry.getValue());
                } else if (entry.getValue() instanceof Double) {
                    span.setAttribute("report.param." + entry.getKey(), (Double) entry.getValue());
                }
            }
            
            Map<String, String> headers = new HashMap<>();
            headers.put(MessageHeaders.CORRELATION_ID, reportId);
            headers.put(MessageHeaders.MESSAGE_TYPE, "report.request");
            headers.put(MessageHeaders.TIMESTAMP, String.valueOf(System.currentTimeMillis()));
            
            // Add trace context to message headers for distributed tracing
            // This would typically use W3C Trace Context propagation
            // OpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), headers, (carrier, key, value) -> carrier.put(key, value));
            
            MessageEnvelope envelope = new MessageEnvelope(payload, headers);
            
            try {
                messageProducer.send(TOPIC_REPORT_REQUESTS, envelope, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Failed to publish report request for user {}", userId, exception);
                        span.recordException(exception);
                        span.setStatus(StatusCode.ERROR, exception.getMessage());
                    } else {
                        span.setStatus(StatusCode.OK);
                    }
                    span.end();
                });
                LOGGER.info("Submitted async report request {} of type {} for user {}", reportId, reportType, userId);
            } catch (Exception e) {
                LOGGER.error("Error sending report request message", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.end();
            }
            
            return reportId;
        }
    }

    /**
     * Executes a report and publishes the result to the message broker.
     *
     * @param userId The user ID requesting the report
     * @param reportId The unique identifier for this report request
     * @param reportType The type of report to generate
     * @param executor The report executor that will generate the report
     * @param notifyUser Whether to send a notification to the user upon completion
     */
    public void executeAndPublishReport(long userId, String reportId, String reportType, 
                                       ReportExecutor executor, boolean notifyUser) {
        executorService.submit(() -> {
            // Create span for tracing this operation
            Span span = TRACER.spanBuilder("execute_and_publish_report")
                    .setSpanKind(SpanKind.INTERNAL)
                    .setAttribute(REPORT_ID_KEY, reportId)
                    .setAttribute(USER_ID_KEY, userId)
                    .setAttribute(REPORT_TYPE_KEY, reportType)
                    .setAttribute("report.notify_user", notifyUser)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                LOGGER.info("Executing report {} of type {} for user {}", reportId, reportType, userId);
                
                long startTime = System.currentTimeMillis();
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                executor.execute(outputStream);
                byte[] reportData = outputStream.toByteArray();
                long executionTime = System.currentTimeMillis() - startTime;
                
                span.setAttribute("report.execution_time_ms", executionTime);
                span.setAttribute("report.size_bytes", reportData.length);
                
                // Publish report completion event
                publishReportCompletion(reportId, userId, reportType, true, null)
                        .thenRun(() -> LOGGER.debug("Report completion published successfully"))
                        .exceptionally(ex -> {
                            LOGGER.error("Failed to publish report completion", ex);
                            return null;
                        });
                
                // Optionally notify the user
                if (notifyUser) {
                    publishReportNotification(reportId, userId, reportType, reportData)
                            .thenRun(() -> LOGGER.debug("Report notification published successfully"))
                            .exceptionally(ex -> {
                                LOGGER.error("Failed to publish report notification", ex);
                                return null;
                            });
                }
                
                LOGGER.info("Successfully executed report {} of type {} for user {} in {} ms", 
                        reportId, reportType, userId, executionTime);
                
                span.setStatus(StatusCode.OK);
            } catch (Exception e) {
                LOGGER.error("Failed to execute report {} of type {} for user {}", 
                        reportId, reportType, userId, e);
                
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                
                // Publish failure notification
                publishReportCompletion(reportId, userId, reportType, false, e.getMessage())
                        .exceptionally(ex -> {
                            LOGGER.error("Failed to publish report failure", ex);
                            return null;
                        });
            } finally {
                span.end();
            }
        });
    }

    /**
     * Handler for processing report generation requests received from the message broker.
     */
    private class ReportRequestHandler implements MessageHandler<Map<String, Object>> {
        
        @SuppressWarnings("unchecked")
        @Override
        public void onMessage(MessageEnvelope envelope) {
            // Extract correlation ID for linking with the producer span
            String correlationId = envelope.getHeaders().get(MessageHeaders.CORRELATION_ID);
            
            // Create span for processing this message
            Span span = TRACER.spanBuilder("process_report_request")
                    .setSpanKind(SpanKind.CONSUMER)
                    .setAttribute("messaging.correlation_id", correlationId != null ? correlationId : "unknown")
                    .setAttribute(TOPIC_KEY, TOPIC_REPORT_REQUESTS)
                    .startSpan();
            
            try (Scope scope = span.makeCurrent()) {
                Map<String, Object> payload = (Map<String, Object>) envelope.getPayload();
                String reportId = (String) payload.get("reportId");
                Long userId = ((Number) payload.get("userId")).longValue();
                String reportType = (String) payload.get("reportType");
                Map<String, Object> parameters = new HashMap<>();
                
                span.setAttribute(REPORT_ID_KEY, reportId);
                span.setAttribute(USER_ID_KEY, userId);
                span.setAttribute(REPORT_TYPE_KEY, reportType);
                
                // Extract report parameters from the payload
                for (Map.Entry<String, Object> entry : payload.entrySet()) {
                    String key = entry.getKey();
                    if (!key.equals("reportId") && !key.equals("userId") && 
                        !key.equals("reportType") && !key.equals("timestamp")) {
                        parameters.put(key, entry.getValue());
                        
                        // Add parameters as span attributes for better observability
                        if (entry.getValue() instanceof String) {
                            span.setAttribute("report.param." + key, (String) entry.getValue());
                        } else if (entry.getValue() instanceof Long) {
                            span.setAttribute("report.param." + key, (Long) entry.getValue());
                        } else if (entry.getValue() instanceof Boolean) {
                            span.setAttribute("report.param." + key, (Boolean) entry.getValue());
                        } else if (entry.getValue() instanceof Double) {
                            span.setAttribute("report.param." + key, (Double) entry.getValue());
                        }
                    }
                }
                
                LOGGER.info("Received report request {} of type {} for user {}", reportId, reportType, userId);
                
                // Process the report request based on the report type
                // This implementation handles common report types in the Traccar system
                span.setAttribute("report.processing.started", true);
                long startTime = System.currentTimeMillis();
                
                switch (reportType) {
                    case "trips":
                        processTripsReport(userId, reportId, parameters);
                        break;
                    case "events":
                        processEventsReport(userId, reportId, parameters);
                        break;
                    case "summary":
                        processSummaryReport(userId, reportId, parameters);
                        break;
                    case "route":
                        processRouteReport(userId, reportId, parameters);
                        break;
                    case "stops":
                        processStopsReport(userId, reportId, parameters);
                        break;
                    default:
                        LOGGER.warn("Unknown report type: {}", reportType);
                        publishReportCompletion(reportId, userId, reportType, false, "Unknown report type");
                        span.setAttribute("report.error", "Unknown report type: " + reportType);
                        span.setStatus(StatusCode.ERROR, "Unknown report type: " + reportType);
                        break;
                }
                
                long processingTime = System.currentTimeMillis() - startTime;
                span.setAttribute("report.processing.time_ms", processingTime);
                span.setStatus(StatusCode.OK);
            } catch (Exception e) {
                LOGGER.error("Error processing report request", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
            } finally {
                span.end();
            }
        }
        
        private void processTripsReport(long userId, String reportId, Map<String, Object> parameters) {
            // Implementation would create a TripsReportExecutor and call executeAndPublishReport
            LOGGER.info("Processing trips report {} for user {}", reportId, userId);
            // Example placeholder - actual implementation would use real report executors
            // TripsReportExecutor executor = new TripsReportExecutor(userId, parameters);
            // executeAndPublishReport(userId, reportId, "trips", executor, true);
            
            // For now, just log and simulate completion
            publishReportCompletion(reportId, userId, "trips", true, null);
        }
        
        private void processEventsReport(long userId, String reportId, Map<String, Object> parameters) {
            // Implementation would create an EventsReportExecutor and call executeAndPublishReport
            LOGGER.info("Processing events report {} for user {}", reportId, userId);
            // Example placeholder - actual implementation would use real report executors
            // EventsReportExecutor executor = new EventsReportExecutor(userId, parameters);
            // executeAndPublishReport(userId, reportId, "events", executor, true);
            
            // For now, just log and simulate completion
            publishReportCompletion(reportId, userId, "events", true, null);
        }
        
        private void processSummaryReport(long userId, String reportId, Map<String, Object> parameters) {
            // Implementation would create a SummaryReportExecutor and call executeAndPublishReport
            LOGGER.info("Processing summary report {} for user {}", reportId, userId);
            // Example placeholder - actual implementation would use real report executors
            // SummaryReportExecutor executor = new SummaryReportExecutor(userId, parameters);
            // executeAndPublishReport(userId, reportId, "summary", executor, true);
            
            // For now, just log and simulate completion
            publishReportCompletion(reportId, userId, "summary", true, null);
        }
        
        private void processRouteReport(long userId, String reportId, Map<String, Object> parameters) {
            // Implementation would create a RouteReportExecutor and call executeAndPublishReport
            LOGGER.info("Processing route report {} for user {}", reportId, userId);
            // Example placeholder - actual implementation would use real report executors
            // RouteReportExecutor executor = new RouteReportExecutor(userId, parameters);
            // executeAndPublishReport(userId, reportId, "route", executor, true);
            
            // For now, just log and simulate completion
            publishReportCompletion(reportId, userId, "route", true, null);
        }
        
        private void processStopsReport(long userId, String reportId, Map<String, Object> parameters) {
            // Implementation would create a StopsReportExecutor and call executeAndPublishReport
            LOGGER.info("Processing stops report {} for user {}", reportId, userId);
            // Example placeholder - actual implementation would use real report executors
            // StopsReportExecutor executor = new StopsReportExecutor(userId, parameters);
            // executeAndPublishReport(userId, reportId, "stops", executor, true);
            
            // For now, just log and simulate completion
            publishReportCompletion(reportId, userId, "stops", true, null);
        }
    }

    /**
     * Shuts down the message broker client and releases resources.
     */
    public void shutdown() {
        Span span = TRACER.spanBuilder("shutdown_message_broker_client")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            executorService.shutdown();
            messageConsumer.close();
            messageProducer.close();
            LOGGER.info("MessageBrokerClient shut down successfully");
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.error("Error shutting down MessageBrokerClient", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
}