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

import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.security.PermissionsService;
import org.traccar.mail.MailManager;
import org.traccar.model.User;
import org.traccar.storage.StorageException;
import org.traccar.storage.ObjectStorage;

import jakarta.activation.DataHandler;
import jakarta.inject.Inject;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeBodyPart;
import jakarta.mail.util.ByteArrayDataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ReportMailer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReportMailer.class);
    private static final String NOTIFICATION_TOPIC = "report-notifications";
    private static final String OBJECT_STORAGE_PREFIX = "reports/";
    private static final String RETRY_NAME = "reportMailer";

    private final PermissionsService permissionsService;
    private final MailManager mailManager;
    private final KafkaProducer<String, Map<String, Object>> kafkaProducer;
    private final ObjectStorage objectStorage;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final Retry retry;
    private final Timer reportGenerationTimer;
    private final Timer reportDeliveryTimer;
    private final Counter reportSuccessCounter;
    private final Counter reportFailureCounter;

    @Inject
    public ReportMailer(
            PermissionsService permissionsService,
            MailManager mailManager,
            KafkaProducer<String, Map<String, Object>> kafkaProducer,
            ObjectStorage objectStorage,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.permissionsService = permissionsService;
        this.mailManager = mailManager;
        this.kafkaProducer = kafkaProducer;
        this.objectStorage = objectStorage;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Configure retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(5)
                .waitDuration(Duration.ofSeconds(1))
                .retryExceptions(StorageException.class, IOException.class, MessagingException.class)
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofMinutes(1), 2.0)
                .build();
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        this.retry = retryRegistry.retry(RETRY_NAME);
        
        // Initialize metrics
        this.reportGenerationTimer = Timer.builder("report.generation.time")
                .description("Time taken to generate reports")
                .register(meterRegistry);
        this.reportDeliveryTimer = Timer.builder("report.delivery.time")
                .description("Time taken to deliver reports")
                .register(meterRegistry);
        this.reportSuccessCounter = Counter.builder("report.delivery.success")
                .description("Number of successfully delivered reports")
                .register(meterRegistry);
        this.reportFailureCounter = Counter.builder("report.delivery.failure")
                .description("Number of failed report deliveries")
                .register(meterRegistry);
    }

    public void sendAsync(long userId, ReportExecutor executor) {
        // Create a span for the entire report generation and delivery process
        Span span = tracer.spanBuilder("report.generate.and.send")
                .setSpanKind(SpanKind.PRODUCER)
                .setAttribute("userId", userId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Generate a unique ID for this report
            String reportId = UUID.randomUUID().toString();
            span.setAttribute("reportId", reportId);
            
            // Use CompletableFuture to handle the async operation
            CompletableFuture.runAsync(() -> {
                // Create a child span for report generation
                Span generationSpan = tracer.spanBuilder("report.generate")
                        .setParent(Context.current().with(span))
                        .setAttribute("reportId", reportId)
                        .setAttribute("userId", userId)
                        .startSpan();
                
                try (Scope generationScope = generationSpan.makeCurrent()) {
                    // Wrap the report generation in a timer and retry mechanism
                    ByteArrayOutputStream stream = reportGenerationTimer.record(() -> {
                        try {
                            return retry.executeSupplier(() -> {
                                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                                try {
                                    executor.execute(outputStream);
                                    return outputStream;
                                } catch (Exception e) {
                                    generationSpan.recordException(e);
                                    generationSpan.setStatus(StatusCode.ERROR, "Report generation failed: " + e.getMessage());
                                    throw new RuntimeException("Failed to generate report", e);
                                }
                            });
                        } catch (Exception e) {
                            reportFailureCounter.increment();
                            generationSpan.recordException(e);
                            generationSpan.setStatus(StatusCode.ERROR, "Report generation failed after retries: " + e.getMessage());
                            LOGGER.error("Report generation failed after retries", e);
                            throw new RuntimeException(e);
                        }
                    });
                    
                    generationSpan.setStatus(StatusCode.OK);
                    generationSpan.end();
                    
                    // Create a child span for storing the report in object storage
                    Span storageSpan = tracer.spanBuilder("report.store")
                            .setParent(Context.current().with(span))
                            .setAttribute("reportId", reportId)
                            .setAttribute("userId", userId)
                            .startSpan();
                    
                    String objectKey = null;
                    try (Scope storageScope = storageSpan.makeCurrent()) {
                        // Store the report in object storage
                        objectKey = OBJECT_STORAGE_PREFIX + reportId + ".xlsx";
                        storageSpan.setAttribute("objectKey", objectKey);
                        
                        retry.executeRunnable(() -> {
                            try {
                                objectStorage.store(objectKey, stream.toByteArray(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
                            } catch (Exception e) {
                                storageSpan.recordException(e);
                                storageSpan.setStatus(StatusCode.ERROR, "Failed to store report: " + e.getMessage());
                                throw new RuntimeException("Failed to store report in object storage", e);
                            }
                        });
                        
                        storageSpan.setStatus(StatusCode.OK);
                    } catch (Exception e) {
                        reportFailureCounter.increment();
                        storageSpan.recordException(e);
                        storageSpan.setStatus(StatusCode.ERROR, "Failed to store report after retries: " + e.getMessage());
                        LOGGER.error("Failed to store report in object storage after retries", e);
                        throw new RuntimeException(e);
                    } finally {
                        storageSpan.end();
                    }
                    
                    // Create a child span for sending the notification
                    Span notificationSpan = tracer.spanBuilder("report.notify")
                            .setParent(Context.current().with(span))
                            .setAttribute("reportId", reportId)
                            .setAttribute("userId", userId)
                            .startSpan();
                    
                    try (Scope notificationScope = notificationSpan.makeCurrent()) {
                        // Send notification via message broker to Notification Service
                        User user = permissionsService.getUser(userId);
                        notificationSpan.setAttribute("userEmail", user.getEmail());
                        
                        // Prepare notification data
                        Map<String, Object> notificationData = new HashMap<>();
                        notificationData.put("type", "report");
                        notificationData.put("userId", userId);
                        notificationData.put("reportId", reportId);
                        notificationData.put("objectKey", objectKey);
                        notificationData.put("timestamp", System.currentTimeMillis());
                        notificationData.put("template", "report_ready");
                        notificationData.put("templateData", Map.of(
                                "userName", user.getName(),
                                "reportType", "Excel Report",
                                "reportId", reportId
                        ));
                        
                        // Record the delivery time
                        reportDeliveryTimer.record(() -> {
                            try {
                                retry.executeRunnable(() -> {
                                    try {
                                        // Send to Kafka topic
                                        ProducerRecord<String, Map<String, Object>> record = 
                                                new ProducerRecord<>(NOTIFICATION_TOPIC, reportId, notificationData);
                                        
                                        // Add tracing headers to the Kafka record
                                        Span.current().setAttribute("messaging.system", "kafka");
                                        Span.current().setAttribute("messaging.destination", NOTIFICATION_TOPIC);
                                        Span.current().setAttribute("messaging.destination_kind", "topic");
                                        
                                        kafkaProducer.send(record, (metadata, exception) -> {
                                            if (exception != null) {
                                                notificationSpan.recordException(exception);
                                                notificationSpan.setStatus(StatusCode.ERROR, "Failed to send notification: " + exception.getMessage());
                                                LOGGER.error("Failed to send notification to Kafka", exception);
                                                reportFailureCounter.increment();
                                            } else {
                                                notificationSpan.setStatus(StatusCode.OK);
                                                reportSuccessCounter.increment();
                                                LOGGER.info("Report notification sent successfully to topic {} with reportId {}", 
                                                        NOTIFICATION_TOPIC, reportId);
                                            }
                                        });
                                    } catch (Exception e) {
                                        notificationSpan.recordException(e);
                                        notificationSpan.setStatus(StatusCode.ERROR, "Failed to send notification: " + e.getMessage());
                                        throw new RuntimeException("Failed to send notification", e);
                                    }
                                });
                            } catch (Exception e) {
                                reportFailureCounter.increment();
                                notificationSpan.recordException(e);
                                notificationSpan.setStatus(StatusCode.ERROR, "Failed to send notification after retries: " + e.getMessage());
                                LOGGER.error("Failed to send notification after retries", e);
                                
                                // Fallback to direct email sending if notification service integration fails
                                sendDirectEmail(userId, stream, reportId);
                            }
                        });
                    } finally {
                        notificationSpan.end();
                    }
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, "Report processing failed: " + e.getMessage());
                    LOGGER.error("Report processing failed", e);
                } finally {
                    span.end();
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.end();
            LOGGER.error("Failed to initiate report generation", e);
        }
    }
    
    /**
     * Fallback method to send email directly if notification service integration fails
     */
    private void sendDirectEmail(long userId, ByteArrayOutputStream stream, String reportId) {
        Span span = tracer.spanBuilder("report.email.fallback")
                .setAttribute("reportId", reportId)
                .setAttribute("userId", userId)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            retry.executeRunnable(() -> {
                try {
                    MimeBodyPart attachment = new MimeBodyPart();
                    attachment.setFileName("report.xlsx");
                    attachment.setDataHandler(new DataHandler(new ByteArrayDataSource(
                            stream.toByteArray(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")));

                    User user = permissionsService.getUser(userId);
                    mailManager.sendMessage(user, false, "Report Ready", 
                            "Your requested report is attached to this email.", attachment);
                    
                    reportSuccessCounter.increment();
                    LOGGER.info("Report sent directly via email as fallback for reportId {}", reportId);
                } catch (StorageException | IOException | MessagingException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, "Email fallback failed: " + e.getMessage());
                    reportFailureCounter.increment();
                    LOGGER.error("Email report fallback failed", e);
                    throw new RuntimeException("Email fallback failed", e);
                }
            });
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Email fallback failed after retries: " + e.getMessage());
            LOGGER.error("Email report fallback failed after retries", e);
        } finally {
            span.end();
        }
    }
}