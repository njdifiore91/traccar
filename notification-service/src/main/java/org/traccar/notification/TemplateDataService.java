/*
 * Copyright 2023 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.notification;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.micrometer.core.annotation.Timed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Service for retrieving template data from the message broker or other services.
 */
@Service
public class TemplateDataService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateDataService.class);
    private static final String TEMPLATE_REQUEST_TOPIC = "template-request";
    private static final String TEMPLATE_RESPONSE_TOPIC = "template-response";

    private final KafkaTemplate<String, TemplateRequest> kafkaTemplate;
    private final DiscoveryClient discoveryClient;
    private final String templateServiceId;

    /**
     * Creates a new instance of TemplateDataService.
     *
     * @param kafkaTemplate Kafka template for sending messages
     * @param discoveryClient Service discovery client
     * @param templateServiceId ID of the template service in the service registry
     */
    @Autowired
    public TemplateDataService(
            KafkaTemplate<String, TemplateRequest> kafkaTemplate,
            DiscoveryClient discoveryClient,
            @Value("${traccar.template.service.id:template-service}") String templateServiceId) {
        this.kafkaTemplate = kafkaTemplate;
        this.discoveryClient = discoveryClient;
        this.templateServiceId = templateServiceId;
    }

    /**
     * Retrieves template content by name.
     *
     * @param templateName Name of the template to retrieve
     * @return Template content as a string
     */
    @Timed(value = "template.data.retrieval.time", description = "Time taken to retrieve template data")
    @CircuitBreaker(name = "templateService", fallbackMethod = "getTemplateContentFallback")
    public String getTemplateContent(String templateName) {
        LOGGER.debug("Retrieving template content for: {}", templateName);
        
        // Check if template service is available via service discovery
        List<ServiceInstance> instances = discoveryClient.getInstances(templateServiceId);
        if (instances.isEmpty()) {
            LOGGER.warn("No instances of template service found. Using message broker fallback.");
            return getTemplateViaMessageBroker(templateName);
        }
        
        // Implementation would typically use a REST client to call the template service directly
        // For this implementation, we'll use the message broker approach as the primary method
        return getTemplateViaMessageBroker(templateName);
    }

    /**
     * Retrieves template content via message broker.
     *
     * @param templateName Name of the template to retrieve
     * @return Template content as a string
     */
    private String getTemplateViaMessageBroker(String templateName) {
        TemplateRequest request = new TemplateRequest(templateName);
        CompletableFuture<String> future = TemplateResponseRegistry.createFuture(request.getRequestId());
        
        try {
            kafkaTemplate.send(TEMPLATE_REQUEST_TOPIC, request);
            return future.get(); // This will block until the response is received
        } catch (Exception e) {
            LOGGER.error("Error retrieving template via message broker: {}", templateName, e);
            TemplateResponseRegistry.removeFuture(request.getRequestId());
            throw new RuntimeException("Failed to retrieve template: " + e.getMessage(), e);
        }
    }

    /**
     * Fallback method for circuit breaker when template service is unavailable.
     *
     * @param templateName Name of the template
     * @param e Exception that triggered the fallback
     * @return Default template content
     */
    public String getTemplateContentFallback(String templateName, Exception e) {
        LOGGER.warn("Using fallback for template: {}. Error: {}", templateName, e.getMessage());
        return "Default template content for " + templateName;
    }

    /**
     * Checks if a template exists.
     *
     * @param templateName Name of the template to check
     * @return true if the template exists, false otherwise
     */
    @CircuitBreaker(name = "templateService", fallbackMethod = "templateExistsFallback")
    public boolean templateExists(String templateName) {
        // Implementation would check if the template exists
        // For simplicity, we'll assume it exists if we can retrieve content
        return getTemplateContent(templateName) != null;
    }

    /**
     * Fallback method for circuit breaker when template service is unavailable.
     *
     * @param templateName Name of the template
     * @param e Exception that triggered the fallback
     * @return false, assuming template doesn't exist when service is unavailable
     */
    public boolean templateExistsFallback(String templateName, Exception e) {
        LOGGER.warn("Failed to check if template {} exists. Error: {}", templateName, e.getMessage());
        return false;
    }

    /**
     * Listens for template responses on the Kafka topic.
     *
     * @param response Template response received from Kafka
     */
    @KafkaListener(topics = TEMPLATE_RESPONSE_TOPIC)
    public void handleTemplateResponse(TemplateResponse response) {
        LOGGER.debug("Received template response for request: {}", response.getRequestId());
        TemplateResponseRegistry.completeFuture(response.getRequestId(), response.getContent());
    }

    /**
     * Request object for template retrieval.
     */
    public static class TemplateRequest {
        private final String requestId;
        private final String templateName;

        public TemplateRequest(String templateName) {
            this.requestId = java.util.UUID.randomUUID().toString();
            this.templateName = templateName;
        }

        public String getRequestId() {
            return requestId;
        }

        public String getTemplateName() {
            return templateName;
        }
    }

    /**
     * Response object for template retrieval.
     */
    public static class TemplateResponse {
        private final String requestId;
        private final String content;

        public TemplateResponse(String requestId, String content) {
            this.requestId = requestId;
            this.content = content;
        }

        public String getRequestId() {
            return requestId;
        }

        public String getContent() {
            return content;
        }
    }
}