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
package org.traccar.sms;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.MessageProducer;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Service for handling failed message deliveries by sending them to a dead letter queue.
 * This allows for later analysis, retry, or manual intervention.
 */
@Singleton
public class DeadLetterQueueService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterQueueService.class);
    
    private final MessageProducer messageProducer;
    private final ObjectMapper objectMapper;
    
    /**
     * Creates a new DeadLetterQueueService with the specified dependencies.
     *
     * @param messageProducer The message producer for sending to the dead letter queue
     * @param objectMapper The object mapper for serializing message data
     */
    @Inject
    public DeadLetterQueueService(MessageProducer messageProducer, ObjectMapper objectMapper) {
        this.messageProducer = messageProducer;
        this.objectMapper = objectMapper;
    }
    
    /**
     * Sends a failed SMS message to the dead letter queue for later processing.
     *
     * @param channel The notification channel (e.g., "sms")
     * @param recipient The message recipient (e.g., phone number)
     * @param content The message content
     * @param correlationId The correlation ID for tracing
     * @param exception The exception that caused the failure
     */
    public void sendToDeadLetterQueue(String channel, String recipient, String content, 
                                     String correlationId, Exception exception) {
        try {
            Map<String, Object> messageData = new HashMap<>();
            messageData.put("channel", channel);
            messageData.put("recipient", recipient);
            messageData.put("content", content);
            messageData.put("correlationId", correlationId);
            messageData.put("timestamp", Instant.now().toString());
            messageData.put("errorType", exception.getClass().getName());
            messageData.put("errorMessage", exception.getMessage());
            
            String messageJson = objectMapper.writeValueAsString(messageData);
            messageProducer.sendMessage("notification.deadletter", messageJson);
            
            LOGGER.info("Message sent to dead letter queue: channel={}, correlationId={}, errorType={}", 
                    channel, correlationId, exception.getClass().getSimpleName());
        } catch (Exception e) {
            LOGGER.error("Failed to send message to dead letter queue: {}", e.getMessage(), e);
        }
    }
}