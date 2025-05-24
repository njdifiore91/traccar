/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.service;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.client.RestTemplate;
import org.traccar.discovery.ServiceDiscovery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the WebSocketService interface that communicates with the API Gateway
 * to discover and interact with WebSocket sessions.
 */
@Singleton
public class WebSocketServiceImpl implements WebSocketService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServiceImpl.class);

    private final RestTemplate restTemplate;
    private final ServiceDiscovery serviceDiscovery;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    
    @Value("${kafka.topic.websocket-command}")
    private String websocketCommandTopic;

    @Inject
    public WebSocketServiceImpl(
            RestTemplate restTemplate,
            ServiceDiscovery serviceDiscovery,
            KafkaTemplate<String, Object> kafkaTemplate) {
        this.restTemplate = restTemplate;
        this.serviceDiscovery = serviceDiscovery;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Checks if a user has an active WebSocket session by querying the API Gateway.
     *
     * @param userId The ID of the user to check
     * @return true if the user has at least one active session, false otherwise
     */
    @Override
    @CircuitBreaker(name = "apiGateway", fallbackMethod = "fallbackHasActiveSession")
    @Retry(name = "apiGateway")
    public boolean hasActiveSession(long userId) {
        try {
            String apiGatewayUrl = serviceDiscovery.getServiceUrl("api-gateway");
            String url = apiGatewayUrl + "/api/v1/websocket/sessions/user/" + userId + "/exists";
            
            ResponseEntity<Boolean> response = restTemplate.getForEntity(url, Boolean.class);
            return Boolean.TRUE.equals(response.getBody());
        } catch (Exception e) {
            LOGGER.error("Error checking active WebSocket session for user {}: {}", userId, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Fallback method for hasActiveSession when the circuit breaker is triggered.
     *
     * @param userId The ID of the user to check
     * @param e The exception that triggered the circuit breaker
     * @return false, assuming no active session when the API Gateway is unavailable
     */
    public boolean fallbackHasActiveSession(long userId, Exception e) {
        LOGGER.warn("Using fallback for hasActiveSession (user {}): {}", userId, e.getMessage());
        return false; // Assume no active session when API Gateway is unavailable
    }

    /**
     * Sends a notification to a specific user through their WebSocket connection(s).
     * Uses the message broker to publish a command that the API Gateway will consume.
     *
     * @param userId The ID of the user to send the notification to
     * @param notification The formatted notification message as a JSON string
     * @return true if the notification was sent to the message broker successfully
     */
    @Override
    @CircuitBreaker(name = "messageBroker", fallbackMethod = "fallbackSendNotification")
    public boolean sendNotification(long userId, String notification) {
        try {
            Map<String, Object> command = new HashMap<>();
            command.put("type", "notification");
            command.put("userId", userId);
            command.put("payload", notification);
            
            kafkaTemplate.send(websocketCommandTopic, String.valueOf(userId), command);
            LOGGER.debug("Sent notification command to message broker for user {}", userId);
            return true;
        } catch (Exception e) {
            LOGGER.error("Error sending notification to WebSocket for user {}: {}", userId, e.getMessage(), e);
            return false;
        }
    }

    /**
     * Fallback method for sendNotification when the circuit breaker is triggered.
     *
     * @param userId The ID of the user to send the notification to
     * @param notification The formatted notification message
     * @param e The exception that triggered the circuit breaker
     * @return false, indicating the notification could not be sent
     */
    public boolean fallbackSendNotification(long userId, String notification, Exception e) {
        LOGGER.warn("Using fallback for sendNotification (user {}): {}", userId, e.getMessage());
        return false;
    }

    /**
     * Gets the count of active WebSocket sessions for a specific user by querying the API Gateway.
     *
     * @param userId The ID of the user to check
     * @return The number of active sessions for the user
     */
    @Override
    @CircuitBreaker(name = "apiGateway", fallbackMethod = "fallbackGetActiveSessionCount")
    @Retry(name = "apiGateway")
    public int getActiveSessionCount(long userId) {
        try {
            String apiGatewayUrl = serviceDiscovery.getServiceUrl("api-gateway");
            String url = apiGatewayUrl + "/api/v1/websocket/sessions/user/" + userId + "/count";
            
            ResponseEntity<Integer> response = restTemplate.getForEntity(url, Integer.class);
            return response.getBody() != null ? response.getBody() : 0;
        } catch (Exception e) {
            LOGGER.error("Error getting active WebSocket session count for user {}: {}", userId, e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Fallback method for getActiveSessionCount when the circuit breaker is triggered.
     *
     * @param userId The ID of the user to check
     * @param e The exception that triggered the circuit breaker
     * @return 0, assuming no active sessions when the API Gateway is unavailable
     */
    public int fallbackGetActiveSessionCount(long userId, Exception e) {
        LOGGER.warn("Using fallback for getActiveSessionCount (user {}): {}", userId, e.getMessage());
        return 0;
    }

    /**
     * Gets the total count of active WebSocket sessions across all users by querying the API Gateway.
     *
     * @return The total number of active WebSocket sessions
     */
    @Override
    @CircuitBreaker(name = "apiGateway", fallbackMethod = "fallbackGetTotalActiveSessionCount")
    @Retry(name = "apiGateway")
    public int getTotalActiveSessionCount() {
        try {
            String apiGatewayUrl = serviceDiscovery.getServiceUrl("api-gateway");
            String url = apiGatewayUrl + "/api/v1/websocket/sessions/count";
            
            ResponseEntity<Integer> response = restTemplate.getForEntity(url, Integer.class);
            return response.getBody() != null ? response.getBody() : 0;
        } catch (Exception e) {
            LOGGER.error("Error getting total active WebSocket session count: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Fallback method for getTotalActiveSessionCount when the circuit breaker is triggered.
     *
     * @param e The exception that triggered the circuit breaker
     * @return 0, assuming no active sessions when the API Gateway is unavailable
     */
    public int fallbackGetTotalActiveSessionCount(Exception e) {
        LOGGER.warn("Using fallback for getTotalActiveSessionCount: {}", e.getMessage());
        return 0;
    }

    /**
     * Broadcasts a notification to all connected WebSocket sessions through the message broker.
     *
     * @param notification The formatted notification message as a JSON string
     * @return 1 if the broadcast command was sent successfully, 0 otherwise
     */
    @Override
    @CircuitBreaker(name = "messageBroker", fallbackMethod = "fallbackBroadcastNotification")
    public int broadcastNotification(String notification) {
        try {
            Map<String, Object> command = new HashMap<>();
            command.put("type", "broadcast");
            command.put("payload", notification);
            
            kafkaTemplate.send(websocketCommandTopic, "broadcast", command);
            LOGGER.debug("Sent broadcast notification command to message broker");
            return 1; // Return 1 to indicate success, actual count is unknown
        } catch (Exception e) {
            LOGGER.error("Error broadcasting notification to WebSockets: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Fallback method for broadcastNotification when the circuit breaker is triggered.
     *
     * @param notification The formatted notification message
     * @param e The exception that triggered the circuit breaker
     * @return 0, indicating the broadcast could not be sent
     */
    public int fallbackBroadcastNotification(String notification, Exception e) {
        LOGGER.warn("Using fallback for broadcastNotification: {}", e.getMessage());
        return 0;
    }
}