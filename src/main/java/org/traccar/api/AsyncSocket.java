/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.helper.model.PositionUtil;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.model.Device;
import org.traccar.model.Event;
import org.traccar.model.LogRecord;
import org.traccar.model.Position;
import org.traccar.session.ConnectionManager;
import org.traccar.session.store.DistributedSessionStore;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class AsyncSocket extends WebSocketAdapter implements ConnectionManager.UpdateListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSocket.class);

    private static final String KEY_DEVICES = "devices";
    private static final String KEY_POSITIONS = "positions";
    private static final String KEY_EVENTS = "events";
    private static final String KEY_LOGS = "logs";

    private final ObjectMapper objectMapper;
    private final ConnectionManager connectionManager;
    private final Storage storage;
    private final long userId;
    private final String connectionId;
    private final DistributedSessionStore webSocketSessionStore;
    private final MessageBrokerManager messageBrokerManager;
    private final MeterRegistry meterRegistry;

    // Metrics
    private final Timer messageSendTimer;
    private final Counter messagesSentCounter;
    private final Counter messagesReceivedCounter;

    private boolean includeLogs;
    private final Set<String> subscribedTopics = new HashSet<>();

    public AsyncSocket(ObjectMapper objectMapper, ConnectionManager connectionManager, Storage storage, long userId,
                      String connectionId, DistributedSessionStore webSocketSessionStore, 
                      MessageBrokerManager messageBrokerManager, MeterRegistry meterRegistry) {
        this.objectMapper = objectMapper;
        this.connectionManager = connectionManager;
        this.storage = storage;
        this.userId = userId;
        this.connectionId = connectionId;
        this.webSocketSessionStore = webSocketSessionStore;
        this.messageBrokerManager = messageBrokerManager;
        this.meterRegistry = meterRegistry;

        // Initialize metrics
        this.messageSendTimer = meterRegistry.timer("websocket.message.send.time");
        this.messagesSentCounter = meterRegistry.counter("websocket.messages.sent");
        this.messagesReceivedCounter = meterRegistry.counter("websocket.messages.received");
    }

    @Override
    public void onWebSocketConnect(Session session) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("onWebSocketConnect").start();
        span.setTag("userId", userId);
        span.setTag("connectionId", connectionId);
        
        try {
            super.onWebSocketConnect(session);

            // Update session with connection details
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("sessionId", session.getRemoteAddress().toString());
            metadata.put("connected", true);
            metadata.put("lastActivity", System.currentTimeMillis());
            webSocketSessionStore.updateWebSocketSession(connectionId, metadata);

            // Subscribe to default topics
            subscribeToBrokerTopics();

            // Send initial data
            Map<String, Collection<?>> data = new HashMap<>();
            data.put(KEY_POSITIONS, PositionUtil.getLatestPositions(storage, userId));
            sendData(data);
            
            // Register as listener for updates
            connectionManager.addListener(userId, this);
            
            LOGGER.info("WebSocket connected: userId={}, connectionId={}", userId, connectionId);
        } catch (StorageException e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.error("Error initializing WebSocket connection", e);
            throw new RuntimeException(e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    @Override
    public void onWebSocketClose(int statusCode, String reason) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("onWebSocketClose").start();
        span.setTag("userId", userId);
        span.setTag("connectionId", connectionId);
        span.setTag("statusCode", statusCode);
        span.setTag("reason", reason);
        
        try {
            super.onWebSocketClose(statusCode, reason);

            // Unsubscribe from all topics
            unsubscribeFromBrokerTopics();

            // Update session status
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("connected", false);
            metadata.put("disconnectTime", System.currentTimeMillis());
            metadata.put("statusCode", statusCode);
            metadata.put("reason", reason);
            webSocketSessionStore.updateWebSocketSession(connectionId, metadata);

            // Remove listener
            connectionManager.removeListener(userId, this);
            
            // Increment disconnection counter
            meterRegistry.counter("websocket.disconnections").increment();
            
            LOGGER.info("WebSocket disconnected: userId={}, connectionId={}, statusCode={}, reason={}", 
                    userId, connectionId, statusCode, reason);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    @Override
    public void onWebSocketText(String message) {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("onWebSocketText").start();
        span.setTag("userId", userId);
        span.setTag("connectionId", connectionId);
        
        try {
            super.onWebSocketText(message);
            messagesReceivedCounter.increment();

            // Update last activity timestamp
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("lastActivity", System.currentTimeMillis());
            webSocketSessionStore.updateWebSocketSession(connectionId, metadata);

            // Process message
            JsonNode node = objectMapper.readTree(message);
            
            // Handle log subscription
            if (node.has("logs")) {
                includeLogs = node.get("logs").asBoolean();
                span.setTag("includeLogs", includeLogs);
            }
            
            // Handle topic subscriptions
            if (node.has("subscribe") && node.get("subscribe").isArray()) {
                for (JsonNode topic : node.get("subscribe")) {
                    String topicName = topic.asText();
                    subscribeToBrokerTopic(topicName);
                    span.setTag("subscribe", topicName);
                }
            }
            
            // Handle topic unsubscriptions
            if (node.has("unsubscribe") && node.get("unsubscribe").isArray()) {
                for (JsonNode topic : node.get("unsubscribe")) {
                    String topicName = topic.asText();
                    unsubscribeFromBrokerTopic(topicName);
                    span.setTag("unsubscribe", topicName);
                }
            }
        } catch (JsonProcessingException e) {
            Tags.ERROR.set(span, true);
            span.log(Map.of("error.message", e.getMessage()));
            LOGGER.warn("Socket JSON parsing error", e);
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    @Override
    public void onKeepalive() {
        String correlationId = UUID.randomUUID().toString();
        MDC.put("correlationId", correlationId);
        Tracer tracer = GlobalTracer.get();
        Span span = tracer.buildSpan("onKeepalive").start();
        span.setTag("userId", userId);
        span.setTag("connectionId", connectionId);
        
        try {
            // Update last activity timestamp
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("lastActivity", System.currentTimeMillis());
            metadata.put("lastKeepalive", System.currentTimeMillis());
            webSocketSessionStore.updateWebSocketSession(connectionId, metadata);
            
            // Send empty data as keepalive
            sendData(new HashMap<>());
        } finally {
            span.finish();
            MDC.remove("correlationId");
        }
    }

    @Override
    public void onUpdateDevice(Device device) {
        sendData(Map.of(KEY_DEVICES, List.of(device)));
    }

    @Override
    public void onUpdatePosition(Position position) {
        sendData(Map.of(KEY_POSITIONS, List.of(position)));
    }

    @Override
    public void onUpdateEvent(Event event) {
        sendData(Map.of(KEY_EVENTS, List.of(event)));
    }

    @Override
    public void onUpdateLog(LogRecord record) {
        if (includeLogs) {
            sendData(Map.of(KEY_LOGS, List.of(record)));
        }
    }

    private void sendData(Map<String, Collection<?>> data) {
        if (isConnected()) {
            String correlationId = UUID.randomUUID().toString();
            MDC.put("correlationId", correlationId);
            Tracer tracer = GlobalTracer.get();
            Span span = tracer.buildSpan("sendData").start();
            span.setTag("userId", userId);
            span.setTag("connectionId", connectionId);
            span.setTag("dataTypes", String.join(",", data.keySet()));
            
            try {
                // Add correlation ID to the data for tracing
                data.put("correlationId", List.of(correlationId));
                
                // Measure message serialization and sending time
                messageSendTimer.record(() -> {
                    try {
                        getRemote().sendString(objectMapper.writeValueAsString(data), null);
                    } catch (JsonProcessingException e) {
                        Tags.ERROR.set(span, true);
                        span.log(Map.of("error.message", e.getMessage()));
                        LOGGER.warn("Socket JSON formatting error", e);
                    }
                    return null;
                });
                
                // Update metrics
                messagesSentCounter.increment();
                
                // Update last activity timestamp
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("lastActivity", System.currentTimeMillis());
                metadata.put("lastMessageSent", System.currentTimeMillis());
                webSocketSessionStore.updateWebSocketSession(connectionId, metadata);
            } finally {
                span.finish();
                MDC.remove("correlationId");
            }
        }
    }
    
    private void subscribeToBrokerTopics() {
        // Subscribe to default topics
        subscribeToBrokerTopic("device-updates");
        subscribeToBrokerTopic("position-updates");
        subscribeToBrokerTopic("event-updates");
        if (includeLogs) {
            subscribeToBrokerTopic("log-updates");
        }
    }
    
    private void subscribeToBrokerTopic(String topic) {
        if (!subscribedTopics.contains(topic)) {
            String userSpecificTopic = topic + "." + userId;
            messageBrokerManager.subscribe(userSpecificTopic, message -> {
                try {
                    // Process message from broker and send to WebSocket
                    Map<String, Collection<?>> data = objectMapper.readValue(message, Map.class);
                    sendData(data);
                } catch (Exception e) {
                    LOGGER.warn("Error processing message from broker topic {}: {}", topic, e.getMessage());
                }
            });
            subscribedTopics.add(topic);
            LOGGER.debug("Subscribed to broker topic: {}", userSpecificTopic);
        }
    }
    
    private void unsubscribeFromBrokerTopics() {
        for (String topic : subscribedTopics) {
            unsubscribeFromBrokerTopic(topic);
        }
        subscribedTopics.clear();
    }
    
    private void unsubscribeFromBrokerTopic(String topic) {
        if (subscribedTopics.contains(topic)) {
            String userSpecificTopic = topic + "." + userId;
            messageBrokerManager.unsubscribe(userSpecificTopic);
            subscribedTopics.remove(topic);
            LOGGER.debug("Unsubscribed from broker topic: {}", userSpecificTopic);
        }
    }
}