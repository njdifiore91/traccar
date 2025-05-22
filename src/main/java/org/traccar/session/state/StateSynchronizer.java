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
package org.traccar.session.state;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Manages the synchronization of state objects across multiple service instances in the distributed architecture.
 * This class handles publishing state changes to the message broker, subscribing to state updates from other instances,
 * and resolving conflicts when multiple instances update the same state.
 */
@Singleton
public class StateSynchronizer {

    private static final Logger LOGGER = LoggerFactory.getLogger(StateSynchronizer.class);
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;
    private static final long CLEANUP_INTERVAL_MS = 300000; // 5 minutes

    private final MessageBrokerClient messageBrokerClient;
    private final ObjectMapper objectMapper;
    private final String instanceId;
    private final Map<String, StateMetadata> stateMetadataMap;
    private final Map<String, List<StateChangeListener>> stateChangeListeners;
    private final ScheduledExecutorService scheduler;
    private final Tracer tracer;
    private final TextMapPropagator propagator;
    
    // Metrics
    private final Counter statePublishedCounter;
    private final Counter stateReceivedCounter;
    private final Counter conflictResolvedCounter;
    private final Counter syncFailedCounter;
    private final Timer syncDurationTimer;

    /**
     * Constructs a new StateSynchronizer with the necessary dependencies.
     *
     * @param messageBrokerClient Client for interacting with the message broker
     * @param objectMapper JSON serialization/deserialization utility
     * @param meterRegistry Registry for metrics collection
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param propagator Context propagator for distributed tracing
     */
    @Inject
    public StateSynchronizer(
            MessageBrokerClient messageBrokerClient,
            ObjectMapper objectMapper,
            MeterRegistry meterRegistry,
            Tracer tracer,
            TextMapPropagator propagator) {
        this.messageBrokerClient = messageBrokerClient;
        this.objectMapper = objectMapper;
        this.instanceId = UUID.randomUUID().toString();
        this.stateMetadataMap = new ConcurrentHashMap<>();
        this.stateChangeListeners = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.tracer = tracer;
        this.propagator = propagator;
        
        // Initialize metrics
        this.statePublishedCounter = Counter.builder("traccar.state.published")
                .description("Number of state changes published to the message broker")
                .register(meterRegistry);
        this.stateReceivedCounter = Counter.builder("traccar.state.received")
                .description("Number of state changes received from the message broker")
                .register(meterRegistry);
        this.conflictResolvedCounter = Counter.builder("traccar.state.conflicts")
                .description("Number of state conflicts resolved")
                .register(meterRegistry);
        this.syncFailedCounter = Counter.builder("traccar.state.sync.failed")
                .description("Number of failed state synchronization attempts")
                .register(meterRegistry);
        this.syncDurationTimer = Timer.builder("traccar.state.sync.duration")
                .description("Time taken to synchronize state")
                .register(meterRegistry);
        
        // Subscribe to state change topics
        subscribeToStateChanges();
        
        // Schedule cleanup of old metadata
        scheduler.scheduleAtFixedRate(
                this::cleanupOldMetadata, 
                CLEANUP_INTERVAL_MS, 
                CLEANUP_INTERVAL_MS, 
                TimeUnit.MILLISECONDS);
    }

    /**
     * Publishes a state change to the message broker for synchronization across instances.
     * This should be called when a state object has been modified and needs to be synchronized.
     *
     * @param stateId Unique identifier for the state (typically deviceId)
     * @param stateType Type of state (e.g., "motion", "overspeed")
     * @param state The state object to synchronize
     * @param <T> Type of the state object
     * @return true if the state was successfully published, false otherwise
     */
    public <T> boolean publishStateChange(String stateId, String stateType, T state) {
        if (state == null) {
            LOGGER.warn("Attempted to publish null state for id: {}, type: {}", stateId, stateType);
            return false;
        }
        
        Span span = tracer.spanBuilder("publishStateChange").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("stateId", stateId);
            span.setAttribute("stateType", stateType);
            
            // Create state change message
            StateChangeMessage<T> message = new StateChangeMessage<>();
            message.setStateId(stateId);
            message.setStateType(stateType);
            message.setState(state);
            message.setInstanceId(instanceId);
            message.setTimestamp(Instant.now().toEpochMilli());
            message.setVersion(getNextVersion(stateId, stateType));
            
            // Serialize and publish message
            String topic = "state-changes." + stateType;
            String key = stateId;
            String payload = objectMapper.writeValueAsString(message);
            
            // Add trace context to headers
            Map<String, String> headers = new ConcurrentHashMap<>();
            propagator.inject(Context.current(), headers, MAP_SETTER);
            
            // Publish to message broker with retry logic
            boolean success = publishWithRetry(topic, key, payload, headers);
            
            if (success) {
                // Update local metadata
                updateStateMetadata(stateId, stateType, message.getVersion(), message.getTimestamp());
                statePublishedCounter.increment();
                LOGGER.debug("Published state change: id={}, type={}, version={}", 
                        stateId, stateType, message.getVersion());
            } else {
                syncFailedCounter.increment();
                LOGGER.error("Failed to publish state change after retries: id={}, type={}", 
                        stateId, stateType);
            }
            
            return success;
        } catch (Exception e) {
            span.recordException(e);
            syncFailedCounter.increment();
            LOGGER.error("Error publishing state change: id={}, type={}", stateId, stateType, e);
            return false;
        } finally {
            span.end();
        }
    }

    /**
     * Subscribes to state change topics in the message broker.
     */
    private void subscribeToStateChanges() {
        try {
            // Subscribe to all state change topics
            messageBrokerClient.subscribe("state-changes.#", this::handleStateChangeMessage);
            LOGGER.info("Subscribed to state change topics");
        } catch (Exception e) {
            LOGGER.error("Failed to subscribe to state change topics", e);
            // Schedule retry with exponential backoff
            scheduler.schedule(this::subscribeToStateChanges, RETRY_DELAY_MS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Handles incoming state change messages from the message broker.
     * Processes state changes from other service instances and resolves conflicts.
     *
     * @param topic The topic the message was received on
     * @param key The message key (typically stateId)
     * @param payload The message payload
     * @param headers Message headers containing trace context
     */
    private void handleStateChangeMessage(String topic, String key, String payload, Map<String, String> headers) {
        // Extract trace context from headers
        Context extractedContext = propagator.extract(Context.current(), headers, MAP_GETTER);
        Span span = tracer.spanBuilder("handleStateChangeMessage")
                .setParent(extractedContext)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            span.setAttribute("topic", topic);
            span.setAttribute("key", key);
            
            // Deserialize message
            StateChangeMessage<?> message = objectMapper.readValue(payload, StateChangeMessage.class);
            
            // Ignore messages from this instance
            if (instanceId.equals(message.getInstanceId())) {
                LOGGER.debug("Ignoring state change from this instance: id={}, type={}", 
                        message.getStateId(), message.getStateType());
                return;
            }
            
            span.setAttribute("stateId", message.getStateId());
            span.setAttribute("stateType", message.getStateType());
            span.setAttribute("version", message.getVersion());
            span.setAttribute("sourceInstance", message.getInstanceId());
            
            stateReceivedCounter.increment();
            
            // Check for conflicts
            StateMetadata metadata = stateMetadataMap.get(getMetadataKey(message.getStateId(), message.getStateType()));
            if (metadata != null) {
                // If local version is newer, ignore the received message
                if (metadata.getVersion() > message.getVersion() || 
                        (metadata.getVersion() == message.getVersion() && 
                         metadata.getTimestamp() > message.getTimestamp())) {
                    LOGGER.debug("Ignoring older state change: id={}, type={}, remote={}, local={}", 
                            message.getStateId(), message.getStateType(), 
                            message.getVersion(), metadata.getVersion());
                    return;
                }
                
                // If versions are equal but timestamps differ, resolve conflict
                if (metadata.getVersion() == message.getVersion() && 
                        metadata.getTimestamp() != message.getTimestamp()) {
                    LOGGER.debug("Resolving state conflict: id={}, type={}, version={}", 
                            message.getStateId(), message.getStateType(), message.getVersion());
                    conflictResolvedCounter.increment();
                }
            }
            
            // Update local metadata
            updateStateMetadata(message.getStateId(), message.getStateType(), 
                    message.getVersion(), message.getTimestamp());
            
            // Notify state change listeners
            notifyStateChangeListeners(message);
            
            LOGGER.debug("Processed state change: id={}, type={}, version={}", 
                    message.getStateId(), message.getStateType(), message.getVersion());
        } catch (Exception e) {
            span.recordException(e);
            LOGGER.error("Error processing state change message: {}", payload, e);
        } finally {
            span.end();
        }
    }

    /**
     * Notifies registered state change listeners about a received state change.
     *
     * @param message The state change message
     */
    private void notifyStateChangeListeners(StateChangeMessage<?> message) {
        // Implementation dispatches to registered listeners based on state type
        if (stateChangeListeners.containsKey(message.getStateType())) {
            for (StateChangeListener listener : stateChangeListeners.get(message.getStateType())) {
                try {
                    listener.onStateChange(message.getStateId(), message.getState());
                } catch (Exception e) {
                    LOGGER.error("Error notifying state change listener: {}", e.getMessage(), e);
                }
            }
        }
        LOGGER.debug("Notified listeners of state change: id={}, type={}", 
                message.getStateId(), message.getStateType());
    }

    /**
     * Gets the next version number for a state.
     *
     * @param stateId The state identifier
     * @param stateType The state type
     * @return The next version number
     */
    private long getNextVersion(String stateId, String stateType) {
        String key = getMetadataKey(stateId, stateType);
        StateMetadata metadata = stateMetadataMap.get(key);
        return metadata != null ? metadata.getVersion() + 1 : 1;
    }

    /**
     * Updates the metadata for a state.
     *
     * @param stateId The state identifier
     * @param stateType The state type
     * @param version The state version
     * @param timestamp The state timestamp
     */
    private void updateStateMetadata(String stateId, String stateType, long version, long timestamp) {
        String key = getMetadataKey(stateId, stateType);
        stateMetadataMap.put(key, new StateMetadata(version, timestamp));
    }

    /**
     * Generates a metadata key from state ID and type.
     *
     * @param stateId The state identifier
     * @param stateType The state type
     * @return The metadata key
     */
    private String getMetadataKey(String stateId, String stateType) {
        return stateId + "-" + stateType;
    }

    /**
     * Publishes a message to the broker with retry logic.
     * Uses exponential backoff with jitter to avoid thundering herd problems.
     *
     * @param topic The topic to publish to
     * @param key The message key
     * @param payload The message payload
     * @param headers The message headers
     * @return true if publishing succeeded, false otherwise
     */
    private boolean publishWithRetry(String topic, String key, String payload, Map<String, String> headers) {
        int attempts = 0;
        boolean success = false;
        Exception lastException = null;
        
        while (attempts < MAX_RETRY_ATTEMPTS && !success) {
            attempts++;
            try {
                Timer.Sample sample = Timer.start();
                messageBrokerClient.publish(topic, key, payload, headers);
                sample.stop(syncDurationTimer);
                success = true;
            } catch (Exception e) {
                lastException = e;
                LOGGER.warn("Failed to publish state change (attempt {}/{}): {}", 
                        attempts, MAX_RETRY_ATTEMPTS, e.getMessage());
                
                if (attempts < MAX_RETRY_ATTEMPTS) {
                    try {
                        // Exponential backoff with jitter
                        long delay = RETRY_DELAY_MS * (1L << (attempts - 1)) * (long) (0.5 + Math.random());
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        if (!success && lastException != null) {
            LOGGER.error("Failed to publish state change after {} attempts", MAX_RETRY_ATTEMPTS, lastException);
        }
        
        return success;
    }

    /**
     * Cleans up old metadata entries to prevent memory leaks.
     */
    private void cleanupOldMetadata() {
        // Remove entries older than 24 hours
        long cutoffTime = Instant.now().minusSeconds(86400).toEpochMilli();
        int initialSize = stateMetadataMap.size();
        
        if (initialSize > 1000) { // Only perform cleanup if we have a significant number of entries
            LOGGER.debug("Starting metadata cleanup, current size: {}", initialSize);
            
            stateMetadataMap.entrySet().removeIf(entry -> {
                StateMetadata metadata = entry.getValue();
                return metadata.getTimestamp() < cutoffTime;
            });
            
            int removedCount = initialSize - stateMetadataMap.size();
            if (removedCount > 0) {
                LOGGER.info("Cleaned up {} old state metadata entries, new size: {}", 
                        removedCount, stateMetadataMap.size());
            }
        }
    }

    /**
     * Registers a listener for state changes of a specific type.
     *
     * @param stateType The type of state to listen for
     * @param listener The listener to register
     * @param <T> The type of state
     */
    public <T> void registerStateChangeListener(String stateType, StateChangeListener<T> listener) {
        stateChangeListeners.computeIfAbsent(stateType, k -> new ArrayList<>()).add(listener);
        LOGGER.debug("Registered state change listener for type: {}", stateType);
    }

    /**
     * Unregisters a listener for state changes of a specific type.
     *
     * @param stateType The type of state
     * @param listener The listener to unregister
     * @param <T> The type of state
     * @return true if the listener was removed, false otherwise
     */
    public <T> boolean unregisterStateChangeListener(String stateType, StateChangeListener<T> listener) {
        List<StateChangeListener> listeners = stateChangeListeners.get(stateType);
        if (listeners != null) {
            boolean removed = listeners.remove(listener);
            if (listeners.isEmpty()) {
                stateChangeListeners.remove(stateType);
            }
            if (removed) {
                LOGGER.debug("Unregistered state change listener for type: {}", stateType);
            }
            return removed;
        }
        return false;
    }
    
    /**
     * Shuts down the synchronizer and releases resources.
     */
    public void shutdown() {
        try {
            scheduler.shutdown();
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }

    /**
     * Metadata for tracking state versions and timestamps.
     * Used for conflict detection and resolution.
     */
    private static class StateMetadata {
        private final long version;
        private final long timestamp;

        public StateMetadata(long version, long timestamp) {
            this.version = version;
            this.timestamp = timestamp;
        }

        public long getVersion() {
            return version;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Message structure for state changes.
     * Contains all information needed for state synchronization and conflict resolution.
     *
     * @param <T> The type of state
     */
    private static class StateChangeMessage<T> {
        private String stateId;
        private String stateType;
        private T state;
        private String instanceId;
        private long timestamp;
        private long version;

        public String getStateId() {
            return stateId;
        }

        public void setStateId(String stateId) {
            this.stateId = stateId;
        }

        public String getStateType() {
            return stateType;
        }

        public void setStateType(String stateType) {
            this.stateType = stateType;
        }

        public T getState() {
            return state;
        }

        public void setState(T state) {
            this.state = state;
        }

        public String getInstanceId() {
            return instanceId;
        }

        public void setInstanceId(String instanceId) {
            this.instanceId = instanceId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getVersion() {
            return version;
        }

        public void setVersion(long version) {
            this.version = version;
        }
    }

    /**
     * Interface for interacting with the message broker.
     * This abstraction allows for different message broker implementations (Kafka, RabbitMQ, etc.)
     */
    public interface MessageBrokerClient {
        void publish(String topic, String key, String payload, Map<String, String> headers) throws Exception;
        void subscribe(String topicPattern, MessageHandler handler) throws Exception;
    }

    /**
     * Handler for incoming messages from the message broker.
     * Callback interface for asynchronous message processing.
     */
    public interface MessageHandler {
        void handle(String topic, String key, String payload, Map<String, String> headers);
    }
    
    /**
     * Interface for state change listeners.
     *
     * @param <T> The type of state
     */
    public interface StateChangeListener<T> {
        void onStateChange(String stateId, T state);
    }

    // OpenTelemetry context propagation utilities for distributed tracing
    private static final TextMapSetter<Map<String, String>> MAP_SETTER = new TextMapSetter<>() {
        @Override
        public void set(Map<String, String> carrier, String key, String value) {
            carrier.put(key, value);
        }
    };

    private static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<>() {
        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }

        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }
    };
}