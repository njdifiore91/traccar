/*
 * Copyright 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.push;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.messaging.FirebaseMessaging;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryManager;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Firebase client for push notifications.
 * Supports Kubernetes secrets and environment variables for configuration.
 * Integrates with OpenTelemetry for tracing and Micrometer for metrics.
 * Provides health check capabilities for container orchestration.
 */
public class FirebaseClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(FirebaseClient.class);

    private final AtomicBoolean healthy = new AtomicBoolean(false);
    private FirebaseMessaging firebaseMessaging;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final Counter messagesSentCounter;
    private final Counter messagesFailedCounter;
    private final Timer messageSendTimer;

    /**
     * Constructs a new FirebaseClient with the provided configuration.
     * 
     * @param config The configuration containing Firebase credentials
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param serviceDiscoveryManager The service discovery manager for registration
     * @throws IOException If there is an error initializing Firebase
     */
    public FirebaseClient(Config config, 
                         MeterRegistry meterRegistry, 
                         Tracer tracer,
                         ServiceDiscoveryManager serviceDiscoveryManager) throws IOException {
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        
        // Initialize metrics
        this.messagesSentCounter = Counter.builder("firebase.messages.sent")
                .description("Number of messages sent to Firebase")
                .register(meterRegistry);
        
        this.messagesFailedCounter = Counter.builder("firebase.messages.failed")
                .description("Number of failed message sends to Firebase")
                .register(meterRegistry);
        
        this.messageSendTimer = Timer.builder("firebase.message.duration")
                .description("Time taken to send messages to Firebase")
                .register(meterRegistry);
        
        // Initialize Firebase with configuration from either config file, environment variables, or Kubernetes secrets
        initializeFirebase(config);
        
        // Register with service discovery if available
        if (serviceDiscoveryManager != null) {
            serviceDiscoveryManager.register("firebase-client", this::isHealthy);
        }
        
        LOGGER.info("Firebase client initialized successfully");
    }

    /**
     * Initialize Firebase with configuration from various sources.
     * Supports traditional config, environment variables, and Kubernetes secrets.
     * 
     * @param config The configuration object
     * @throws IOException If there is an error initializing Firebase
     */
    private void initializeFirebase(Config config) throws IOException {
        Span span = tracer.spanBuilder("FirebaseClient.initializeFirebase").startSpan();
        try (Scope scope = span.makeCurrent()) {
            String serviceAccountJson = getServiceAccountJson(config);
            
            if (serviceAccountJson == null || serviceAccountJson.isEmpty()) {
                LOGGER.error("Firebase service account configuration is missing");
                span.setAttribute("firebase.init.success", false);
                return;
            }
            
            InputStream serviceAccount = new ByteArrayInputStream(serviceAccountJson.getBytes());
            
            FirebaseOptions options = FirebaseOptions.builder()
                    .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                    .build();
            
            if (FirebaseApp.getApps().isEmpty()) {
                FirebaseApp.initializeApp(options);
            }
            
            firebaseMessaging = FirebaseMessaging.getInstance();
            healthy.set(true);
            span.setAttribute("firebase.init.success", true);
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Firebase", e);
            span.recordException(e);
            span.setAttribute("firebase.init.success", false);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Get the Firebase service account JSON from config, environment variables, or Kubernetes secrets.
     * 
     * @param config The configuration object
     * @return The service account JSON string
     */
    private String getServiceAccountJson(Config config) {
        // Try environment variable first
        String envValue = System.getenv("FIREBASE_SERVICE_ACCOUNT");
        if (envValue != null && !envValue.isEmpty()) {
            LOGGER.info("Using Firebase service account from environment variable");
            return envValue;
        }
        
        // Then try config
        if (config.hasKey(Keys.NOTIFICATOR_FIREBASE_SERVICE_ACCOUNT)) {
            LOGGER.info("Using Firebase service account from configuration");
            return config.getString(Keys.NOTIFICATOR_FIREBASE_SERVICE_ACCOUNT);
        }
        
        LOGGER.warn("Firebase service account configuration not found");
        return null;
    }

    /**
     * Get the Firebase messaging instance.
     * 
     * @return The Firebase messaging instance
     */
    public FirebaseMessaging getInstance() {
        Span span = tracer.spanBuilder("FirebaseClient.getInstance").startSpan();
        try (Scope scope = span.makeCurrent()) {
            if (firebaseMessaging == null) {
                LOGGER.error("Firebase messaging is not initialized");
                span.setAttribute("firebase.available", false);
                throw new IllegalStateException("Firebase messaging is not initialized");
            }
            span.setAttribute("firebase.available", true);
            return firebaseMessaging;
        } finally {
            span.end();
        }
    }

    /**
     * Check if the Firebase client is healthy.
     * Used by health check endpoints and service discovery.
     * 
     * @return true if the client is healthy, false otherwise
     */
    public boolean isHealthy() {
        return healthy.get() && firebaseMessaging != null;
    }

    /**
     * Shutdown the Firebase client.
     * Cleans up resources when the application is shutting down.
     */
    public void shutdown() {
        Span span = tracer.spanBuilder("FirebaseClient.shutdown").startSpan();
        try (Scope scope = span.makeCurrent()) {
            LOGGER.info("Shutting down Firebase client");
            healthy.set(false);
            // Firebase doesn't have an explicit shutdown method, but we mark as unhealthy
            // to prevent new operations during shutdown
            span.setAttribute("firebase.shutdown.success", true);
        } catch (Exception e) {
            LOGGER.error("Error during Firebase client shutdown", e);
            span.recordException(e);
            span.setAttribute("firebase.shutdown.success", false);
        } finally {
            span.end();
        }
    }

    /**
     * Record a successful message send in metrics.
     */
    public void recordMessageSent() {
        messagesSentCounter.increment();
    }

    /**
     * Record a failed message send in metrics.
     */
    public void recordMessageFailed() {
        messagesFailedCounter.increment();
    }

    /**
     * Get the timer for measuring message send duration.
     * 
     * @return The message send timer
     */
    public Timer getMessageSendTimer() {
        return messageSendTimer;
    }
}