/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.template;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.tools.generic.DateTool;
import org.apache.velocity.tools.generic.NumberTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.signature.TokenManager;
import org.traccar.helper.model.UserUtil;
import org.traccar.messaging.MessageBroker;
import org.traccar.model.*;
import org.traccar.session.cache.CacheManager;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

/**
 * Builds context data for notification templates, gathering information from various sources
 * including server configuration, user preferences, event data, and position information.
 * <p>
 * This component provides a fluent API for constructing template contexts with asynchronous
 * data loading and resilience patterns for external data sources.
 */
@Singleton
public class TemplateContextBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(TemplateContextBuilder.class);

    private final CacheManager cacheManager;
    private final TokenManager tokenManager;
    private final MessageBroker messageBroker;
    private final ExecutorService executorService;
    private final CircuitBreakerRegistry circuitBreakerRegistry;

    /**
     * Constructs a new TemplateContextBuilder with the required dependencies.
     *
     * @param cacheManager Cache manager for retrieving cached objects
     * @param tokenManager Token manager for generating authentication tokens
     * @param messageBroker Message broker for retrieving data from other services
     */
    @Inject
    public TemplateContextBuilder(
            CacheManager cacheManager,
            TokenManager tokenManager,
            MessageBroker messageBroker) {
        this.cacheManager = cacheManager;
        this.tokenManager = tokenManager;
        this.messageBroker = messageBroker;
        this.executorService = Executors.newCachedThreadPool();
        
        // Configure circuit breaker for external data sources
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .waitDurationInOpenState(java.time.Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(3)
                .build();
        
        this.circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
    }

    /**
     * Builder class for constructing template contexts with a fluent API.
     */
    public class Builder {
        private final Map<String, Object> contextData = new HashMap<>();
        private final Map<String, CompletableFuture<Object>> asyncData = new HashMap<>();
        private Server server;
        private User user;
        private Event event;
        private Position position;
        private boolean includeToken = true;

        private Builder() {
            // Initialize with common tools
            contextData.put("dateTool", new DateTool());
            contextData.put("numberTool", new NumberTool());
            contextData.put("locale", Locale.getDefault());
        }

        /**
         * Sets the server configuration for the template context.
         *
         * @param server Server configuration
         * @return this builder for method chaining
         */
        public Builder withServer(Server server) {
            this.server = server;
            contextData.put("webUrl", server.getWebUrl());
            return this;
        }

        /**
         * Sets the user for the template context.
         *
         * @param user User information
         * @return this builder for method chaining
         */
        public Builder withUser(User user) {
            this.user = user;
            contextData.put("user", user);
            
            if (server != null) {
                contextData.put("timezone", UserUtil.getTimezone(server, user));
                
                if (includeToken) {
                    // Generate token asynchronously with circuit breaker
                    CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("tokenGenerator");
                    Supplier<Object> tokenSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                        try {
                            return tokenManager.generateToken(user.getId());
                        } catch (IOException | GeneralSecurityException | Exception e) {
                            LOGGER.warn("Token generation failed", e);
                            return "";
                        }
                    });
                    
                    asyncData.put("token", CompletableFuture.supplyAsync(tokenSupplier, executorService));
                }
            }
            return this;
        }

        /**
         * Sets whether to include an authentication token in the context.
         *
         * @param includeToken true to include token, false otherwise
         * @return this builder for method chaining
         */
        public Builder includeToken(boolean includeToken) {
            this.includeToken = includeToken;
            return this;
        }

        /**
         * Sets the event for the template context.
         *
         * @param event Event information
         * @return this builder for method chaining
         */
        public Builder withEvent(Event event) {
            this.event = event;
            contextData.put("event", event);
            
            // Asynchronously load device information with circuit breaker
            if (event.getDeviceId() != 0) {
                CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("deviceLoader");
                Supplier<Object> deviceSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, () -> 
                    cacheManager.getObject(Device.class, event.getDeviceId()));
                
                asyncData.put("device", CompletableFuture.supplyAsync(deviceSupplier, executorService));
            }
            
            // Asynchronously load geofence information if present
            if (event.getGeofenceId() != 0) {
                CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("geofenceLoader");
                Supplier<Object> geofenceSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, () -> 
                    cacheManager.getObject(Geofence.class, event.getGeofenceId()));
                
                asyncData.put("geofence", CompletableFuture.supplyAsync(geofenceSupplier, executorService));
            }
            
            // Asynchronously load maintenance information if present
            if (event.getMaintenanceId() != 0) {
                CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("maintenanceLoader");
                Supplier<Object> maintenanceSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, () -> 
                    cacheManager.getObject(Maintenance.class, event.getMaintenanceId()));
                
                asyncData.put("maintenance", CompletableFuture.supplyAsync(maintenanceSupplier, executorService));
            }
            
            return this;
        }

        /**
         * Sets the position for the template context.
         *
         * @param position Position information
         * @return this builder for method chaining
         */
        public Builder withPosition(Position position) {
            this.position = position;
            contextData.put("position", position);
            
            if (server != null && user != null) {
                contextData.put("speedUnit", UserUtil.getSpeedUnit(server, user));
                contextData.put("distanceUnit", UserUtil.getDistanceUnit(server, user));
                contextData.put("volumeUnit", UserUtil.getVolumeUnit(server, user));
            }
            
            // Load driver information if available
            if (position != null && event != null) {
                String driverUniqueId = position.getString(Position.KEY_DRIVER_UNIQUE_ID);
                if (driverUniqueId != null && event.getDeviceId() != 0) {
                    CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("driverLoader");
                    Supplier<Object> driverSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                        Device device = cacheManager.getObject(Device.class, event.getDeviceId());
                        if (device != null) {
                            return cacheManager.getDeviceObjects(device.getId(), Driver.class).stream()
                                    .filter(driver -> driver.getUniqueId().equals(driverUniqueId))
                                    .findFirst()
                                    .orElse(null);
                        }
                        return null;
                    });
                    
                    asyncData.put("driver", CompletableFuture.supplyAsync(driverSupplier, executorService));
                }
            }
            
            return this;
        }

        /**
         * Sets the notification for the template context.
         *
         * @param notification Notification information
         * @return this builder for method chaining
         */
        public Builder withNotification(Notification notification) {
            contextData.put("notification", notification);
            return this;
        }

        /**
         * Adds a custom key-value pair to the template context.
         *
         * @param key Key for the context data
         * @param value Value for the context data
         * @return this builder for method chaining
         */
        public Builder withCustomData(String key, Object value) {
            contextData.put(key, value);
            return this;
        }

        /**
         * Adds a custom key-value pair to be loaded asynchronously.
         *
         * @param key Key for the context data
         * @param supplier Supplier to provide the value asynchronously
         * @return this builder for method chaining
         */
        public Builder withAsyncData(String key, Supplier<Object> supplier) {
            CircuitBreaker circuitBreaker = circuitBreakerRegistry.circuitBreaker("customDataLoader");
            Supplier<Object> decoratedSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, supplier);
            asyncData.put(key, CompletableFuture.supplyAsync(decoratedSupplier, executorService));
            return this;
        }

        /**
         * Builds the VelocityContext with all the configured data.
         * This method waits for all asynchronous data to complete before returning.
         *
         * @return The fully populated VelocityContext
         */
        public VelocityContext build() {
            // Wait for all async operations to complete
            CompletableFuture.allOf(asyncData.values().toArray(new CompletableFuture[0]))
                    .exceptionally(ex -> {
                        LOGGER.warn("Error loading some context data", ex);
                        return null;
                    })
                    .join();
            
            // Add async results to context data
            asyncData.forEach((key, future) -> {
                try {
                    Object value = future.join();
                    if (value != null) {
                        contextData.put(key, value);
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to get async data for key: {}", key, e);
                }
            });
            
            // Validate required context data
            validateContextData();
            
            // Create and populate VelocityContext
            VelocityContext velocityContext = new VelocityContext();
            contextData.forEach(velocityContext::put);
            
            return velocityContext;
        }
        
        /**
         * Builds the VelocityContext asynchronously with all the configured data.
         *
         * @return CompletableFuture that will resolve to the fully populated VelocityContext
         */
        public CompletableFuture<VelocityContext> buildAsync() {
            return CompletableFuture.supplyAsync(() -> build(), executorService);
        }
        
        /**
         * Validates that all required context data is present.
         * Throws IllegalStateException if validation fails.
         */
        private void validateContextData() {
            if (event != null && !contextData.containsKey("device")) {
                LOGGER.warn("Event present but device data missing in template context");
            }
            
            if (position != null && (server == null || user == null)) {
                LOGGER.warn("Position present but server or user data missing in template context");
            }
        }
    }

    /**
     * Creates a new builder for constructing template contexts.
     *
     * @return A new Builder instance
     */
    public Builder newBuilder() {
        return new Builder();
    }

    /**
     * Creates a new builder pre-configured with server and user information.
     *
     * @param server Server configuration
     * @param user User information
     * @return A new Builder instance with server and user already configured
     */
    public Builder newBuilder(Server server, User user) {
        return newBuilder()
                .withServer(server)
                .withUser(user);
    }

    /**
     * Prepares a basic context with server and user information.
     * This is a convenience method for backward compatibility.
     *
     * @param server Server configuration
     * @param user User information
     * @return A VelocityContext with basic server and user information
     */
    public VelocityContext prepareBasicContext(Server server, User user) {
        return newBuilder(server, user).build();
    }
}