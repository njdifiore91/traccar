/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.health;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.stream.binding.BinderAwareChannelResolver;
import org.springframework.stereotype.Component;

/**
 * Custom health indicator for the notification service.
 * This class provides health information about the notification service's dependencies,
 * including the message broker connection and service discovery status.
 */
@Component
public class NotificationServiceHealthIndicator implements HealthIndicator {

    private final BinderAwareChannelResolver binderAwareChannelResolver;
    private final DiscoveryClient discoveryClient;

    /**
     * Constructs a new NotificationServiceHealthIndicator with the required dependencies.
     * 
     * @param binderAwareChannelResolver The channel resolver for checking message broker connectivity
     * @param discoveryClient The discovery client for checking service discovery status
     */
    @Autowired
    public NotificationServiceHealthIndicator(
            BinderAwareChannelResolver binderAwareChannelResolver,
            DiscoveryClient discoveryClient) {
        this.binderAwareChannelResolver = binderAwareChannelResolver;
        this.discoveryClient = discoveryClient;
    }

    /**
     * Provides health information about the notification service.
     * Checks the status of message broker connectivity and service discovery.
     * 
     * @return The health status of the notification service
     */
    @Override
    public Health health() {
        Health.Builder builder = new Health.Builder();
        
        try {
            // Check message broker connectivity
            if (binderAwareChannelResolver != null) {
                builder.withDetail("messageBroker", "connected");
            } else {
                return builder.down()
                        .withDetail("messageBroker", "disconnected")
                        .withDetail("error", "Message broker connection is not available")
                        .build();
            }
            
            // Check service discovery status
            if (discoveryClient != null) {
                builder.withDetail("serviceDiscovery", "connected")
                       .withDetail("services", discoveryClient.getServices());
            } else {
                return builder.down()
                        .withDetail("serviceDiscovery", "disconnected")
                        .withDetail("error", "Service discovery is not available")
                        .build();
            }
            
            return builder.up().build();
        } catch (Exception e) {
            return builder.down()
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
}