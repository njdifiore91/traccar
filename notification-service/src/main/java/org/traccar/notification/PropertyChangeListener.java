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
package org.traccar.notification;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Listens for property change events from the message broker and processes them.
 * This component is responsible for handling property changes that are published
 * by other instances of the notification service or by external configuration
 * management systems like Kubernetes ConfigMap updates.
 */
@Component
public class PropertyChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyChangeListener.class);
    private static final String PROPERTY_CHANGE_TOPIC = "notification-property-changes";

    private final PropertiesProvider propertiesProvider;

    /**
     * Constructor with required dependencies.
     *
     * @param propertiesProvider Properties provider for updating local properties
     */
    @Autowired
    public PropertyChangeListener(PropertiesProvider propertiesProvider) {
        this.propertiesProvider = propertiesProvider;
    }

    /**
     * Kafka listener for property change events.
     * Updates the local property cache when a property change is detected.
     *
     * @param changeEvent Property change event data
     */
    @KafkaListener(topics = PROPERTY_CHANGE_TOPIC, groupId = "${spring.kafka.consumer.group-id:notification-service}")
    public void handlePropertyChange(Map<String, Object> changeEvent) {
        try {
            String key = (String) changeEvent.get("key");
            Object value = changeEvent.get("value");
            Long timestamp = (Long) changeEvent.get("timestamp");

            LOGGER.debug("Received property change event: key={}, timestamp={}", key, timestamp);

            // Clear the cache for this property
            propertiesProvider.clearCache();

            // If value is null, remove the property, otherwise update it
            if (value == null) {
                LOGGER.info("Removing property due to external change: {}", key);
                propertiesProvider.removeProperty(key);
            } else {
                LOGGER.info("Updating property due to external change: {} = {}", key, value);
                propertiesProvider.setProperty(key, value);
            }
        } catch (Exception e) {
            LOGGER.error("Error processing property change event", e);
        }
    }
}