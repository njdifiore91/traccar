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
package org.traccar.session.cache;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.messaging.MessageConsumer;
import org.traccar.messaging.MessageConsumerFactory;
import org.traccar.messaging.MessageEnvelope;
import org.traccar.messaging.MessageHandler;
import org.traccar.messaging.MessageHeaders;
import org.traccar.model.BaseModel;
import org.traccar.model.ObjectOperation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Listens for cache invalidation events from the message broker and processes them
 * to maintain cache consistency across service instances.
 */
@Singleton
public class CacheInvalidationListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheInvalidationListener.class);

    private final CacheManager cacheManager;
    private final MessageConsumerFactory messageConsumerFactory;
    private final ExecutorService executorService;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private MessageConsumer objectInvalidationConsumer;
    private MessageConsumer permissionInvalidationConsumer;

    /**
     * Cache invalidation message types
     */
    private static final String OBJECT_INVALIDATION_TYPE = "cache.object.invalidation";
    private static final String PERMISSION_INVALIDATION_TYPE = "cache.permission.invalidation";

    /**
     * Topic names for cache invalidation messages
     */
    private static final String OBJECT_INVALIDATION_TOPIC = "cache-object-invalidation";
    private static final String PERMISSION_INVALIDATION_TOPIC = "cache-permission-invalidation";

    /**
     * Message payload field names
     */
    private static final String FIELD_CLASS_NAME = "className";
    private static final String FIELD_ID = "id";
    private static final String FIELD_OPERATION = "operation";
    private static final String FIELD_CLASS_NAME_1 = "className1";
    private static final String FIELD_ID_1 = "id1";
    private static final String FIELD_CLASS_NAME_2 = "className2";
    private static final String FIELD_ID_2 = "id2";
    private static final String FIELD_LINK = "link";

    /**
     * Constructs a new CacheInvalidationListener.
     *
     * @param cacheManager The cache manager to update when invalidation events are received
     * @param messageConsumerFactory Factory for creating message consumers
     * @param executorService Executor service for processing invalidation messages
     */
    @Inject
    public CacheInvalidationListener(
            CacheManager cacheManager,
            MessageConsumerFactory messageConsumerFactory,
            ExecutorService executorService) {
        this.cacheManager = cacheManager;
        this.messageConsumerFactory = messageConsumerFactory;
        this.executorService = executorService;
        initialize();
    }

    /**
     * Initializes the cache invalidation listeners by creating and configuring
     * the message consumers for object and permission invalidation events.
     */
    private void initialize() {
        try {
            // Create and configure the object invalidation consumer
            objectInvalidationConsumer = messageConsumerFactory.createConsumer(
                    OBJECT_INVALIDATION_TOPIC,
                    "cache-invalidation-group");
            objectInvalidationConsumer.subscribe(new ObjectInvalidationHandler());

            // Create and configure the permission invalidation consumer
            permissionInvalidationConsumer = messageConsumerFactory.createConsumer(
                    PERMISSION_INVALIDATION_TOPIC,
                    "cache-invalidation-group");
            permissionInvalidationConsumer.subscribe(new PermissionInvalidationHandler());

            LOGGER.info("Cache invalidation listeners initialized successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to initialize cache invalidation listeners", e);
        }
    }

    /**
     * Handles object invalidation messages.
     */
    private class ObjectInvalidationHandler implements MessageHandler<MessageEnvelope> {

        @Override
        public void handle(MessageEnvelope message) {
            if (!OBJECT_INVALIDATION_TYPE.equals(message.getHeaders().get(MessageHeaders.MESSAGE_TYPE))) {
                LOGGER.warn("Received message with unexpected type: {}", message.getHeaders().get(MessageHeaders.MESSAGE_TYPE));
                return;
            }

            executorService.submit(() -> {
                try {
                    lock.readLock().lock();
                    try {
                        processObjectInvalidation(message);
                    } finally {
                        lock.readLock().unlock();
                    }
                } catch (Exception e) {
                    LOGGER.error("Error processing object invalidation message", e);
                }
            });
        }

        /**
         * Processes an object invalidation message by extracting the class, ID, and operation
         * and calling the appropriate method on the cache manager.
         *
         * @param message The message envelope containing the invalidation details
         * @throws Exception If an error occurs during processing
         */
        @SuppressWarnings("unchecked")
        private void processObjectInvalidation(MessageEnvelope message) throws Exception {
            String className = (String) message.getPayload().get(FIELD_CLASS_NAME);
            Long id = ((Number) message.getPayload().get(FIELD_ID)).longValue();
            String operationName = (String) message.getPayload().get(FIELD_OPERATION);

            if (className == null || id == null || operationName == null) {
                LOGGER.warn("Received invalid object invalidation message: missing required fields");
                return;
            }

            try {
                Class<? extends BaseModel> clazz = (Class<? extends BaseModel>) Class.forName(className);
                ObjectOperation operation = ObjectOperation.valueOf(operationName);

                LOGGER.debug("Processing object invalidation: class={}, id={}, operation={}", className, id, operation);
                cacheManager.invalidateObject(false, clazz, id, operation);
            } catch (ClassNotFoundException e) {
                LOGGER.error("Invalid class name in object invalidation message: {}", className, e);
            } catch (IllegalArgumentException e) {
                LOGGER.error("Invalid operation in object invalidation message: {}", operationName, e);
            }
        }
    }

    /**
     * Handles permission invalidation messages.
     */
    private class PermissionInvalidationHandler implements MessageHandler<MessageEnvelope> {

        @Override
        public void handle(MessageEnvelope message) {
            if (!PERMISSION_INVALIDATION_TYPE.equals(message.getHeaders().get(MessageHeaders.MESSAGE_TYPE))) {
                LOGGER.warn("Received message with unexpected type: {}", message.getHeaders().get(MessageHeaders.MESSAGE_TYPE));
                return;
            }

            executorService.submit(() -> {
                try {
                    lock.readLock().lock();
                    try {
                        processPermissionInvalidation(message);
                    } finally {
                        lock.readLock().unlock();
                    }
                } catch (Exception e) {
                    LOGGER.error("Error processing permission invalidation message", e);
                }
            });
        }

        /**
         * Processes a permission invalidation message by extracting the classes, IDs, and link flag
         * and calling the appropriate method on the cache manager.
         *
         * @param message The message envelope containing the invalidation details
         * @throws Exception If an error occurs during processing
         */
        @SuppressWarnings("unchecked")
        private void processPermissionInvalidation(MessageEnvelope message) throws Exception {
            String className1 = (String) message.getPayload().get(FIELD_CLASS_NAME_1);
            Long id1 = ((Number) message.getPayload().get(FIELD_ID_1)).longValue();
            String className2 = (String) message.getPayload().get(FIELD_CLASS_NAME_2);
            Long id2 = ((Number) message.getPayload().get(FIELD_ID_2)).longValue();
            Boolean link = (Boolean) message.getPayload().get(FIELD_LINK);

            if (className1 == null || id1 == null || className2 == null || id2 == null || link == null) {
                LOGGER.warn("Received invalid permission invalidation message: missing required fields");
                return;
            }

            try {
                Class<? extends BaseModel> clazz1 = (Class<? extends BaseModel>) Class.forName(className1);
                Class<? extends BaseModel> clazz2 = (Class<? extends BaseModel>) Class.forName(className2);

                LOGGER.debug("Processing permission invalidation: class1={}, id1={}, class2={}, id2={}, link={}",
                        className1, id1, className2, id2, link);
                cacheManager.invalidatePermission(false, clazz1, id1, clazz2, id2, link);
            } catch (ClassNotFoundException e) {
                LOGGER.error("Invalid class name in permission invalidation message: {} or {}", className1, className2, e);
            }
        }
    }
}