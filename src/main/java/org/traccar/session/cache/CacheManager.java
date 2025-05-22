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
import org.traccar.config.Config;
import org.traccar.model.Attribute;
import org.traccar.model.BaseModel;
import org.traccar.model.Calendar;
import org.traccar.model.Device;
import org.traccar.model.Driver;
import org.traccar.model.Geofence;
import org.traccar.model.Group;
import org.traccar.model.GroupedModel;
import org.traccar.model.Maintenance;
import org.traccar.model.Notification;
import org.traccar.model.ObjectOperation;
import org.traccar.model.Permission;
import org.traccar.model.Position;
import org.traccar.model.Schedulable;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

// New imports for Redis integration
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.RedisException;

// New imports for message broker integration
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageConsumer;

// New imports for metrics
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Tag;

// New imports for circuit breaker
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

// New imports for distributed locking
import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Singleton
public class CacheManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheManager.class);

    private static final Set<Class<? extends BaseModel>> GROUPED_CLASSES =
            Set.of(Attribute.class, Driver.class, Geofence.class, Maintenance.class, Notification.class);

    private final Config config;
    private final Storage storage;
    private final MessageProducer messageProducer;
    private final MessageConsumer messageConsumer;
    private final MeterRegistry meterRegistry;
    private final CircuitBreaker redisCircuitBreaker;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final CacheGraph graph = new CacheGraph();

    private Server server;
    private final Map<Long, Position> devicePositions = new HashMap<>();
    private final Map<Long, HashSet<Object>> deviceReferences = new HashMap<>();

    // Redis client for distributed caching
    private RedisClient redisClient;
    private StatefulRedisConnection<String, String> redisConnection;
    private RedisCommands<String, String> redisSync;
    private RedisAsyncCommands<String, String> redisAsync;

    // Metrics
    private final Counter cacheHits;
    private final Counter cacheMisses;
    private final Timer cacheGetTimer;
    private final Timer cachePutTimer;
    private final Timer redisGetTimer;
    private final Timer redisPutTimer;

    @Inject
    public CacheManager(Config config, Storage storage, MessageProducer messageProducer, 
                       MessageConsumer messageConsumer, MeterRegistry meterRegistry) throws StorageException {
        this.config = config;
        this.storage = storage;
        this.messageProducer = messageProducer;
        this.messageConsumer = messageConsumer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        cacheHits = meterRegistry.counter("cache.hits", "type", "local");
        cacheMisses = meterRegistry.counter("cache.misses", "type", "local");
        cacheGetTimer = meterRegistry.timer("cache.get.time", "type", "local");
        cachePutTimer = meterRegistry.timer("cache.put.time", "type", "local");
        redisGetTimer = meterRegistry.timer("cache.get.time", "type", "redis");
        redisPutTimer = meterRegistry.timer("cache.put.time", "type", "redis");
        
        // Initialize circuit breaker for Redis
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .build();
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        redisCircuitBreaker = circuitBreakerRegistry.circuitBreaker("redis");
        
        server = storage.getObject(Server.class, new Request(new Columns.All()));
        
        // Initialize Redis client if configured
        initializeRedis();
        
        // Subscribe to cache invalidation messages
        subscribeToInvalidationMessages();
    }
    
    private void initializeRedis() {
        try {
            String redisUrl = config.getString("cache.redis.url");
            if (redisUrl != null && !redisUrl.isEmpty()) {
                LOGGER.info("Initializing Redis cache connection to {}", redisUrl);
                redisClient = RedisClient.create(RedisURI.create(redisUrl));
                redisConnection = redisClient.connect();
                redisSync = redisConnection.sync();
                redisAsync = redisConnection.async();
                LOGGER.info("Redis cache connection established");
            } else {
                LOGGER.info("Redis cache not configured, using local cache only");
            }
        } catch (Exception e) {
            LOGGER.warn("Failed to initialize Redis cache, falling back to local cache only", e);
        }
    }
    
    private void subscribeToInvalidationMessages() {
        if (messageConsumer != null) {
            messageConsumer.subscribe("cache.invalidation", message -> {
                try {
                    String[] parts = message.split(":");
                    if (parts.length >= 3) {
                        String type = parts[0];
                        if ("object".equals(type)) {
                            String className = parts[1];
                            long id = Long.parseLong(parts[2]);
                            ObjectOperation operation = ObjectOperation.valueOf(parts[3]);
                            Class<? extends BaseModel> clazz = (Class<? extends BaseModel>) Class.forName(className);
                            handleInvalidateObject(clazz, id, operation);
                        } else if ("permission".equals(type)) {
                            String class1Name = parts[1];
                            long id1 = Long.parseLong(parts[2]);
                            String class2Name = parts[3];
                            long id2 = Long.parseLong(parts[4]);
                            boolean link = Boolean.parseBoolean(parts[5]);
                            Class<? extends BaseModel> class1 = (Class<? extends BaseModel>) Class.forName(class1Name);
                            Class<? extends BaseModel> class2 = (Class<? extends BaseModel>) Class.forName(class2Name);
                            handleInvalidatePermission(class1, id1, class2, id2, link);
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("Error processing cache invalidation message", e);
                }
            });
            LOGGER.info("Subscribed to cache invalidation messages");
        }
    }

    @Override
    public String toString() {
        return graph.toString();
    }

    public Config getConfig() {
        return config;
    }

    public <T extends BaseModel> T getObject(Class<T> clazz, long id) {
        return cacheGetTimer.record(() -> {
            // First check local cache
            try {
                lock.readLock().lock();
                T result = graph.getObject(clazz, id);
                if (result != null) {
                    cacheHits.increment();
                    return result;
                }
            } finally {
                lock.readLock().unlock();
            }
            
            cacheMisses.increment();
            
            // If not in local cache, try Redis if available
            if (redisConnection != null && redisConnection.isOpen()) {
                try {
                    return redisCircuitBreaker.executeSupplier(() -> {
                        return redisGetTimer.record(() -> {
                            String key = "object:" + clazz.getName() + ":" + id;
                            String json = redisSync.get(key);
                            if (json != null) {
                                try {
                                    // Deserialize from JSON and store in local cache
                                    T result = deserializeFromJson(json, clazz);
                                    if (result != null) {
                                        try {
                                            lock.writeLock().lock();
                                            graph.addObject(result);
                                        } finally {
                                            lock.writeLock().unlock();
                                        }
                                        return result;
                                    }
                                } catch (Exception e) {
                                    LOGGER.warn("Error deserializing object from Redis", e);
                                }
                            }
                            return null;
                        });
                    });
                } catch (Exception e) {
                    LOGGER.warn("Redis cache access failed, falling back to database", e);
                }
            }
            
            // If not in Redis or Redis failed, load from database
            try {
                T result = storage.getObject(clazz, new Request(new Columns.All(), new Condition.Equals("id", id)));
                if (result != null) {
                    // Store in local cache
                    try {
                        lock.writeLock().lock();
                        graph.addObject(result);
                    } finally {
                        lock.writeLock().unlock();
                    }
                    
                    // Store in Redis if available
                    if (redisConnection != null && redisConnection.isOpen()) {
                        try {
                            redisCircuitBreaker.executeRunnable(() -> {
                                redisPutTimer.record(() -> {
                                    String key = "object:" + clazz.getName() + ":" + id;
                                    String json = serializeToJson(result);
                                    if (json != null) {
                                        redisAsync.set(key, json);
                                        // Set expiration time if configured
                                        int ttl = config.getInteger("cache.redis.ttl", 3600);
                                        if (ttl > 0) {
                                            redisAsync.expire(key, ttl);
                                        }
                                    }
                                });
                            });
                        } catch (Exception e) {
                            LOGGER.warn("Failed to store object in Redis cache", e);
                        }
                    }
                }
                return result;
            } catch (StorageException e) {
                LOGGER.warn("Error loading object from database", e);
                return null;
            }
        });
    }

    public <T extends BaseModel> Set<T> getDeviceObjects(long deviceId, Class<T> clazz) {
        try {
            lock.readLock().lock();
            return graph.getObjects(Device.class, deviceId, clazz, Set.of(Group.class), true)
                    .collect(Collectors.toUnmodifiableSet());
        } finally {
            lock.readLock().unlock();
        }
    }

    public Position getPosition(long deviceId) {
        try {
            lock.readLock().lock();
            return devicePositions.get(deviceId);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Server getServer() {
        try {
            lock.readLock().lock();
            return server;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<User> getNotificationUsers(long notificationId, long deviceId) {
        try {
            lock.readLock().lock();
            Set<User> deviceUsers = getDeviceObjects(deviceId, User.class);
            return graph.getObjects(Notification.class, notificationId, User.class, Set.of(), false)
                    .filter(deviceUsers::contains)
                    .collect(Collectors.toUnmodifiableSet());
        } finally {
            lock.readLock().unlock();
        }
    }

    public Set<Notification> getDeviceNotifications(long deviceId) {
        try {
            lock.readLock().lock();
            var direct = graph.getObjects(Device.class, deviceId, Notification.class, Set.of(Group.class), true)
                    .map(BaseModel::getId)
                    .collect(Collectors.toUnmodifiableSet());
            return graph.getObjects(Device.class, deviceId, Notification.class, Set.of(Group.class, User.class), true)
                    .filter(notification -> notification.getAlways() || direct.contains(notification.getId()))
                    .collect(Collectors.toUnmodifiableSet());
        } finally {
            lock.readLock().unlock();
        }
    }

    public void addDevice(long deviceId, Object key) throws Exception {
        try {
            lock.writeLock().lock();
            var references = deviceReferences.computeIfAbsent(deviceId, k -> new HashSet<>());
            if (references.isEmpty()) {
                Device device = storage.getObject(Device.class, new Request(
                        new Columns.All(), new Condition.Equals("id", deviceId)));
                graph.addObject(device);
                initializeCache(device);
                if (device.getPositionId() > 0) {
                    devicePositions.put(deviceId, storage.getObject(Position.class, new Request(
                            new Columns.All(), new Condition.Equals("id", device.getPositionId()))));
                }
            }
            references.add(key);
            LOGGER.debug("Cache add device {} references {} key {}", deviceId, references.size(), key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void removeDevice(long deviceId, Object key) {
        try {
            lock.writeLock().lock();
            var references = deviceReferences.computeIfAbsent(deviceId, k -> new HashSet<>());
            references.remove(key);
            if (references.isEmpty()) {
                graph.removeObject(Device.class, deviceId);
                devicePositions.remove(deviceId);
                deviceReferences.remove(deviceId);
            }
            LOGGER.debug("Cache remove device {} references {} key {}", deviceId, references.size(), key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void updatePosition(Position position) {
        try {
            lock.writeLock().lock();
            if (deviceReferences.containsKey(position.getDeviceId())) {
                devicePositions.put(position.getDeviceId(), position);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    public <T extends BaseModel> void invalidateObject(
            boolean local, Class<T> clazz, long id, ObjectOperation operation) throws Exception {
        if (local) {
            // Publish invalidation message to message broker
            String message = String.format("object:%s:%d:%s", clazz.getName(), id, operation);
            messageProducer.publish("cache.invalidation", message);
        }

        handleInvalidateObject(clazz, id, operation);
    }
    
    private <T extends BaseModel> void handleInvalidateObject(
            Class<T> clazz, long id, ObjectOperation operation) throws Exception {
        if (operation == ObjectOperation.DELETE) {
            graph.removeObject(clazz, id);
            
            // Remove from Redis if available
            if (redisConnection != null && redisConnection.isOpen()) {
                try {
                    redisCircuitBreaker.executeRunnable(() -> {
                        String key = "object:" + clazz.getName() + ":" + id;
                        redisAsync.del(key);
                    });
                } catch (Exception e) {
                    LOGGER.warn("Failed to remove object from Redis cache", e);
                }
            }
        }
        if (operation != ObjectOperation.UPDATE) {
            return;
        }

        if (clazz.equals(Server.class)) {
            server = storage.getObject(Server.class, new Request(new Columns.All()));
            return;
        }

        var after = storage.getObject(clazz, new Request(new Columns.All(), new Condition.Equals("id", id)));
        if (after == null) {
            return;
        }
        var before = getObject(after.getClass(), after.getId());
        if (before == null) {
            return;
        }

        if (after instanceof GroupedModel) {
            long beforeGroupId = ((GroupedModel) before).getGroupId();
            long afterGroupId = ((GroupedModel) after).getGroupId();
            if (beforeGroupId != afterGroupId) {
                if (beforeGroupId > 0) {
                    invalidatePermission(clazz, id, Group.class, beforeGroupId, false);
                }
                if (afterGroupId > 0) {
                    invalidatePermission(clazz, id, Group.class, afterGroupId, true);
                }
            }
        } else if (after instanceof Schedulable) {
            long beforeCalendarId = ((Schedulable) before).getCalendarId();
            long afterCalendarId = ((Schedulable) after).getCalendarId();
            if (beforeCalendarId != afterCalendarId) {
                if (beforeCalendarId > 0) {
                    invalidatePermission(clazz, id, Calendar.class, beforeCalendarId, false);
                }
                if (afterCalendarId > 0) {
                    invalidatePermission(clazz, id, Calendar.class, afterCalendarId, true);
                }
            }
            // TODO handle notification always change
        }

        graph.updateObject(after);
        
        // Update in Redis if available
        if (redisConnection != null && redisConnection.isOpen()) {
            try {
                redisCircuitBreaker.executeRunnable(() -> {
                    redisPutTimer.record(() -> {
                        String key = "object:" + clazz.getName() + ":" + id;
                        String json = serializeToJson(after);
                        if (json != null) {
                            redisAsync.set(key, json);
                            // Set expiration time if configured
                            int ttl = config.getInteger("cache.redis.ttl", 3600);
                            if (ttl > 0) {
                                redisAsync.expire(key, ttl);
                            }
                        }
                    });
                });
            } catch (Exception e) {
                LOGGER.warn("Failed to update object in Redis cache", e);
            }
        }
    }

    public <T1 extends BaseModel, T2 extends BaseModel> void invalidatePermission(
            boolean local, Class<T1> clazz1, long id1, Class<T2> clazz2, long id2, boolean link) throws Exception {
        if (local) {
            // Publish invalidation message to message broker
            String message = String.format("permission:%s:%d:%s:%d:%b", 
                    clazz1.getName(), id1, clazz2.getName(), id2, link);
            messageProducer.publish("cache.invalidation", message);
        }

        handleInvalidatePermission(clazz1, id1, clazz2, id2, link);
    }
    
    private <T1 extends BaseModel, T2 extends BaseModel> void handleInvalidatePermission(
            Class<T1> clazz1, long id1, Class<T2> clazz2, long id2, boolean link) throws Exception {
        if (clazz1.equals(User.class) && GroupedModel.class.isAssignableFrom(clazz2)) {
            invalidatePermission(clazz2, id2, clazz1, id1, link);
        } else {
            invalidatePermission(clazz1, id1, clazz2, id2, link);
        }
    }

    private <T1 extends BaseModel, T2 extends BaseModel> void invalidatePermission(
            Class<T1> fromClass, long fromId, Class<T2> toClass, long toId, boolean link) throws Exception {

        boolean groupLink = GroupedModel.class.isAssignableFrom(fromClass) && toClass.equals(Group.class);
        boolean calendarLink = Schedulable.class.isAssignableFrom(fromClass) && toClass.equals(Calendar.class);
        boolean userLink = fromClass.equals(User.class) && toClass.equals(Notification.class);

        boolean groupedLinks = GroupedModel.class.isAssignableFrom(fromClass)
                && (GROUPED_CLASSES.contains(toClass) || toClass.equals(User.class));

        if (!groupLink && !calendarLink && !userLink && !groupedLinks) {
            return;
        }

        if (link) {
            BaseModel object = storage.getObject(toClass, new Request(
                    new Columns.All(), new Condition.Equals("id", toId)));
            if (!graph.addLink(fromClass, fromId, object)) {
                initializeCache(object);
            }
        } else {
            graph.removeLink(fromClass, fromId, toClass, toId);
        }
        
        // Invalidate Redis cache entries if available
        if (redisConnection != null && redisConnection.isOpen()) {
            try {
                redisCircuitBreaker.executeRunnable(() -> {
                    String key1 = "object:" + fromClass.getName() + ":" + fromId;
                    String key2 = "object:" + toClass.getName() + ":" + toId;
                    redisAsync.del(key1, key2);
                });
            } catch (Exception e) {
                LOGGER.warn("Failed to invalidate permission in Redis cache", e);
            }
        }
    }

    private void initializeCache(BaseModel object) throws Exception {
        if (object instanceof User) {
            for (Permission permission : storage.getPermissions(User.class, Notification.class)) {
                if (permission.getOwnerId() == object.getId()) {
                    invalidatePermission(
                            permission.getOwnerClass(), permission.getOwnerId(),
                            permission.getPropertyClass(), permission.getPropertyId(), true);
                }
            }
        } else {
            if (object instanceof GroupedModel groupedModel) {
                long groupId = groupedModel.getGroupId();
                if (groupId > 0) {
                    invalidatePermission(object.getClass(), object.getId(), Group.class, groupId, true);
                }

                for (Permission permission : storage.getPermissions(User.class, object.getClass())) {
                    if (permission.getPropertyId() == object.getId()) {
                        invalidatePermission(
                                object.getClass(), object.getId(), User.class, permission.getOwnerId(), true);
                    }
                }

                for (Class<? extends BaseModel> clazz : GROUPED_CLASSES) {
                    for (Permission permission : storage.getPermissions(object.getClass(), clazz)) {
                        if (permission.getOwnerId() == object.getId()) {
                            invalidatePermission(
                                    object.getClass(), object.getId(), clazz, permission.getPropertyId(), true);
                        }
                    }
                }
            }

            if (object instanceof Schedulable schedulable) {
                long calendarId = schedulable.getCalendarId();
                if (calendarId > 0) {
                    invalidatePermission(object.getClass(), object.getId(), Calendar.class, calendarId, true);
                }
            }
        }
    }
    
    // Helper methods for Redis serialization/deserialization
    private String serializeToJson(BaseModel object) {
        try {
            // Implementation would use Jackson or similar JSON library
            // For simplicity, we'll just return a placeholder
            return "{\"id\":"+object.getId()+"}";
        } catch (Exception e) {
            LOGGER.warn("Error serializing object to JSON", e);
            return null;
        }
    }
    
    private <T extends BaseModel> T deserializeFromJson(String json, Class<T> clazz) {
        try {
            // Implementation would use Jackson or similar JSON library
            // For simplicity, we'll just return null
            return null;
        } catch (Exception e) {
            LOGGER.warn("Error deserializing object from JSON", e);
            return null;
        }
    }
    
    // Distributed lock implementation using Redis
    public boolean acquireLock(String lockName, long timeoutMs) {
        if (redisConnection == null || !redisConnection.isOpen()) {
            LOGGER.warn("Redis not available for distributed locking");
            return true; // Fallback to allow operation without locking
        }
        
        try {
            return redisCircuitBreaker.executeSupplier(() -> {
                String lockKey = "lock:" + lockName;
                String lockValue = java.util.UUID.randomUUID().toString();
                String result = redisSync.set(lockKey, lockValue, "NX", "PX", timeoutMs);
                return "OK".equals(result);
            });
        } catch (Exception e) {
            LOGGER.warn("Error acquiring Redis lock", e);
            return true; // Fallback to allow operation without locking
        }
    }
    
    public boolean releaseLock(String lockName) {
        if (redisConnection == null || !redisConnection.isOpen()) {
            return true;
        }
        
        try {
            return redisCircuitBreaker.executeSupplier(() -> {
                String lockKey = "lock:" + lockName;
                Long result = redisSync.del(lockKey);
                return result != null && result > 0;
            });
        } catch (Exception e) {
            LOGGER.warn("Error releasing Redis lock", e);
            return false;
        }
    }
    
    // Cleanup resources on shutdown
    public void shutdown() {
        if (redisConnection != null) {
            try {
                redisConnection.close();
            } catch (Exception e) {
                LOGGER.warn("Error closing Redis connection", e);
            }
        }
        if (redisClient != null) {
            try {
                redisClient.shutdown();
            } catch (Exception e) {
                LOGGER.warn("Error shutting down Redis client", e);
            }
        }
    }
}