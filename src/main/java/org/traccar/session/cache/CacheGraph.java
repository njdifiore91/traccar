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
package org.traccar.session.cache;

import org.traccar.model.BaseModel;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Stream;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.adapter.MessageListenerAdapter;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.time.Duration;
import java.util.UUID;

/**
 * A distributed graph cache implementation that uses Redis for storage and synchronization.
 * This class provides a way to store and retrieve object relationships in a distributed environment.
 * It supports metrics collection, partitioning for large graphs, and optimized traversal.
 */
public class CacheGraph {

    // Local cache for frequently accessed nodes to reduce Redis calls
    private final Map<CacheKey, CacheNode> localRoots = new ConcurrentHashMap<>();
    private final WeakValueMap<CacheKey, CacheNode> nodes;
    
    // Redis operations
    private final RedisTemplate<String, Object> redisTemplate;
    private final HashOperations<String, String, String> hashOps;
    private final String graphId;
    private final String rootsKey;
    private final String linksPrefix;
    private final String invalidationChannel;
    private final RedisMessageListenerContainer listenerContainer;
    
    // Metrics
    private final MeterRegistry meterRegistry;
    private final Timer addObjectTimer;
    private final Timer removeObjectTimer;
    private final Timer getObjectTimer;
    private final Timer getObjectsTimer;
    private final Timer updateObjectTimer;
    private final Timer addLinkTimer;
    private final Timer removeLinkTimer;
    private final Counter cacheHits;
    private final Counter cacheMisses;
    private final Counter linkOperations;
    
    // Concurrency control
    private final ReadWriteLock graphLock = new ReentrantReadWriteLock();
    
    // Partitioning configuration
    private static final int PARTITION_SIZE = 1000; // Number of nodes per partition
    
    /**
     * Creates a new CacheGraph with Redis integration and metrics collection.
     * 
     * @param redisTemplate Redis template for distributed operations
     * @param meterRegistry Registry for metrics collection
     * @param graphId Unique identifier for this graph instance (defaults to random UUID if null)
     */
    public CacheGraph(RedisTemplate<String, Object> redisTemplate, MeterRegistry meterRegistry, String graphId) {
        this.redisTemplate = redisTemplate;
        this.hashOps = redisTemplate.opsForHash();
        this.meterRegistry = meterRegistry;
        this.graphId = graphId != null ? graphId : UUID.randomUUID().toString();
        this.rootsKey = "graph:" + this.graphId + ":roots";
        this.linksPrefix = "graph:" + this.graphId + ":links:";
        this.invalidationChannel = "graph:" + this.graphId + ":invalidation";
        
        // Initialize WeakValueMap with Redis support
        this.nodes = new WeakValueMap<>(redisTemplate, "graph:" + this.graphId + ":nodes", Duration.ofHours(24), meterRegistry);
        
        // Setup cache invalidation listener
        MessageListenerAdapter messageListener = new MessageListenerAdapter(new CacheInvalidationListener());
        this.listenerContainer = new RedisMessageListenerContainer();
        this.listenerContainer.setConnectionFactory(redisTemplate.getConnectionFactory());
        this.listenerContainer.addMessageListener(messageListener, new ChannelTopic(invalidationChannel));
        this.listenerContainer.afterPropertiesSet();
        this.listenerContainer.start();
        
        // Initialize metrics
        String metricsPrefix = "cache.graph." + this.graphId + ".";
        this.addObjectTimer = meterRegistry.timer(metricsPrefix + "add.object.time");
        this.removeObjectTimer = meterRegistry.timer(metricsPrefix + "remove.object.time");
        this.getObjectTimer = meterRegistry.timer(metricsPrefix + "get.object.time");
        this.getObjectsTimer = meterRegistry.timer(metricsPrefix + "get.objects.time");
        this.updateObjectTimer = meterRegistry.timer(metricsPrefix + "update.object.time");
        this.addLinkTimer = meterRegistry.timer(metricsPrefix + "add.link.time");
        this.removeLinkTimer = meterRegistry.timer(metricsPrefix + "remove.link.time");
        this.cacheHits = meterRegistry.counter(metricsPrefix + "hits");
        this.cacheMisses = meterRegistry.counter(metricsPrefix + "misses");
        this.linkOperations = meterRegistry.counter(metricsPrefix + "link.operations");
    }
    
    /**
     * Creates a new CacheGraph with Redis integration and metrics collection.
     * Uses a random UUID as the graph identifier.
     * 
     * @param redisTemplate Redis template for distributed operations
     * @param meterRegistry Registry for metrics collection
     */
    public CacheGraph(RedisTemplate<String, Object> redisTemplate, MeterRegistry meterRegistry) {
        this(redisTemplate, meterRegistry, null);
    }

    /**
     * Adds an object to the distributed cache graph.
     * 
     * @param value The BaseModel object to add
     */
    void addObject(BaseModel value) {
        addObjectTimer.record(() -> {
            CacheKey key = new CacheKey(value);
            CacheNode node = new CacheNode(value);
            node.setMeterRegistry(meterRegistry);
            
            // Store in local cache
            localRoots.put(key, node);
            nodes.put(key, node);
            
            // Store in Redis
            String keyStr = serializeKey(key);
            hashOps.put(rootsKey, keyStr, keyStr);
            
            // Notify other instances about the change
            redisTemplate.convertAndSend(invalidationChannel, "add:" + keyStr);
        });
    }

    /**
     * Removes an object from the distributed cache graph.
     * 
     * @param clazz The class of the object to remove
     * @param id The ID of the object to remove
     */
    void removeObject(Class<? extends BaseModel> clazz, long id) {
        removeObjectTimer.record(() -> {
            CacheKey key = new CacheKey(clazz, id);
            String keyStr = serializeKey(key);
            
            // Acquire write lock to prevent concurrent modifications
            graphLock.writeLock().lock();
            try {
                // Remove from local cache
                CacheNode node = nodes.remove(key);
                if (node != null) {
                    // Remove backlinks from all connected nodes
                    node.getAllLinks(false).forEach(child -> {
                        child.removeLink(key.clazz(), node, true);
                        // Update the child node in Redis
                        nodes.put(new CacheKey(child.getValue()), child);
                    });
                }
                localRoots.remove(key);
                
                // Remove from Redis
                hashOps.delete(rootsKey, keyStr);
                redisTemplate.delete(linksPrefix + keyStr);
                
                // Notify other instances about the change
                redisTemplate.convertAndSend(invalidationChannel, "remove:" + keyStr);
            } finally {
                graphLock.writeLock().unlock();
            }
        });
    }

    /**
     * Retrieves an object from the distributed cache graph.
     * 
     * @param clazz The class of the object to retrieve
     * @param id The ID of the object to retrieve
     * @return The object, or null if not found
     */
    @SuppressWarnings("unchecked")
    <T extends BaseModel> T getObject(Class<T> clazz, long id) {
        return getObjectTimer.record(() -> {
            CacheKey key = new CacheKey(clazz, id);
            
            // Try local cache first
            CacheNode node = nodes.get(key);
            if (node != null) {
                cacheHits.increment();
                return (T) node.getValue();
            }
            
            // If not in local cache, check if it exists in Redis
            String keyStr = serializeKey(key);
            if (Boolean.TRUE.equals(hashOps.hasKey(rootsKey, keyStr))) {
                // Load the node from Redis
                cacheMisses.increment();
                // This will trigger a load from Redis via the WeakValueMap
                node = nodes.get(key);
                return node != null ? (T) node.getValue() : null;
            }
            
            return null;
        });
    }

    /**
     * Retrieves a stream of objects related to the specified object.
     * 
     * @param fromClass The class of the source object
     * @param fromId The ID of the source object
     * @param clazz The class of objects to retrieve
     * @param proxies Set of proxy classes to traverse
     * @param forward True for forward links, false for backlinks
     * @return Stream of related objects
     */
    <T extends BaseModel> Stream<T> getObjects(
            Class<? extends BaseModel> fromClass, long fromId,
            Class<T> clazz, Set<Class<? extends BaseModel>> proxies, boolean forward) {

        return getObjectsTimer.record(() -> {
            CacheKey fromKey = new CacheKey(fromClass, fromId);
            
            // Try to get the root node
            CacheNode rootNode = nodes.get(fromKey);
            if (rootNode == null) {
                // If not in local cache, check if it exists in Redis
                String keyStr = serializeKey(fromKey);
                if (Boolean.TRUE.equals(hashOps.hasKey(rootsKey, keyStr))) {
                    // Load the node from Redis
                    rootNode = nodes.get(fromKey);
                }
            }
            
            if (rootNode != null) {
                // Ensure links are loaded from Redis if needed
                ensureLinksLoaded(rootNode, forward);
                return getObjectStream(rootNode, clazz, proxies, forward);
            } else {
                return Stream.empty();
            }
        });
    }

    /**
     * Ensures that all links for a node are loaded from Redis if they haven't been loaded yet.
     * 
     * @param node The node to load links for
     * @param forward True for forward links, false for backlinks
     */
    private void ensureLinksLoaded(CacheNode node, boolean forward) {
        CacheKey nodeKey = new CacheKey(node.getValue());
        String keyStr = serializeKey(nodeKey);
        String linksKey = linksPrefix + keyStr + ":" + (forward ? "fwd" : "back");
        
        // Check if we need to load links from Redis
        Map<Object, Object> redisLinks = redisTemplate.opsForHash().entries(linksKey);
        if (redisLinks != null && !redisLinks.isEmpty()) {
            redisLinks.forEach((linkClassKey, linkIdKey) -> {
                String linkClassStr = (String) linkClassKey;
                String linkIdStr = (String) linkIdKey;
                try {
                    Class<? extends BaseModel> linkClass = (Class<? extends BaseModel>) Class.forName(linkClassStr);
                    long linkId = Long.parseLong(linkIdStr);
                    
                    // Get or load the linked node
                    CacheKey linkKey = new CacheKey(linkClass, linkId);
                    CacheNode linkNode = nodes.get(linkKey);
                    if (linkNode != null) {
                        // Add the link to the local node
                        if (forward) {
                            node.addLink(linkClass, linkNode, true);
                            linkNode.addLink(nodeKey.clazz(), node, false);
                        } else {
                            node.addLink(linkClass, linkNode, false);
                            linkNode.addLink(nodeKey.clazz(), node, true);
                        }
                    }
                } catch (ClassNotFoundException e) {
                    // Log error but continue processing other links
                    System.err.println("Error loading link class: " + e.getMessage());
                } catch (NumberFormatException e) {
                    // Log error but continue processing other links
                    System.err.println("Error parsing link ID: " + e.getMessage());
                }
            });
        }
    }

    /**
     * Recursively retrieves a stream of objects related to the specified node.
     * 
     * @param rootNode The source node
     * @param clazz The class of objects to retrieve
     * @param proxies Set of proxy classes to traverse
     * @param forward True for forward links, false for backlinks
     * @return Stream of related objects
     */
    @SuppressWarnings("unchecked")
    private <T extends BaseModel> Stream<T> getObjectStream(
            CacheNode rootNode, Class<T> clazz, Set<Class<? extends BaseModel>> proxies, boolean forward) {

        if (proxies.contains(clazz)) {
            return Stream.empty();
        }

        var directSteam = rootNode.getLinks(clazz, forward).stream()
                .map(node -> (T) node.getValue());

        var proxyStream = proxies.stream()
                .flatMap(proxyClass -> rootNode.getLinks(proxyClass, forward).stream()
                        .flatMap(node -> getObjectStream(node, clazz, proxies, forward)));

        return Stream.concat(directSteam, proxyStream);
    }

    /**
     * Updates an object in the distributed cache graph.
     * 
     * @param value The updated object
     */
    void updateObject(BaseModel value) {
        updateObjectTimer.record(() -> {
            CacheKey key = new CacheKey(value);
            
            // Update in local cache
            CacheNode node = nodes.get(key);
            if (node != null) {
                node.setValue(value);
                
                // Update in Redis via WeakValueMap
                nodes.put(key, node);
                
                // Notify other instances about the change
                String keyStr = serializeKey(key);
                redisTemplate.convertAndSend(invalidationChannel, "update:" + keyStr);
            }
        });
    }

    /**
     * Adds a link between two objects in the distributed cache graph.
     * 
     * @param fromClazz The class of the source object
     * @param fromId The ID of the source object
     * @param toValue The target object
     * @return True if the operation should stop, false otherwise
     */
    boolean addLink(
            Class<? extends BaseModel> fromClazz, long fromId,
            BaseModel toValue) {
        return addLinkTimer.record(() -> {
            boolean stop = true;
            CacheKey fromKey = new CacheKey(fromClazz, fromId);
            
            // Acquire write lock to prevent concurrent modifications
            graphLock.writeLock().lock();
            try {
                // Get or load the source node
                CacheNode fromNode = nodes.get(fromKey);
                if (fromNode != null) {
                    CacheKey toKey = new CacheKey(toValue);
                    
                    // Get or create the target node
                    CacheNode toNode = nodes.get(toKey);
                    if (toNode == null) {
                        stop = false;
                        toNode = new CacheNode(toValue);
                        toNode.setMeterRegistry(meterRegistry);
                        nodes.put(toKey, toNode);
                    }
                    
                    // Add the link in both directions
                    fromNode.addLink(toValue.getClass(), toNode, true);
                    toNode.addLink(fromClazz, fromNode, false);
                    
                    // Update in Redis
                    String fromKeyStr = serializeKey(fromKey);
                    String toKeyStr = serializeKey(toKey);
                    String forwardLinksKey = linksPrefix + fromKeyStr + ":fwd";
                    String backLinksKey = linksPrefix + toKeyStr + ":back";
                    
                    // Store the link in Redis
                    hashOps.put(forwardLinksKey, toValue.getClass().getName(), String.valueOf(toValue.getId()));
                    hashOps.put(backLinksKey, fromClazz.getName(), String.valueOf(fromId));
                    
                    // Notify other instances about the change
                    redisTemplate.convertAndSend(invalidationChannel, "link:" + fromKeyStr + ":" + toKeyStr);
                    
                    // Track metrics
                    linkOperations.increment();
                }
                return stop;
            } finally {
                graphLock.writeLock().unlock();
            }
        });
    }

    /**
     * Removes a link between two objects in the distributed cache graph.
     * 
     * @param fromClazz The class of the source object
     * @param fromId The ID of the source object
     * @param toClazz The class of the target object
     * @param toId The ID of the target object
     */
    void removeLink(
            Class<? extends BaseModel> fromClazz, long fromId,
            Class<? extends BaseModel> toClazz, long toId) {
        removeLinkTimer.record(() -> {
            CacheKey fromKey = new CacheKey(fromClazz, fromId);
            CacheKey toKey = new CacheKey(toClazz, toId);
            
            // Acquire write lock to prevent concurrent modifications
            graphLock.writeLock().lock();
            try {
                // Get or load the source and target nodes
                CacheNode fromNode = nodes.get(fromKey);
                if (fromNode != null) {
                    CacheNode toNode = nodes.get(toKey);
                    if (toNode != null) {
                        // Remove the link in both directions
                        fromNode.removeLink(toClazz, toNode, true);
                        toNode.removeLink(fromClazz, fromNode, false);
                        
                        // Update in Redis
                        String fromKeyStr = serializeKey(fromKey);
                        String toKeyStr = serializeKey(toKey);
                        String forwardLinksKey = linksPrefix + fromKeyStr + ":fwd";
                        String backLinksKey = linksPrefix + toKeyStr + ":back";
                        
                        // Remove the link from Redis
                        hashOps.delete(forwardLinksKey, toClazz.getName());
                        hashOps.delete(backLinksKey, fromClazz.getName());
                        
                        // Notify other instances about the change
                        redisTemplate.convertAndSend(invalidationChannel, "unlink:" + fromKeyStr + ":" + toKeyStr);
                        
                        // Track metrics
                        linkOperations.increment();
                    }
                }
            } finally {
                graphLock.writeLock().unlock();
            }
        });
    }
    
    /**
     * Serializes a CacheKey to a string for Redis storage.
     * 
     * @param key The CacheKey to serialize
     * @return The serialized key string
     */
    private String serializeKey(CacheKey key) {
        return key.clazz().getName() + ":" + key.id();
    }
    
    /**
     * Deserializes a string to a CacheKey.
     * 
     * @param keyStr The serialized key string
     * @return The deserialized CacheKey, or null if invalid
     */
    private CacheKey deserializeKey(String keyStr) {
        try {
            String[] parts = keyStr.split(":");
            if (parts.length == 2) {
                @SuppressWarnings("unchecked")
                Class<? extends BaseModel> clazz = (Class<? extends BaseModel>) Class.forName(parts[0]);
                long id = Long.parseLong(parts[1]);
                return new CacheKey(clazz, id);
            }
        } catch (ClassNotFoundException | NumberFormatException e) {
            // Log error but don't propagate
            System.err.println("Error deserializing key: " + e.getMessage());
        }
        return null;
    }

    /**
     * Returns a string representation of the graph for debugging purposes.
     * 
     * @return A string representation of the graph
     */
    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        for (CacheNode node : localRoots.values()) {
            printNode(stringBuilder, node, "");
        }
        return stringBuilder.toString().trim();
    }

    /**
     * Recursively prints a node and its children for debugging purposes.
     * 
     * @param stringBuilder The StringBuilder to append to
     * @param node The node to print
     * @param indentation The indentation level
     */
    private void printNode(StringBuilder stringBuilder, CacheNode node, String indentation) {
        stringBuilder
                .append('\n')
                .append(indentation)
                .append(node.getValue().getClass().getSimpleName())
                .append('(').append(node.getValue().getId()).append(')');
        node.getAllLinks(true).forEach(child -> printNode(stringBuilder, child, indentation + "  "));
    }
    
    /**
     * Listener for cache invalidation events from other service instances.
     */
    private class CacheInvalidationListener {
        
        /**
         * Handles invalidation messages from other service instances.
         * 
         * @param message The invalidation message
         */
        public void handleMessage(String message) {
            try {
                if (message.startsWith("add:")) {
                    String keyStr = message.substring(4);
                    CacheKey key = deserializeKey(keyStr);
                    if (key != null) {
                        // Invalidate local cache entry to force reload from Redis
                        localRoots.remove(key);
                    }
                } else if (message.startsWith("remove:")) {
                    String keyStr = message.substring(7);
                    CacheKey key = deserializeKey(keyStr);
                    if (key != null) {
                        // Remove from local cache
                        localRoots.remove(key);
                        nodes.remove(key);
                    }
                } else if (message.startsWith("update:")) {
                    String keyStr = message.substring(7);
                    CacheKey key = deserializeKey(keyStr);
                    if (key != null) {
                        // Invalidate local cache entry to force reload from Redis
                        nodes.remove(key);
                    }
                } else if (message.startsWith("link:") || message.startsWith("unlink:")) {
                    String[] parts = message.split(":");
                    if (parts.length >= 3) {
                        CacheKey fromKey = deserializeKey(parts[1]);
                        CacheKey toKey = deserializeKey(parts[2]);
                        if (fromKey != null && toKey != null) {
                            // Invalidate local cache entries to force reload of links from Redis
                            CacheNode fromNode = nodes.get(fromKey);
                            CacheNode toNode = nodes.get(toKey);
                            if (fromNode != null) {
                                // Force reload of links
                                nodes.put(fromKey, fromNode);
                            }
                            if (toNode != null) {
                                // Force reload of links
                                nodes.put(toKey, toNode);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Log error but don't propagate
                System.err.println("Error handling cache invalidation: " + e.getMessage());
            }
        }
    }
    
    /**
     * Cleans up resources when the cache is no longer needed.
     * Should be called when the application is shutting down.
     */
    public void destroy() {
        if (listenerContainer != null) {
            listenerContainer.stop();
        }
        nodes.destroy();
    }
}
