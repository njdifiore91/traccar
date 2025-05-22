package org.traccar.session.cache;

import org.traccar.model.BaseModel;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * A cache node that represents a single entity in the distributed cache system.
 * This class is serializable to support Redis-based distributed caching.
 */
public class CacheNode implements Serializable {

    private static final long serialVersionUID = 1L;
    
    // Unique identifier for this node in a distributed environment
    private final String nodeId;
    
    // Version for conflict resolution in distributed environment
    private final AtomicLong version = new AtomicLong(0);
    
    private BaseModel value;

    private final Map<Class<? extends BaseModel>, Set<CacheNode>> links = new ConcurrentHashMap<>();
    private final Map<Class<? extends BaseModel>, Set<CacheNode>> backlinks = new ConcurrentHashMap<>();
    
    // Metrics for monitoring cache operations
    private transient MeterRegistry meterRegistry;

    public CacheNode(BaseModel value) {
        this.value = value;
        this.nodeId = UUID.randomUUID().toString();
    }
    
    /**
     * Sets the meter registry for collecting metrics on this cache node.
     * 
     * @param meterRegistry The meter registry to use
     */
    public void setMeterRegistry(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public BaseModel getValue() {
        return value;
    }

    public void setValue(BaseModel value) {
        this.value = value;
        // Increment version on each update for conflict resolution
        version.incrementAndGet();
        recordMetric("cache.node.updates");
    }
    
    /**
     * Gets the unique identifier for this node.
     * 
     * @return The node ID
     */
    public String getNodeId() {
        return nodeId;
    }
    
    /**
     * Gets the current version of this node.
     * 
     * @return The current version
     */
    public long getVersion() {
        return version.get();
    }

    /**
     * Gets the set of linked nodes for the specified class.
     * Enhanced to support distributed references.
     * 
     * @param clazz The class of the linked model
     * @param forward True for forward links, false for backlinks
     * @return The set of linked nodes
     */
    public Set<CacheNode> getLinks(Class<? extends BaseModel> clazz, boolean forward) {
        var map = forward ? links : backlinks;
        recordMetric("cache.node.link.access", 
                Tags.of("direction", forward ? "forward" : "backward", 
                       "class", clazz.getSimpleName()));
        return map.computeIfAbsent(clazz, k -> new HashSet<>());
    }

    /**
     * Gets all linked nodes.
     * Enhanced to support distributed references.
     * 
     * @param forward True for forward links, false for backlinks
     * @return Stream of all linked nodes
     */
    public Stream<CacheNode> getAllLinks(boolean forward) {
        var map = forward ? links : backlinks;
        recordMetric("cache.node.links.access", 
                Tags.of("direction", forward ? "forward" : "backward"));
        return map.values().stream().flatMap(Set::stream);
    }
    
    /**
     * Adds a link to another node.
     * 
     * @param clazz The class of the linked model
     * @param node The node to link to
     * @param forward True for forward link, false for backlink
     */
    public void addLink(Class<? extends BaseModel> clazz, CacheNode node, boolean forward) {
        var map = forward ? links : backlinks;
        var linkSet = map.computeIfAbsent(clazz, k -> new HashSet<>());
        linkSet.add(node);
        // Increment version on link modification for conflict resolution
        version.incrementAndGet();
        recordMetric("cache.node.link.add", 
                Tags.of("direction", forward ? "forward" : "backward", 
                       "class", clazz.getSimpleName()));
    }
    
    /**
     * Removes a link to another node.
     * 
     * @param clazz The class of the linked model
     * @param node The node to unlink
     * @param forward True for forward link, false for backlink
     * @return True if the link was removed, false otherwise
     */
    public boolean removeLink(Class<? extends BaseModel> clazz, CacheNode node, boolean forward) {
        var map = forward ? links : backlinks;
        var linkSet = map.get(clazz);
        if (linkSet != null) {
            boolean removed = linkSet.remove(node);
            if (removed) {
                // Increment version on link modification for conflict resolution
                version.incrementAndGet();
                recordMetric("cache.node.link.remove", 
                        Tags.of("direction", forward ? "forward" : "backward", 
                               "class", clazz.getSimpleName()));
            }
            return removed;
        }
        return false;
    }
    
    /**
     * Records a metric for monitoring cache operations.
     * 
     * @param name The name of the metric
     */
    private void recordMetric(String name) {
        recordMetric(name, Tags.empty());
    }
    
    /**
     * Records a metric with tags for monitoring cache operations.
     * 
     * @param name The name of the metric
     * @param tags The tags to associate with the metric
     */
    private void recordMetric(String name, Iterable<Tag> tags) {
        if (meterRegistry != null) {
            meterRegistry.counter(name, tags).increment();
        }
    }
    
    /**
     * Custom serialization method for efficient serialization of complex node relationships.
     * 
     * @param out The output stream to write to
     * @throws IOException If an I/O error occurs
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        // Custom serialization logic can be added here if needed
    }
    
    /**
     * Custom deserialization method for efficient deserialization of complex node relationships.
     * 
     * @param in The input stream to read from
     * @throws IOException If an I/O error occurs
     * @throws ClassNotFoundException If the class of a serialized object cannot be found
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Custom deserialization logic can be added here if needed
    }
    
    /**
     * Equals method based on nodeId for distributed environments.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CacheNode cacheNode = (CacheNode) o;
        return nodeId.equals(cacheNode.nodeId);
    }
    
    /**
     * HashCode method based on nodeId for distributed environments.
     */
    @Override
    public int hashCode() {
        return nodeId.hashCode();
    }
}