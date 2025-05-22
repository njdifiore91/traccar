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
package org.traccar.geocoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Geocoder implementation that uses dynamically discovered geocoding services through service discovery mechanisms.
 * It allows the system to use geocoding services that are registered with the service discovery system,
 * enabling flexible and resilient geocoding in a microservices architecture.
 */
public class ServiceBasedGeocoder extends JsonGeocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceBasedGeocoder.class);

    private final ServiceDiscoveryClient discoveryClient;
    private final Map<String, GeocoderServiceInstance> serviceInstances = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> requestCounters = new ConcurrentHashMap<>();
    private final CircuitBreakerManager circuitBreakerManager;
    private final String serviceType;
    private final int refreshInterval;
    private final int failoverThreshold;
    private final boolean enableLoadBalancing;
    private final String preferredProvider;
    
    private volatile long lastRefreshTime;

    /**
     * Creates a new instance of ServiceBasedGeocoder.
     *
     * @param client HTTP client for making requests
     * @param discoveryClient Service discovery client for finding geocoding services
     * @param cacheSize Size of the geocoding cache (0 to disable)
     * @param addressFormat Format for address display
     * @param serviceType Type of service to discover (e.g., "geocoder")
     * @param refreshInterval Interval in milliseconds to refresh service instances
     * @param failoverThreshold Number of failures before triggering failover
     * @param enableLoadBalancing Whether to enable load balancing across instances
     * @param preferredProvider Preferred geocoding provider (can be null)
     */
    public ServiceBasedGeocoder(Client client, 
                               ServiceDiscoveryClient discoveryClient,
                               int cacheSize, 
                               AddressFormat addressFormat,
                               String serviceType,
                               int refreshInterval,
                               int failoverThreshold,
                               boolean enableLoadBalancing,
                               String preferredProvider) {
        super(client, "", cacheSize, addressFormat); // URL will be dynamically determined
        this.discoveryClient = discoveryClient;
        this.serviceType = serviceType;
        this.refreshInterval = refreshInterval;
        this.failoverThreshold = failoverThreshold;
        this.enableLoadBalancing = enableLoadBalancing;
        this.preferredProvider = preferredProvider;
        this.circuitBreakerManager = new CircuitBreakerManager(failoverThreshold);
        
        refreshServiceInstances();
    }

    /**
     * Refreshes the list of available geocoding service instances from the service discovery system.
     */
    private synchronized void refreshServiceInstances() {
        try {
            List<GeocoderServiceInstance> instances = discoveryClient.getServiceInstances(serviceType);
            
            if (instances.isEmpty()) {
                LOGGER.warn("No geocoding service instances found for type: {}", serviceType);
                return;
            }
            
            // Update the service instances map
            Map<String, GeocoderServiceInstance> newInstances = new HashMap<>();
            for (GeocoderServiceInstance instance : instances) {
                newInstances.put(instance.getId(), instance);
                // Initialize request counter for new instances
                requestCounters.putIfAbsent(instance.getId(), new AtomicInteger(0));
            }
            
            // Remove instances that are no longer available
            List<String> removedInstances = serviceInstances.keySet().stream()
                    .filter(id -> !newInstances.containsKey(id))
                    .collect(Collectors.toList());
            
            for (String id : removedInstances) {
                serviceInstances.remove(id);
                requestCounters.remove(id);
                circuitBreakerManager.removeCircuitBreaker(id);
            }
            
            // Update with new instances
            serviceInstances.putAll(newInstances);
            
            lastRefreshTime = System.currentTimeMillis();
            LOGGER.debug("Refreshed geocoding service instances. Found {} instances.", serviceInstances.size());
        } catch (Exception e) {
            LOGGER.error("Failed to refresh geocoding service instances", e);
        }
    }

    /**
     * Selects a geocoding service instance based on availability, preferences, and load balancing.
     *
     * @return The selected geocoding service instance or null if none available
     */
    private GeocoderServiceInstance selectServiceInstance() {
        // Check if refresh is needed
        if (System.currentTimeMillis() - lastRefreshTime > refreshInterval) {
            refreshServiceInstances();
        }
        
        if (serviceInstances.isEmpty()) {
            LOGGER.warn("No geocoding service instances available");
            return null;
        }
        
        // First try preferred provider if specified
        if (preferredProvider != null && !preferredProvider.isEmpty()) {
            for (GeocoderServiceInstance instance : serviceInstances.values()) {
                if (preferredProvider.equals(instance.getProvider()) && 
                        circuitBreakerManager.isCircuitClosed(instance.getId())) {
                    return instance;
                }
            }
            LOGGER.debug("Preferred provider '{}' not available, falling back to other providers", preferredProvider);
        }
        
        // Filter available instances (circuit not open)
        List<GeocoderServiceInstance> availableInstances = serviceInstances.values().stream()
                .filter(instance -> circuitBreakerManager.isCircuitClosed(instance.getId()))
                .collect(Collectors.toList());
        
        if (availableInstances.isEmpty()) {
            LOGGER.warn("All geocoding service instances have open circuits, resetting one instance");
            // Reset one circuit breaker to allow retry
            if (!serviceInstances.isEmpty()) {
                String instanceId = serviceInstances.keySet().iterator().next();
                circuitBreakerManager.resetCircuitBreaker(instanceId);
                return serviceInstances.get(instanceId);
            }
            return null;
        }
        
        // Apply load balancing if enabled
        if (enableLoadBalancing && availableInstances.size() > 1) {
            return selectInstanceWithLoadBalancing(availableInstances);
        }
        
        // Default to first available instance
        return availableInstances.get(0);
    }

    /**
     * Selects a service instance using load balancing.
     *
     * @param availableInstances List of available service instances
     * @return The selected instance based on load balancing algorithm
     */
    private GeocoderServiceInstance selectInstanceWithLoadBalancing(List<GeocoderServiceInstance> availableInstances) {
        // Find instance with lowest request count (round-robin approach)
        GeocoderServiceInstance selectedInstance = null;
        int lowestCount = Integer.MAX_VALUE;
        
        for (GeocoderServiceInstance instance : availableInstances) {
            int count = requestCounters.get(instance.getId()).get();
            if (count < lowestCount) {
                lowestCount = count;
                selectedInstance = instance;
            }
        }
        
        // Increment request counter for selected instance
        if (selectedInstance != null) {
            requestCounters.get(selectedInstance.getId()).incrementAndGet();
        }
        
        return selectedInstance;
    }

    /**
     * Records a successful request to a service instance.
     *
     * @param instanceId ID of the service instance
     */
    private void recordSuccess(String instanceId) {
        circuitBreakerManager.recordSuccess(instanceId);
    }

    /**
     * Records a failed request to a service instance.
     *
     * @param instanceId ID of the service instance
     */
    private void recordFailure(String instanceId) {
        circuitBreakerManager.recordFailure(instanceId);
    }

    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        GeocoderServiceInstance instance = selectServiceInstance();
        
        if (instance == null) {
            String errorMessage = "No geocoding service instances available";
            if (callback != null) {
                callback.onFailure(new GeocoderException(errorMessage));
            } else {
                LOGGER.warn(errorMessage);
            }
            return null;
        }
        
        // Construct URL for the selected service instance
        String url = instance.getUrl();
        if (!url.contains("%f")) {
            // Add query parameters if not already in URL template
            url = url + (url.contains("?") ? "&" : "?") + "lat=%f&lon=%f";
        }
        
        // Create a wrapper callback to handle circuit breaker logic
        ReverseGeocoderCallback wrapperCallback = callback != null ? new ReverseGeocoderCallback() {
            @Override
            public void onSuccess(String address) {
                recordSuccess(instance.getId());
                callback.onSuccess(address);
            }

            @Override
            public void onFailure(Throwable e) {
                recordFailure(instance.getId());
                callback.onFailure(e);
            }
        } : null;
        
        // Use the parent class implementation with the dynamic URL
        return super.getAddress(latitude, longitude, wrapperCallback);
    }

    @Override
    public Address parseAddress(JsonObject json) {
        GeocoderServiceInstance instance = selectServiceInstance();
        
        if (instance == null) {
            LOGGER.warn("No geocoding service instances available for parsing address");
            return null;
        }
        
        // Delegate to the appropriate parser based on the provider type
        String provider = instance.getProvider();
        Address address;
        
        try {
            if ("nominatim".equalsIgnoreCase(provider)) {
                address = new NominatimGeocoder(getClient(), "", 0, null).parseAddress(json);
            } else if ("google".equalsIgnoreCase(provider)) {
                address = new GoogleGeocoder(getClient(), "", 0, null).parseAddress(json);
            } else if ("here".equalsIgnoreCase(provider)) {
                address = new HereGeocoder(getClient(), "", "", 0, null).parseAddress(json);
            } else if ("mapbox".equalsIgnoreCase(provider)) {
                address = new MapboxGeocoder(getClient(), "", 0, null).parseAddress(json);
            } else if ("gisgraphy".equalsIgnoreCase(provider)) {
                address = new GisgraphyGeocoder(getClient(), "", 0, null).parseAddress(json);
            } else {
                // Default to a generic parser if provider is unknown
                address = parseGenericAddress(json);
            }
            
            recordSuccess(instance.getId());
            return address;
        } catch (Exception e) {
            LOGGER.warn("Error parsing address with provider {}: {}", provider, e.getMessage());
            recordFailure(instance.getId());
            return null;
        }
    }

    /**
     * Generic address parser for unknown provider types.
     * Attempts to extract common address fields from JSON response.
     *
     * @param json The JSON response to parse
     * @return Parsed address or null if parsing failed
     */
    private Address parseGenericAddress(JsonObject json) {
        Address address = new Address();
        
        // Try common field names for address components
        trySetField(json, address, "formatted_address", "formattedAddress", "display_name", Address::setFormattedAddress);
        trySetField(json, address, "house_number", "houseNumber", "housenumber", Address::setHouse);
        trySetField(json, address, "road", "street", "thoroughfare", Address::setStreet);
        trySetField(json, address, "suburb", "district", "neighborhood", Address::setSuburb);
        trySetField(json, address, "city", "town", "locality", Address::setSettlement);
        trySetField(json, address, "state", "province", "region", Address::setState);
        trySetField(json, address, "postcode", "postal_code", "zip", Address::setPostcode);
        trySetField(json, address, "country", "country_name", "nation", Address::setCountry);
        
        return address;
    }

    /**
     * Helper method to try setting an address field from multiple possible JSON field names.
     *
     * @param json The JSON object to extract from
     * @param address The address object to update
     * @param fieldName1 First possible field name
     * @param fieldName2 Second possible field name
     * @param fieldName3 Third possible field name
     * @param setter Method reference to the appropriate address setter
     */
    private void trySetField(JsonObject json, Address address, String fieldName1, String fieldName2, String fieldName3, 
                            java.util.function.Consumer<String> setter) {
        String value = null;
        
        if (json.containsKey(fieldName1) && !json.isNull(fieldName1)) {
            value = json.getString(fieldName1);
        } else if (json.containsKey(fieldName2) && !json.isNull(fieldName2)) {
            value = json.getString(fieldName2);
        } else if (json.containsKey(fieldName3) && !json.isNull(fieldName3)) {
            value = json.getString(fieldName3);
        }
        
        if (value != null && !value.isEmpty()) {
            setter.accept(value);
        }
    }

    /**
     * Gets the HTTP client used for requests.
     *
     * @return The HTTP client
     */
    private Client getClient() {
        return super.client;
    }

    /**
     * Interface for service discovery client implementations.
     */
    public interface ServiceDiscoveryClient {
        /**
         * Gets service instances of a specific type.
         *
         * @param serviceType The type of service to discover
         * @return List of service instances
         */
        List<GeocoderServiceInstance> getServiceInstances(String serviceType);
    }

    /**
     * Represents a discovered geocoding service instance.
     */
    public static class GeocoderServiceInstance {
        private final String id;
        private final String url;
        private final String provider;
        private final Map<String, String> metadata;

        public GeocoderServiceInstance(String id, String url, String provider, Map<String, String> metadata) {
            this.id = id;
            this.url = url;
            this.provider = provider;
            this.metadata = metadata != null ? metadata : new HashMap<>();
        }

        public String getId() {
            return id;
        }

        public String getUrl() {
            return url;
        }

        public String getProvider() {
            return provider;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        public String getMetadata(String key) {
            return metadata.get(key);
        }
    }

    /**
     * Manages circuit breakers for service instances to implement failover.
     */
    private static class CircuitBreakerManager {
        private final Map<String, CircuitBreaker> circuitBreakers = new ConcurrentHashMap<>();
        private final int failureThreshold;

        public CircuitBreakerManager(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }

        public boolean isCircuitClosed(String instanceId) {
            CircuitBreaker breaker = circuitBreakers.computeIfAbsent(instanceId, 
                    id -> new CircuitBreaker(failureThreshold));
            return breaker.isClosed();
        }

        public void recordSuccess(String instanceId) {
            CircuitBreaker breaker = circuitBreakers.get(instanceId);
            if (breaker != null) {
                breaker.recordSuccess();
            }
        }

        public void recordFailure(String instanceId) {
            CircuitBreaker breaker = circuitBreakers.computeIfAbsent(instanceId, 
                    id -> new CircuitBreaker(failureThreshold));
            breaker.recordFailure();
        }

        public void resetCircuitBreaker(String instanceId) {
            CircuitBreaker breaker = circuitBreakers.get(instanceId);
            if (breaker != null) {
                breaker.reset();
            }
        }

        public void removeCircuitBreaker(String instanceId) {
            circuitBreakers.remove(instanceId);
        }
    }

    /**
     * Simple circuit breaker implementation for failover.
     */
    private static class CircuitBreaker {
        private final int failureThreshold;
        private AtomicInteger failureCount = new AtomicInteger(0);
        private volatile boolean open = false;
        private volatile long openTimestamp = 0;
        private static final long RESET_TIMEOUT_MS = 60000; // 1 minute

        public CircuitBreaker(int failureThreshold) {
            this.failureThreshold = failureThreshold;
        }

        public boolean isClosed() {
            if (open) {
                // Check if reset timeout has elapsed
                if (System.currentTimeMillis() - openTimestamp > RESET_TIMEOUT_MS) {
                    reset();
                    return true;
                }
                return false;
            }
            return true;
        }

        public void recordSuccess() {
            failureCount.set(0);
            open = false;
        }

        public void recordFailure() {
            int failures = failureCount.incrementAndGet();
            if (failures >= failureThreshold) {
                open = true;
                openTimestamp = System.currentTimeMillis();
            }
        }

        public void reset() {
            failureCount.set(0);
            open = false;
        }
    }
}