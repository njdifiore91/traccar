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
package org.traccar.geocoder;

import com.fasterxml.jackson.databind.JsonNode;
 import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.database.StatisticsManager;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Service discovery implementation for geocoding services.
 * Supports both Consul and Kubernetes service discovery mechanisms.
 */
@Singleton
public class GeocoderServiceDiscovery implements Geocoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeocoderServiceDiscovery.class);

    private static final String CONSUL_API_URL = "http://consul:8500/v1/catalog/service/";
    private static final String KUBERNETES_API_URL = "https://kubernetes.default.svc/api/v1/namespaces/%s/endpoints/%s";
    private static final String KUBERNETES_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token";
    private static final String KUBERNETES_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace";

    private final String serviceName;
    private final String discoveryType;
    private final String kubernetesNamespace;
    private final long refreshInterval;
    private final ObjectMapper objectMapper;
    private final Map<String, GeocoderEndpoint> endpoints;
    private final ScheduledExecutorService executorService;
    private StatisticsManager statisticsManager;

    private static class GeocoderEndpoint {
        private final String url;
        private final Map<String, String> metadata;
        private boolean healthy;

        public GeocoderEndpoint(String url, Map<String, String> metadata) {
            this.url = url;
            this.metadata = metadata;
            this.healthy = true;
        }

        public String getUrl() {
            return url;
        }

        public Map<String, String> getMetadata() {
            return metadata;
        }

        public boolean isHealthy() {
            return healthy;
        }

        public void setHealthy(boolean healthy) {
            this.healthy = healthy;
        }
    }

    /**
     * Creates a new GeocoderServiceDiscovery instance.
     *
     * @param serviceName The name of the geocoding service to discover
     * @param discoveryType The type of service discovery to use ("consul" or "kubernetes")
     * @param refreshInterval The interval in seconds at which to refresh the service discovery
     */
    @Inject
    public GeocoderServiceDiscovery(
            String serviceName,
            String discoveryType,
            long refreshInterval) {
        this.serviceName = serviceName;
        this.discoveryType = discoveryType.toLowerCase();
        this.refreshInterval = refreshInterval;
        this.objectMapper = new ObjectMapper();
        this.endpoints = new ConcurrentHashMap<>();
        this.executorService = Executors.newSingleThreadScheduledExecutor();
        this.kubernetesNamespace = readKubernetesNamespace();

        // Start the service discovery refresh task
        executorService.scheduleAtFixedRate(
                this::refreshServiceDiscovery,
                0,
                refreshInterval,
                TimeUnit.SECONDS);
    }

    /**
     * Refreshes the service discovery by querying the service registry.
     */
    private void refreshServiceDiscovery() {
        try {
            List<GeocoderEndpoint> discoveredEndpoints = new ArrayList<>();

            if ("consul".equals(discoveryType)) {
                discoveredEndpoints = discoverWithConsul();
            } else if ("kubernetes".equals(discoveryType)) {
                discoveredEndpoints = discoverWithKubernetes();
            } else {
                LOGGER.warn("Unknown service discovery type: {}", discoveryType);
                return;
            }

            // Update the endpoints map
            updateEndpoints(discoveredEndpoints);

            // Perform health checks on all endpoints
            checkEndpointsHealth();

            LOGGER.debug("Discovered {} geocoder endpoints", endpoints.size());
        } catch (Exception e) {
            LOGGER.error("Error refreshing service discovery", e);
        }
    }

    /**
     * Discovers geocoder endpoints using Consul service discovery.
     *
     * @return A list of discovered geocoder endpoints
     * @throws IOException If an error occurs during discovery
     */
    private List<GeocoderEndpoint> discoverWithConsul() throws IOException {
        List<GeocoderEndpoint> discoveredEndpoints = new ArrayList<>();
        URL url = new URL(CONSUL_API_URL + serviceName);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        try {
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");

            if (connection.getResponseCode() != 200) {
                LOGGER.warn("Failed to discover services with Consul: HTTP error code {}", 
                        connection.getResponseCode());
                return discoveredEndpoints;
            }

            JsonNode servicesNode = objectMapper.readTree(connection.getInputStream());
            if (servicesNode.isArray()) {
                for (JsonNode serviceNode : servicesNode) {
                    String address = serviceNode.path("ServiceAddress").asText();
                    int port = serviceNode.path("ServicePort").asInt();
                    String serviceUrl = String.format("http://%s:%d", address, port);

                    Map<String, String> metadata = new ConcurrentHashMap<>();
                    JsonNode tagsNode = serviceNode.path("ServiceTags");
                    if (tagsNode.isArray()) {
                        for (JsonNode tagNode : tagsNode) {
                            String tag = tagNode.asText();
                            if (tag.contains("=")) {
                                String[] parts = tag.split("=", 2);
                                metadata.put(parts[0], parts[1]);
                            }
                        }
                    }

                    discoveredEndpoints.add(new GeocoderEndpoint(serviceUrl, metadata));
                }
            }
        } finally {
            connection.disconnect();
        }

        return discoveredEndpoints;
    }

    /**
     * Discovers geocoder endpoints using Kubernetes service discovery.
     *
     * @return A list of discovered geocoder endpoints
     * @throws IOException If an error occurs during discovery
     */
    private List<GeocoderEndpoint> discoverWithKubernetes() throws IOException {
        List<GeocoderEndpoint> discoveredEndpoints = new ArrayList<>();

        if (kubernetesNamespace == null) {
            LOGGER.warn("Kubernetes namespace not found");
            return discoveredEndpoints;
        }

        String token = readKubernetesToken();
        if (token == null) {
            LOGGER.warn("Kubernetes token not found");
            return discoveredEndpoints;
        }

        URL url = new URL(String.format(KUBERNETES_API_URL, kubernetesNamespace, serviceName));
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();

        try {
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            connection.setRequestProperty("Authorization", "Bearer " + token);

            if (connection.getResponseCode() != 200) {
                LOGGER.warn("Failed to discover services with Kubernetes: HTTP error code {}", 
                        connection.getResponseCode());
                return discoveredEndpoints;
            }

            JsonNode endpointsNode = objectMapper.readTree(connection.getInputStream());
            JsonNode subsetsNode = endpointsNode.path("subsets");

            if (subsetsNode.isArray()) {
                for (JsonNode subsetNode : subsetsNode) {
                    JsonNode addressesNode = subsetNode.path("addresses");
                    JsonNode portsNode = subsetNode.path("ports");

                    if (addressesNode.isArray() && portsNode.isArray()) {
                        for (JsonNode addressNode : addressesNode) {
                            String ip = addressNode.path("ip").asText();
                            Map<String, String> metadata = new ConcurrentHashMap<>();

                            // Extract metadata from labels if available
                            JsonNode targetRefNode = addressNode.path("targetRef");
                            if (!targetRefNode.isMissingNode()) {
                                String podName = targetRefNode.path("name").asText();
                                // In a real implementation, you might want to fetch pod details
                                // to get labels as metadata
                                metadata.put("podName", podName);
                            }

                            for (JsonNode portNode : portsNode) {
                                int port = portNode.path("port").asInt();
                                String serviceUrl = String.format("http://%s:%d", ip, port);
                                discoveredEndpoints.add(new GeocoderEndpoint(serviceUrl, metadata));
                            }
                        }
                    }
                }
            }
        } finally {
            connection.disconnect();
        }

        return discoveredEndpoints;
    }

    /**
     * Reads the Kubernetes namespace from the service account.
     *
     * @return The Kubernetes namespace or null if not available
     */
    private String readKubernetesNamespace() {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(KUBERNETES_NAMESPACE_PATH);
            if (java.nio.file.Files.exists(path)) {
                return new String(java.nio.file.Files.readAllBytes(path)).trim();
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to read Kubernetes namespace", e);
        }
        return null;
    }

    /**
     * Reads the Kubernetes token from the service account.
     *
     * @return The Kubernetes token or null if not available
     */
    private String readKubernetesToken() {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(KUBERNETES_TOKEN_PATH);
            if (java.nio.file.Files.exists(path)) {
                return new String(java.nio.file.Files.readAllBytes(path)).trim();
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to read Kubernetes token", e);
        }
        return null;
    }

    /**
     * Updates the endpoints map with the newly discovered endpoints.
     *
     * @param discoveredEndpoints The list of newly discovered endpoints
     */
    private void updateEndpoints(List<GeocoderEndpoint> discoveredEndpoints) {
        // Create a map of discovered endpoints by URL
        Map<String, GeocoderEndpoint> discoveredMap = new ConcurrentHashMap<>();
        for (GeocoderEndpoint endpoint : discoveredEndpoints) {
            discoveredMap.put(endpoint.getUrl(), endpoint);
        }

        // Remove endpoints that are no longer available
        endpoints.keySet().removeIf(url -> !discoveredMap.containsKey(url));

        // Add or update endpoints
        for (GeocoderEndpoint endpoint : discoveredEndpoints) {
            GeocoderEndpoint existingEndpoint = endpoints.get(endpoint.getUrl());
            if (existingEndpoint == null) {
                // New endpoint
                endpoints.put(endpoint.getUrl(), endpoint);
            } else {
                // Update metadata but preserve health status
                existingEndpoint.getMetadata().clear();
                existingEndpoint.getMetadata().putAll(endpoint.getMetadata());
            }
        }
    }

    /**
     * Performs health checks on all endpoints.
     */
    private void checkEndpointsHealth() {
        for (GeocoderEndpoint endpoint : endpoints.values()) {
            executorService.submit(() -> {
                try {
                    URL url = new URL(endpoint.getUrl() + "/health");
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("GET");
                    connection.setConnectTimeout(5000);
                    connection.setReadTimeout(5000);

                    int responseCode = connection.getResponseCode();
                    endpoint.setHealthy(responseCode >= 200 && responseCode < 300);

                    connection.disconnect();
                } catch (Exception e) {
                    LOGGER.debug("Health check failed for endpoint {}: {}", endpoint.getUrl(), e.getMessage());
                    endpoint.setHealthy(false);
                }
            });
        }
    }

    /**
     * Selects a healthy geocoder endpoint based on metadata.
     *
     * @param preferredProvider The preferred geocoder provider, if any
     * @return The selected geocoder endpoint URL or null if none available
     */
    private String selectEndpoint(String preferredProvider) {
        // First try to find a healthy endpoint with the preferred provider
        if (preferredProvider != null && !preferredProvider.isEmpty()) {
            for (GeocoderEndpoint endpoint : endpoints.values()) {
                if (endpoint.isHealthy() && 
                        preferredProvider.equals(endpoint.getMetadata().get("provider"))) {
                    return endpoint.getUrl();
                }
            }
        }

        // If no preferred provider or none found, select any healthy endpoint
        for (GeocoderEndpoint endpoint : endpoints.values()) {
            if (endpoint.isHealthy()) {
                return endpoint.getUrl();
            }
        }

        return null;
    }

    @Override
    public String getAddress(double latitude, double longitude, ReverseGeocoderCallback callback) {
        String endpointUrl = selectEndpoint(null);
        if (endpointUrl == null) {
            LOGGER.warn("No healthy geocoder endpoints available");
            callback.onFailure(new RuntimeException("No healthy geocoder endpoints available"));
            return null;
        }

        try {
            URL url = new URL(endpointUrl + "/geocode?lat=" + latitude + "&lon=" + longitude);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");

            if (connection.getResponseCode() != 200) {
                String errorMessage = "Geocoder service returned HTTP error code: " + connection.getResponseCode();
                LOGGER.warn(errorMessage);
                callback.onFailure(new IOException(errorMessage));
                return null;
            }

            JsonNode responseNode = objectMapper.readTree(connection.getInputStream());
            String address = responseNode.path("address").asText();

            if (address != null && !address.isEmpty()) {
                callback.onSuccess(address);
                if (statisticsManager != null) {
                    statisticsManager.registerGeocoderSuccess();
                }
            } else {
                callback.onFailure(new RuntimeException("Empty address returned"));
                if (statisticsManager != null) {
                    statisticsManager.registerGeocoderFailure();
                }
            }

            return address;
        } catch (Exception e) {
            LOGGER.warn("Geocoder service request failed", e);
            callback.onFailure(e);
            if (statisticsManager != null) {
                statisticsManager.registerGeocoderFailure();
            }
            return null;
        }
    }

    @Override
    public void setStatisticsManager(StatisticsManager statisticsManager) {
        this.statisticsManager = statisticsManager;
    }

    /**
     * Shuts down the service discovery executor service.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}