/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ConfigMap;
import io.kubernetes.client.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.ConfigKey;
import org.traccar.helper.Log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Configuration class for the API Gateway service that loads and manages service settings
 * from environment variables, configuration files, and Kubernetes ConfigMaps.
 * It provides typed access to configuration properties, supports dynamic reloading
 * of certain settings, and exposes configuration endpoints for management.
 */
@Singleton
public class ApiGatewayConfiguration extends org.traccar.config.Config {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewayConfiguration.class);

    private static final String CONFIG_MAP_NAME = "api-gateway-config";
    private static final String CONFIG_NAMESPACE = "default";
    private static final long CONFIG_RELOAD_INTERVAL_SECONDS = 60;

    private final Properties dynamicProperties = new Properties();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private CoreV1Api kubernetesApi;
    private boolean kubernetesEnabled;

    /**
     * Creates a new ApiGatewayConfiguration instance with the specified configuration file.
     * Initializes Kubernetes client if running in a Kubernetes environment.
     *
     * @param file Path to the configuration file
     * @throws IOException If there is an error reading the configuration file
     */
    @Inject
    public ApiGatewayConfiguration(@Named("configFile") String file) throws IOException {
        super(file);
        initializeKubernetes();
        loadDynamicProperties();
        scheduleConfigReload();
    }

    /**
     * Initializes the Kubernetes client if running in a Kubernetes environment.
     * Detects Kubernetes environment by checking for service account token.
     */
    private void initializeKubernetes() {
        try {
            Path serviceAccountPath = Paths.get("/var/run/secrets/kubernetes.io/serviceaccount/token");
            if (Files.exists(serviceAccountPath)) {
                LOGGER.info("Kubernetes service account detected, initializing Kubernetes client");
                ApiClient client = Config.defaultClient();
                Configuration.setDefaultApiClient(client);
                kubernetesApi = new CoreV1Api();
                kubernetesEnabled = true;
            } else {
                LOGGER.info("Not running in Kubernetes environment, ConfigMap integration disabled");
                kubernetesEnabled = false;
            }
        } catch (IOException e) {
            LOGGER.warn("Failed to initialize Kubernetes client", e);
            kubernetesEnabled = false;
        }
    }

    /**
     * Loads dynamic properties from Kubernetes ConfigMap if available.
     */
    private void loadDynamicProperties() {
        if (kubernetesEnabled) {
            try {
                V1ConfigMap configMap = kubernetesApi.readNamespacedConfigMap(
                        CONFIG_MAP_NAME, CONFIG_NAMESPACE, null);
                if (configMap != null && configMap.getData() != null) {
                    Map<String, String> data = configMap.getData();
                    for (Map.Entry<String, String> entry : data.entrySet()) {
                        dynamicProperties.setProperty(entry.getKey(), entry.getValue());
                    }
                    LOGGER.info("Loaded {} properties from Kubernetes ConfigMap", data.size());
                }
            } catch (ApiException e) {
                if (e.getCode() == 404) {
                    LOGGER.info("ConfigMap {} not found in namespace {}", CONFIG_MAP_NAME, CONFIG_NAMESPACE);
                } else {
                    LOGGER.warn("Failed to load ConfigMap: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * Schedules periodic reloading of dynamic configuration properties.
     */
    private void scheduleConfigReload() {
        scheduler.scheduleAtFixedRate(this::loadDynamicProperties,
                CONFIG_RELOAD_INTERVAL_SECONDS, CONFIG_RELOAD_INTERVAL_SECONDS, TimeUnit.SECONDS);
        LOGGER.info("Scheduled configuration reload every {} seconds", CONFIG_RELOAD_INTERVAL_SECONDS);
    }

    /**
     * Gets a dynamic property value, checking both the ConfigMap and environment variables.
     *
     * @param key Property key
     * @return Property value or null if not found
     */
    public String getDynamicProperty(String key) {
        // First check dynamic properties from ConfigMap
        String value = dynamicProperties.getProperty(key);
        if (value != null) {
            return value;
        }

        // Then check environment variables
        String envKey = getEnvironmentVariableName(key);
        return System.getenv(envKey);
    }

    /**
     * Gets a typed configuration value, checking dynamic properties first.
     *
     * @param key Configuration key
     * @param <T> Type of the configuration value
     * @return Configuration value or default value if not found
     */
    @Override
    public <T> T get(ConfigKey<T> key) {
        String dynamicValue = getDynamicProperty(key.getKey());
        if (dynamicValue != null) {
            return key.parseValue(dynamicValue);
        }
        return super.get(key);
    }

    /**
     * Gets all configuration properties, including dynamic ones.
     *
     * @return Map of all configuration properties
     */
    public Map<String, String> getAllProperties() {
        Map<String, String> allProperties = new HashMap<>();
        
        // Add properties from the base configuration
        for (Object key : super.getProperties().keySet()) {
            String keyStr = (String) key;
            allProperties.put(keyStr, super.getString(keyStr));
        }
        
        // Add dynamic properties
        for (Object key : dynamicProperties.keySet()) {
            String keyStr = (String) key;
            allProperties.put(keyStr, dynamicProperties.getProperty(keyStr));
        }
        
        // Add relevant environment variables
        System.getenv().forEach((key, value) -> {
            if (key.startsWith("TRACCAR_") || key.startsWith("API_GATEWAY_")) {
                String configKey = key.replace("TRACCAR_", "").replace("API_GATEWAY_", "");
                configKey = configKey.toLowerCase().replace("_", ".");
                allProperties.put(configKey, value);
            }
        });
        
        return allProperties;
    }

    /**
     * Updates a dynamic configuration property.
     *
     * @param key Property key
     * @param value Property value
     */
    public void updateDynamicProperty(String key, String value) {
        dynamicProperties.setProperty(key, value);
        LOGGER.info("Updated dynamic property: {}={}", key, value);
    }

    /**
     * Checks if a property is dynamically reloadable.
     *
     * @param key Property key
     * @return true if the property is dynamically reloadable
     */
    public boolean isDynamicProperty(String key) {
        // List of properties that can be dynamically reloaded
        return key.startsWith("web.") || 
               key.startsWith("api.") || 
               key.startsWith("notification.") || 
               key.equals("logger.level");
    }

    /**
     * Shuts down the configuration service, stopping the reload scheduler.
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            scheduler.shutdownNow();
        }
    }

    /**
     * Gets the properties object from the base configuration.
     * This is used internally for accessing all properties.
     *
     * @return Properties object from the base configuration
     */
    private Properties getProperties() {
        return super.getProperties();
    }
}