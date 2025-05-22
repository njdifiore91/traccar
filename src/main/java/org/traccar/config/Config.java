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
package org.traccar.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.name.Named;
import org.traccar.helper.Log;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Configuration manager that supports loading from multiple sources including:
 * - XML configuration files
 * - Environment variables
 * - Kubernetes ConfigMaps and Secrets
 * - Service-specific configuration
 * 
 * The configuration system supports hierarchical configuration with service-specific
 * overrides and dynamic configuration updates.
 */
@Singleton
public class Config {

    private final Properties properties = new Properties();
    private final Map<String, Properties> serviceProperties = new ConcurrentHashMap<>();
    private final Map<String, Consumer<String>> configChangeListeners = new ConcurrentHashMap<>();
    private final ScheduledExecutorService configRefreshExecutor;
    
    private boolean useEnvironmentVariables;
    private boolean useKubernetesConfig;
    private String serviceName;
    private String configMapPath;
    private String secretsPath;
    private long refreshIntervalSeconds;

    /**
     * Default constructor for dependency injection.
     */
    public Config() {
        this.configRefreshExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "config-refresh");
            thread.setDaemon(true);
            return thread;
        });
    }

    /**
     * Constructor with configuration file path.
     * 
     * @param file Path to the configuration file
     * @throws IOException If there is an error reading the configuration file
     */
    @Inject
    public Config(@Named("configFile") String file) throws IOException {
        this();
        try {
            // Load base configuration from XML file
            try (InputStream inputStream = new FileInputStream(file)) {
                properties.loadFromXML(inputStream);
            }

            // Check if environment variables should be used
            useEnvironmentVariables = Boolean.parseBoolean(System.getenv("CONFIG_USE_ENVIRONMENT_VARIABLES"))
                    || Boolean.parseBoolean(properties.getProperty("config.useEnvironmentVariables"));
            
            // Check if Kubernetes ConfigMaps and Secrets should be used
            useKubernetesConfig = Boolean.parseBoolean(System.getenv("CONFIG_USE_KUBERNETES"))
                    || Boolean.parseBoolean(properties.getProperty("config.useKubernetes"));
            
            // Get service name for service-specific configuration
            serviceName = System.getenv("SERVICE_NAME");
            if (serviceName == null) {
                serviceName = properties.getProperty("config.serviceName", "traccar");
            }
            
            // Get ConfigMap and Secrets paths
            configMapPath = System.getenv("CONFIG_MAP_PATH");
            if (configMapPath == null) {
                configMapPath = properties.getProperty("config.kubernetes.configMapPath", "/etc/config");
            }
            
            secretsPath = System.getenv("SECRETS_PATH");
            if (secretsPath == null) {
                secretsPath = properties.getProperty("config.kubernetes.secretsPath", "/etc/secrets");
            }
            
            // Get refresh interval for dynamic configuration updates
            String refreshInterval = System.getenv("CONFIG_REFRESH_INTERVAL");
            if (refreshInterval == null) {
                refreshInterval = properties.getProperty("config.refreshInterval", "60");
            }
            refreshIntervalSeconds = Long.parseLong(refreshInterval);
            
            // Load additional configuration sources
            loadAdditionalSources();
            
            // Setup configuration refresh if enabled
            if (refreshIntervalSeconds > 0) {
                configRefreshExecutor.scheduleAtFixedRate(
                    this::refreshConfiguration, 
                    refreshIntervalSeconds, 
                    refreshIntervalSeconds, 
                    TimeUnit.SECONDS);
            }

            Log.setupLogger(this);
        } catch (InvalidPropertiesFormatException e) {
            Log.setupDefaultLogger();
            throw new RuntimeException("Configuration file is not a valid XML document", e);
        } catch (Exception e) {
            Log.setupDefaultLogger();
            throw e;
        }
    }
    
    /**
     * Load configuration from additional sources like Kubernetes ConfigMaps and Secrets.
     */
    private void loadAdditionalSources() {
        // Load from Kubernetes ConfigMaps if enabled
        if (useKubernetesConfig) {
            loadFromKubernetesConfigMap();
            loadFromKubernetesSecrets();
        }
        
        // Load service-specific configuration
        loadServiceSpecificConfig();
    }
    
    /**
     * Load configuration from Kubernetes ConfigMap mounted as files.
     */
    private void loadFromKubernetesConfigMap() {
        Path configPath = Paths.get(configMapPath);
        if (Files.exists(configPath) && Files.isDirectory(configPath)) {
            try {
                Files.list(configPath).forEach(file -> {
                    if (Files.isRegularFile(file)) {
                        try {
                            String key = file.getFileName().toString();
                            String value = new String(Files.readAllBytes(file)).trim();
                            properties.setProperty(key, value);
                        } catch (IOException e) {
                            Log.warning("Failed to read ConfigMap file: " + file, e);
                        }
                    }
                });
            } catch (IOException e) {
                Log.warning("Failed to read ConfigMap directory: " + configPath, e);
            }
        }
    }
    
    /**
     * Load configuration from Kubernetes Secrets mounted as files.
     */
    private void loadFromKubernetesSecrets() {
        Path secretsDir = Paths.get(secretsPath);
        if (Files.exists(secretsDir) && Files.isDirectory(secretsDir)) {
            try {
                Files.list(secretsDir).forEach(file -> {
                    if (Files.isRegularFile(file)) {
                        try {
                            String key = file.getFileName().toString();
                            String value = new String(Files.readAllBytes(file)).trim();
                            properties.setProperty(key, value);
                        } catch (IOException e) {
                            Log.warning("Failed to read Secret file: " + file, e);
                        }
                    }
                });
            } catch (IOException e) {
                Log.warning("Failed to read Secrets directory: " + secretsDir, e);
            }
        }
    }
    
    /**
     * Load service-specific configuration.
     */
    private void loadServiceSpecificConfig() {
        if (serviceName != null && !serviceName.isEmpty()) {
            // Load service-specific configuration from environment variables
            if (useEnvironmentVariables) {
                Properties serviceEnvProps = new Properties();
                System.getenv().forEach((key, value) -> {
                    if (key.startsWith(serviceName.toUpperCase() + "_")) {
                        String configKey = key.substring(serviceName.length() + 1).toLowerCase();
                        configKey = configKey.replace('_', '.');
                        serviceEnvProps.setProperty(configKey, value);
                    }
                });
                serviceProperties.put(serviceName, serviceEnvProps);
            }
            
            // Load service-specific configuration from Kubernetes ConfigMap
            if (useKubernetesConfig) {
                Path serviceConfigPath = Paths.get(configMapPath, serviceName);
                if (Files.exists(serviceConfigPath) && Files.isDirectory(serviceConfigPath)) {
                    Properties serviceConfigProps = serviceProperties.computeIfAbsent(serviceName, k -> new Properties());
                    try {
                        Files.list(serviceConfigPath).forEach(file -> {
                            if (Files.isRegularFile(file)) {
                                try {
                                    String key = file.getFileName().toString();
                                    String value = new String(Files.readAllBytes(file)).trim();
                                    serviceConfigProps.setProperty(key, value);
                                } catch (IOException e) {
                                    Log.warning("Failed to read service ConfigMap file: " + file, e);
                                }
                            }
                        });
                    } catch (IOException e) {
                        Log.warning("Failed to read service ConfigMap directory: " + serviceConfigPath, e);
                    }
                }
            }
        }
    }
    
    /**
     * Refresh configuration from all sources.
     */
    private synchronized void refreshConfiguration() {
        try {
            // Track changed keys to notify listeners
            Map<String, String> oldValues = new HashMap<>();
            for (String key : configChangeListeners.keySet()) {
                oldValues.put(key, getString(key));
            }
            
            // Reload configuration from all sources
            loadAdditionalSources();
            
            // Notify listeners of changes
            for (Map.Entry<String, String> entry : oldValues.entrySet()) {
                String key = entry.getKey();
                String oldValue = entry.getValue();
                String newValue = getString(key);
                if (!Objects.equals(oldValue, newValue)) {
                    Consumer<String> listener = configChangeListeners.get(key);
                    if (listener != null) {
                        listener.accept(newValue);
                    }
                }
            }
        } catch (Exception e) {
            Log.warning("Failed to refresh configuration", e);
        }
    }
    
    /**
     * Register a listener for configuration changes.
     * 
     * @param key The configuration key to watch
     * @param listener The listener to call when the value changes
     */
    public void addChangeListener(String key, Consumer<String> listener) {
        configChangeListeners.put(key, listener);
    }
    
    /**
     * Register a listener for configuration changes.
     * 
     * @param key The configuration key to watch
     * @param listener The listener to call when the value changes
     */
    public void addChangeListener(ConfigKey<?> key, Consumer<String> listener) {
        addChangeListener(key.getKey(), listener);
    }
    
    /**
     * Remove a configuration change listener.
     * 
     * @param key The configuration key
     */
    public void removeChangeListener(String key) {
        configChangeListeners.remove(key);
    }
    
    /**
     * Remove a configuration change listener.
     * 
     * @param key The configuration key
     */
    public void removeChangeListener(ConfigKey<?> key) {
        removeChangeListener(key.getKey());
    }

    /**
     * Check if a configuration key exists.
     * 
     * @param key The configuration key
     * @return True if the key exists
     */
    public boolean hasKey(ConfigKey<?> key) {
        return hasKey(key.getKey());
    }

    /**
     * Check if a configuration key exists.
     * 
     * @param key The configuration key
     * @return True if the key exists
     */
    private boolean hasKey(String key) {
        // Check service-specific configuration first
        if (serviceName != null && !serviceName.isEmpty()) {
            Properties serviceProps = serviceProperties.get(serviceName);
            if (serviceProps != null && serviceProps.containsKey(key)) {
                return true;
            }
        }
        
        // Check environment variables
        if (useEnvironmentVariables && System.getenv().containsKey(getEnvironmentVariableName(key))) {
            return true;
        }
        
        // Check base properties
        return properties.containsKey(key);
    }

    /**
     * Get a string configuration value.
     * 
     * @param key The configuration key
     * @return The configuration value or null if not found
     */
    public String getString(ConfigKey<String> key) {
        return getString(key.getKey(), key.getDefaultValue());
    }

    /**
     * Get a string configuration value.
     * 
     * @param key The configuration key
     * @return The configuration value or null if not found
     * @deprecated Use getString(ConfigKey) instead
     */
    @Deprecated
    public String getString(String key) {
        // Check service-specific configuration first
        if (serviceName != null && !serviceName.isEmpty()) {
            Properties serviceProps = serviceProperties.get(serviceName);
            if (serviceProps != null && serviceProps.containsKey(key)) {
                return serviceProps.getProperty(key);
            }
        }
        
        // Check environment variables
        if (useEnvironmentVariables) {
            String value = System.getenv(getEnvironmentVariableName(key));
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        
        // Check base properties
        return properties.getProperty(key);
    }

    /**
     * Get a string configuration value with a default.
     * 
     * @param key The configuration key
     * @param defaultValue The default value if not found
     * @return The configuration value or the default if not found
     */
    public String getString(ConfigKey<String> key, String defaultValue) {
        return getString(key.getKey(), defaultValue);
    }

    /**
     * Get a string configuration value with a default.
     * 
     * @param key The configuration key
     * @param defaultValue The default value if not found
     * @return The configuration value or the default if not found
     * @deprecated Use getString(ConfigKey, String) instead
     */
    @Deprecated
    public String getString(String key, String defaultValue) {
        return hasKey(key) ? getString(key) : defaultValue;
    }

    /**
     * Get a boolean configuration value.
     * 
     * @param key The configuration key
     * @return The configuration value or the default if not found
     */
    public boolean getBoolean(ConfigKey<Boolean> key) {
        String value = getString(key.getKey());
        if (value != null) {
            return Boolean.parseBoolean(value);
        } else {
            Boolean defaultValue = key.getDefaultValue();
            return Objects.requireNonNullElse(defaultValue, false);
        }
    }

    /**
     * Get an integer configuration value.
     * 
     * @param key The configuration key
     * @return The configuration value or the default if not found
     */
    public int getInteger(ConfigKey<Integer> key) {
        String value = getString(key.getKey());
        if (value != null) {
            return Integer.parseInt(value);
        } else {
            Integer defaultValue = key.getDefaultValue();
            return Objects.requireNonNullElse(defaultValue, 0);
        }
    }

    /**
     * Get an integer configuration value with a default.
     * 
     * @param key The configuration key
     * @param defaultValue The default value if not found
     * @return The configuration value or the default if not found
     */
    public int getInteger(ConfigKey<Integer> key, int defaultValue) {
        return getInteger(key.getKey(), defaultValue);
    }

    /**
     * Get an integer configuration value with a default.
     * 
     * @param key The configuration key
     * @param defaultValue The default value if not found
     * @return The configuration value or the default if not found
     * @deprecated Use getInteger(ConfigKey, int) instead
     */
    @Deprecated
    public int getInteger(String key, int defaultValue) {
        return hasKey(key) ? Integer.parseInt(getString(key)) : defaultValue;
    }

    /**
     * Get a long configuration value.
     * 
     * @param key The configuration key
     * @return The configuration value or the default if not found
     */
    public long getLong(ConfigKey<Long> key) {
        String value = getString(key.getKey());
        if (value != null) {
            return Long.parseLong(value);
        } else {
            Long defaultValue = key.getDefaultValue();
            return Objects.requireNonNullElse(defaultValue, 0L);
        }
    }

    /**
     * Get a double configuration value.
     * 
     * @param key The configuration key
     * @return The configuration value or the default if not found
     */
    public double getDouble(ConfigKey<Double> key) {
        String value = getString(key.getKey());
        if (value != null) {
            return Double.parseDouble(value);
        } else {
            Double defaultValue = key.getDefaultValue();
            return Objects.requireNonNullElse(defaultValue, 0.0);
        }
    }

    /**
     * Set a configuration value (for testing).
     * 
     * @param key The configuration key
     * @param value The configuration value
     */
    @VisibleForTesting
    public void setString(ConfigKey<?> key, String value) {
        properties.put(key.getKey(), value);
    }
    
    /**
     * Set a service-specific configuration value.
     * 
     * @param service The service name
     * @param key The configuration key
     * @param value The configuration value
     */
    @VisibleForTesting
    public void setServiceString(String service, String key, String value) {
        Properties serviceProps = serviceProperties.computeIfAbsent(service, k -> new Properties());
        serviceProps.setProperty(key, value);
    }

    /**
     * Convert a configuration key to an environment variable name.
     * 
     * @param key The configuration key
     * @return The environment variable name
     */
    static String getEnvironmentVariableName(String key) {
        return key.replaceAll("\\.", "_").replaceAll("(\\p{Lu})", "_$1").toUpperCase();
    }
    
    /**
     * Shutdown the configuration manager and release resources.
     */
    public void shutdown() {
        configRefreshExecutor.shutdownNow();
    }
}