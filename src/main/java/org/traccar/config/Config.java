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
import java.util.InvalidPropertiesFormatException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

/**
 * Configuration manager that handles loading and accessing configuration values.
 */
@Singleton
public class Config {

    private final Properties properties = new Properties();

    private boolean useEnvironmentVariables;
    private String serviceScope;

    /**
     * Default constructor.
     */
    public Config() {
    }

    /**
     * Creates a new configuration instance and loads configuration from a file.
     *
     * @param file Configuration file path
     * @throws IOException If the file cannot be read
     */
    @Inject
    public Config(@Named("configFile") String file) throws IOException {
        try {
            try (InputStream inputStream = new FileInputStream(file)) {
                properties.loadFromXML(inputStream);
            }

            useEnvironmentVariables = Boolean.parseBoolean(System.getenv("CONFIG_USE_ENVIRONMENT_VARIABLES"))
                    || Boolean.parseBoolean(properties.getProperty("config.useEnvironmentVariables"));

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
     * Sets the service scope for this configuration instance.
     * This affects how service-specific configuration keys are resolved.
     *
     * @param serviceScope The service scope identifier
     * @return This Config instance for method chaining
     */
    public Config withServiceScope(String serviceScope) {
        this.serviceScope = serviceScope;
        return this;
    }

    /**
     * Gets the current service scope.
     *
     * @return The current service scope or null if not set
     */
    public String getServiceScope() {
        return serviceScope;
    }

    /**
     * Checks if a configuration key exists.
     *
     * @param key The configuration key
     * @return True if the key exists in the configuration
     */
    public boolean hasKey(ConfigKey<?> key) {
        return hasKey(key.getKey());
    }

    /**
     * Checks if a configuration key exists.
     *
     * @param key The configuration key name
     * @return True if the key exists in the configuration
     */
    public boolean hasKey(String key) {
        return useEnvironmentVariables && System.getenv().containsKey(getEnvironmentVariableName(key))
                || properties.containsKey(key);
    }

    /**
     * Gets a string configuration value.
     *
     * @param key The configuration key
     * @return The configuration value or default value if not set
     */
    public String getString(ConfigKey<String> key) {
        return getString(key.getKey(), key.getDefaultValue());
    }

    /**
     * Gets a string configuration value.
     *
     * @param key The configuration key name
     * @return The configuration value or null if not set
     * @deprecated Use getString(ConfigKey) instead
     */
    @Deprecated
    public String getString(String key) {
        if (useEnvironmentVariables) {
            String value = System.getenv(getEnvironmentVariableName(key));
            if (value != null && !value.isEmpty()) {
                return value;
            }
        }
        return properties.getProperty(key);
    }

    /**
     * Gets a string configuration value with a default.
     *
     * @param key          The configuration key
     * @param defaultValue The default value if not set
     * @return The configuration value or default value if not set
     */
    public String getString(ConfigKey<String> key, String defaultValue) {
        return getString(key.getKey(), defaultValue);
    }

    /**
     * Gets a string configuration value with a default.
     *
     * @param key          The configuration key name
     * @param defaultValue The default value if not set
     * @return The configuration value or default value if not set
     * @deprecated Use getString(ConfigKey, String) instead
     */
    @Deprecated
    public String getString(String key, String defaultValue) {
        return hasKey(key) ? getString(key) : defaultValue;
    }

    /**
     * Gets a boolean configuration value.
     *
     * @param key The configuration key
     * @return The configuration value or default value if not set
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
     * Gets an integer configuration value.
     *
     * @param key The configuration key
     * @return The configuration value or default value if not set
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
     * Gets an integer configuration value with a default.
     *
     * @param key          The configuration key
     * @param defaultValue The default value if not set
     * @return The configuration value or default value if not set
     */
    public int getInteger(ConfigKey<Integer> key, int defaultValue) {
        return getInteger(key.getKey(), defaultValue);
    }

    /**
     * Gets an integer configuration value with a default.
     *
     * @param key          The configuration key name
     * @param defaultValue The default value if not set
     * @return The configuration value or default value if not set
     * @deprecated Use getInteger(ConfigKey, int) instead
     */
    @Deprecated
    public int getInteger(String key, int defaultValue) {
        return hasKey(key) ? Integer.parseInt(getString(key)) : defaultValue;
    }

    /**
     * Gets a long configuration value.
     *
     * @param key The configuration key
     * @return The configuration value or default value if not set
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
     * Gets a double configuration value.
     *
     * @param key The configuration key
     * @return The configuration value or default value if not set
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
     * Gets a URI configuration value.
     *
     * @param key The configuration key
     * @return The configuration value or default value if not set
     */
    public String getUri(ConfigKey<String> key) {
        return getString(key);
    }

    /**
     * Gets a JSON configuration value.
     *
     * @param key The configuration key
     * @return The configuration value or default value if not set
     */
    public String getJson(ConfigKey<String> key) {
        return getString(key);
    }

    /**
     * Gets a typed configuration value.
     *
     * @param key The configuration key
     * @param <T> The type of the configuration value
     * @return The configuration value or default value if not set
     */
    @SuppressWarnings("unchecked")
    public <T> T getValue(ConfigKey<T> key) {
        Class<T> valueClass = key.getValueClass();
        if (valueClass.equals(String.class)) {
            return (T) getString(key);
        } else if (valueClass.equals(Boolean.class)) {
            return (T) Boolean.valueOf(getBoolean(key));
        } else if (valueClass.equals(Integer.class)) {
            return (T) Integer.valueOf(getInteger(key));
        } else if (valueClass.equals(Long.class)) {
            return (T) Long.valueOf(getLong(key));
        } else if (valueClass.equals(Double.class)) {
            return (T) Double.valueOf(getDouble(key));
        }
        return null;
    }

    /**
     * Sets a string configuration value (for testing).
     *
     * @param key   The configuration key
     * @param value The configuration value
     */
    @VisibleForTesting
    public void setString(ConfigKey<?> key, String value) {
        properties.put(key.getKey(), value);
    }

    /**
     * Validates all configuration values against their validation rules.
     *
     * @param keys The configuration keys to validate
     * @return A list of validation errors, empty if all validations pass
     */
    public List<ConfigValidator.ValidationError> validate(List<ConfigKey<?>> keys) {
        return ConfigValidator.validateAll(this, keys);
    }

    /**
     * Converts a configuration key to an environment variable name.
     *
     * @param key The configuration key name
     * @return The environment variable name
     */
    static String getEnvironmentVariableName(String key) {
        return key.replaceAll("\\.\\.", "_").replaceAll("(\\p{Lu})", "_$1").toUpperCase();
    }

}