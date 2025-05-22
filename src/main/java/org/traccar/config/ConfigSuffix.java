/*
 * Copyright 2019 - 2024 Anton Tananaev (anton@traccar.org)
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

import java.util.List;

/**
 * Base class for configuration key suffixes that can be combined with prefixes to create complete configuration keys.
 * Supports microservices configuration patterns including service-specific, environment-specific, and grouped configurations.
 *
 * @param <T> The type of the configuration value (String, Boolean, Integer, etc.)
 */
public abstract class ConfigSuffix<T> {

    protected final String keySuffix;
    protected final List<KeyType> types;
    protected final T defaultValue;
    
    private static final String SERVICE_SEPARATOR = ".";
    private static final String GROUP_SEPARATOR = ".";
    private static final String ENV_SEPARATOR = ".";

    ConfigSuffix(String keySuffix, List<KeyType> types, T defaultValue) {
        this.keySuffix = keySuffix;
        this.types = types;
        this.defaultValue = defaultValue;
    }

    /**
     * Creates a ConfigKey with the given prefix.
     *
     * @param prefix The prefix to prepend to the key suffix
     * @return A new ConfigKey with the combined key name
     */
    public abstract ConfigKey<T> withPrefix(String prefix);
    
    /**
     * Creates a ConfigKey specific to a microservice.
     *
     * @param serviceName The name of the microservice (e.g., "protocol-service", "position-service")
     * @return A new ConfigKey with the service-specific prefix
     */
    public ConfigKey<T> forService(String serviceName) {
        return withPrefix(serviceName + SERVICE_SEPARATOR);
    }
    
    /**
     * Creates a ConfigKey for a group of related services.
     *
     * @param groupName The name of the service group (e.g., "messaging", "tracing")
     * @return A new ConfigKey with the group-specific prefix
     */
    public ConfigKey<T> forGroup(String groupName) {
        return withPrefix(groupName + GROUP_SEPARATOR);
    }
    
    /**
     * Creates a ConfigKey specific to an environment.
     *
     * @param environment The environment name (e.g., "dev", "staging", "prod")
     * @return A new ConfigKey with the environment-specific prefix
     */
    public ConfigKey<T> forEnvironment(String environment) {
        return withPrefix(environment + ENV_SEPARATOR);
    }
    
    /**
     * Creates a ConfigKey specific to a service in a particular environment.
     *
     * @param serviceName The name of the microservice
     * @param environment The environment name
     * @return A new ConfigKey with combined service and environment prefix
     */
    public ConfigKey<T> forServiceInEnvironment(String serviceName, String environment) {
        return withPrefix(environment + ENV_SEPARATOR + serviceName + SERVICE_SEPARATOR);
    }
    
    /**
     * Generates an environment variable name suitable for containerized environments.
     * Converts dot notation to uppercase with underscores (e.g., "service.key" becomes "SERVICE_KEY").
     *
     * @param prefix The prefix to prepend to the key suffix
     * @return The environment variable name
     */
    public String toEnvironmentVariable(String prefix) {
        String fullKey = prefix + keySuffix;
        return fullKey.replaceAll("\\.+", "_").toUpperCase();
    }
    
    /**
     * Generates a service-specific environment variable name for containerized environments.
     *
     * @param serviceName The name of the microservice
     * @return The service-specific environment variable name
     */
    public String toServiceEnvironmentVariable(String serviceName) {
        return toEnvironmentVariable(serviceName + SERVICE_SEPARATOR);
    }
}

class StringConfigSuffix extends ConfigSuffix<String> {
    StringConfigSuffix(String key, List<KeyType> types) {
        super(key, types, null);
    }
    StringConfigSuffix(String key, List<KeyType> types, String defaultValue) {
        super(key, types, defaultValue);
    }
    @Override
    public ConfigKey<String> withPrefix(String prefix) {
        return new StringConfigKey(prefix + keySuffix, types, defaultValue);
    }
}

class BooleanConfigSuffix extends ConfigSuffix<Boolean> {
    BooleanConfigSuffix(String key, List<KeyType> types) {
        super(key, types, null);
    }
    BooleanConfigSuffix(String key, List<KeyType> types, Boolean defaultValue) {
        super(key, types, defaultValue);
    }
    @Override
    public ConfigKey<Boolean> withPrefix(String prefix) {
        return new BooleanConfigKey(prefix + keySuffix, types, defaultValue);
    }
}

class IntegerConfigSuffix extends ConfigSuffix<Integer> {
    IntegerConfigSuffix(String key, List<KeyType> types) {
        super(key, types, null);
    }
    IntegerConfigSuffix(String key, List<KeyType> types, Integer defaultValue) {
        super(key, types, defaultValue);
    }
    @Override
    public ConfigKey<Integer> withPrefix(String prefix) {
        return new IntegerConfigKey(prefix + keySuffix, types, defaultValue);
    }
}

class LongConfigSuffix extends ConfigSuffix<Long> {
    LongConfigSuffix(String key, List<KeyType> types) {
        super(key, types, null);
    }
    LongConfigSuffix(String key, List<KeyType> types, Long defaultValue) {
        super(key, types, defaultValue);
    }
    @Override
    public ConfigKey<Long> withPrefix(String prefix) {
        return new LongConfigKey(prefix + keySuffix, types, defaultValue);
    }
}

class DoubleConfigSuffix extends ConfigSuffix<Double> {
    DoubleConfigSuffix(String key, List<KeyType> types) {
        super(key, types, null);
    }
    DoubleConfigSuffix(String key, List<KeyType> types, Double defaultValue) {
        super(key, types, defaultValue);
    }
    @Override
    public ConfigKey<Double> withPrefix(String prefix) {
        return new DoubleConfigKey(prefix + keySuffix, types, defaultValue);
    }
}