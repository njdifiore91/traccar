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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Configuration key definition with type safety and metadata.
 * @param <T> The type of the configuration value
 */
public abstract class ConfigKey<T> {

    private final String key;
    private final Set<KeyType> types = new HashSet<>();
    private final Class<T> valueClass;
    private final T defaultValue;
    private final int version;
    private final String serviceScope;
    private final Predicate<T> validationRule;
    private final String validationMessage;

    /**
     * Creates a configuration key with validation.
     *
     * @param key               The configuration key name
     * @param types             List of key types where this configuration applies
     * @param valueClass        The class of the configuration value
     * @param defaultValue      Default value if not specified
     * @param version           Version when this configuration key was introduced
     * @param serviceScope      Service scope this configuration belongs to (null for global)
     * @param validationRule    Predicate to validate the configuration value
     * @param validationMessage Message to display when validation fails
     */
    ConfigKey(String key, List<KeyType> types, Class<T> valueClass, T defaultValue, 
              int version, String serviceScope, Predicate<T> validationRule, String validationMessage) {
        this.key = key;
        this.types.addAll(types);
        this.valueClass = valueClass;
        this.defaultValue = defaultValue;
        this.version = version;
        this.serviceScope = serviceScope;
        this.validationRule = validationRule;
        this.validationMessage = validationMessage;
    }

    /**
     * Creates a configuration key without validation.
     *
     * @param key          The configuration key name
     * @param types        List of key types where this configuration applies
     * @param valueClass   The class of the configuration value
     * @param defaultValue Default value if not specified
     * @param version      Version when this configuration key was introduced
     * @param serviceScope Service scope this configuration belongs to (null for global)
     */
    ConfigKey(String key, List<KeyType> types, Class<T> valueClass, T defaultValue, 
              int version, String serviceScope) {
        this(key, types, valueClass, defaultValue, version, serviceScope, null, null);
    }

    /**
     * Legacy constructor for backward compatibility.
     *
     * @param key          The configuration key name
     * @param types        List of key types where this configuration applies
     * @param valueClass   The class of the configuration value
     * @param defaultValue Default value if not specified
     */
    ConfigKey(String key, List<KeyType> types, Class<T> valueClass, T defaultValue) {
        this(key, types, valueClass, defaultValue, 1, null, null, null);
    }

    /**
     * @return The configuration key name
     */
    public String getKey() {
        return key;
    }

    /**
     * Check if the key has a specific type.
     *
     * @param type Key type to check
     * @return True if the key has the specified type
     */
    public boolean hasType(KeyType type) {
        return types.contains(type);
    }

    /**
     * @return The class of the configuration value
     */
    public Class<T> getValueClass() {
        return valueClass;
    }

    /**
     * @return Default value if not specified
     */
    public T getDefaultValue() {
        return defaultValue;
    }
    
    /**
     * @return Version when this configuration key was introduced
     */
    public int getVersion() {
        return version;
    }
    
    /**
     * @return Service scope this configuration belongs to (null for global)
     */
    public String getServiceScope() {
        return serviceScope;
    }
    
    /**
     * Validates the provided value against the validation rule.
     *
     * @param value Value to validate
     * @return True if the value is valid or no validation rule exists
     */
    public boolean validate(T value) {
        return validationRule == null || validationRule.test(value);
    }
    
    /**
     * @return Message to display when validation fails
     */
    public String getValidationMessage() {
        return validationMessage;
    }

    /**
     * @return True if this configuration key has a validation rule
     */
    public boolean hasValidation() {
        return validationRule != null;
    }
}

/**
 * String configuration key implementation.
 */
class StringConfigKey extends ConfigKey<String> {
    StringConfigKey(String key, List<KeyType> types) {
        super(key, types, String.class, null);
    }
    
    StringConfigKey(String key, List<KeyType> types, String defaultValue) {
        super(key, types, String.class, defaultValue);
    }
    
    StringConfigKey(String key, List<KeyType> types, String defaultValue, int version, String serviceScope) {
        super(key, types, String.class, defaultValue, version, serviceScope);
    }
    
    StringConfigKey(String key, List<KeyType> types, String defaultValue, int version, String serviceScope,
                   Predicate<String> validationRule, String validationMessage) {
        super(key, types, String.class, defaultValue, version, serviceScope, validationRule, validationMessage);
    }
}

/**
 * Boolean configuration key implementation.
 */
class BooleanConfigKey extends ConfigKey<Boolean> {
    BooleanConfigKey(String key, List<KeyType> types) {
        super(key, types, Boolean.class, null);
    }
    
    BooleanConfigKey(String key, List<KeyType> types, Boolean defaultValue) {
        super(key, types, Boolean.class, defaultValue);
    }
    
    BooleanConfigKey(String key, List<KeyType> types, Boolean defaultValue, int version, String serviceScope) {
        super(key, types, Boolean.class, defaultValue, version, serviceScope);
    }
    
    BooleanConfigKey(String key, List<KeyType> types, Boolean defaultValue, int version, String serviceScope,
                    Predicate<Boolean> validationRule, String validationMessage) {
        super(key, types, Boolean.class, defaultValue, version, serviceScope, validationRule, validationMessage);
    }
}

/**
 * Integer configuration key implementation.
 */
class IntegerConfigKey extends ConfigKey<Integer> {
    IntegerConfigKey(String key, List<KeyType> types) {
        super(key, types, Integer.class, null);
    }
    
    IntegerConfigKey(String key, List<KeyType> types, Integer defaultValue) {
        super(key, types, Integer.class, defaultValue);
    }
    
    IntegerConfigKey(String key, List<KeyType> types, Integer defaultValue, int version, String serviceScope) {
        super(key, types, Integer.class, defaultValue, version, serviceScope);
    }
    
    IntegerConfigKey(String key, List<KeyType> types, Integer defaultValue, int version, String serviceScope,
                    Predicate<Integer> validationRule, String validationMessage) {
        super(key, types, Integer.class, defaultValue, version, serviceScope, validationRule, validationMessage);
    }
}

/**
 * Long configuration key implementation.
 */
class LongConfigKey extends ConfigKey<Long> {
    LongConfigKey(String key, List<KeyType> types) {
        super(key, types, Long.class, null);
    }
    
    LongConfigKey(String key, List<KeyType> types, Long defaultValue) {
        super(key, types, Long.class, defaultValue);
    }
    
    LongConfigKey(String key, List<KeyType> types, Long defaultValue, int version, String serviceScope) {
        super(key, types, Long.class, defaultValue, version, serviceScope);
    }
    
    LongConfigKey(String key, List<KeyType> types, Long defaultValue, int version, String serviceScope,
                 Predicate<Long> validationRule, String validationMessage) {
        super(key, types, Long.class, defaultValue, version, serviceScope, validationRule, validationMessage);
    }
}

/**
 * Double configuration key implementation.
 */
class DoubleConfigKey extends ConfigKey<Double> {
    DoubleConfigKey(String key, List<KeyType> types) {
        super(key, types, Double.class, null);
    }
    
    DoubleConfigKey(String key, List<KeyType> types, Double defaultValue) {
        super(key, types, Double.class, defaultValue);
    }
    
    DoubleConfigKey(String key, List<KeyType> types, Double defaultValue, int version, String serviceScope) {
        super(key, types, Double.class, defaultValue, version, serviceScope);
    }
    
    DoubleConfigKey(String key, List<KeyType> types, Double defaultValue, int version, String serviceScope,
                   Predicate<Double> validationRule, String validationMessage) {
        super(key, types, Double.class, defaultValue, version, serviceScope, validationRule, validationMessage);
    }
}

/**
 * URI/URL configuration key implementation.
 */
class UriConfigKey extends ConfigKey<String> {
    UriConfigKey(String key, List<KeyType> types) {
        super(key, types, String.class, null);
    }
    
    UriConfigKey(String key, List<KeyType> types, String defaultValue) {
        super(key, types, String.class, defaultValue);
    }
    
    UriConfigKey(String key, List<KeyType> types, String defaultValue, int version, String serviceScope) {
        super(key, types, String.class, defaultValue, version, serviceScope);
    }
    
    UriConfigKey(String key, List<KeyType> types, String defaultValue, int version, String serviceScope,
                Predicate<String> validationRule, String validationMessage) {
        super(key, types, String.class, defaultValue, version, serviceScope, validationRule, validationMessage);
    }
}

/**
 * JSON configuration key implementation.
 */
class JsonConfigKey extends ConfigKey<String> {
    JsonConfigKey(String key, List<KeyType> types) {
        super(key, types, String.class, null);
    }
    
    JsonConfigKey(String key, List<KeyType> types, String defaultValue) {
        super(key, types, String.class, defaultValue);
    }
    
    JsonConfigKey(String key, List<KeyType> types, String defaultValue, int version, String serviceScope) {
        super(key, types, String.class, defaultValue, version, serviceScope);
    }
    
    JsonConfigKey(String key, List<KeyType> types, String defaultValue, int version, String serviceScope,
                 Predicate<String> validationRule, String validationMessage) {
        super(key, types, String.class, defaultValue, version, serviceScope, validationRule, validationMessage);
    }
}