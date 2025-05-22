/*
 * Copyright 2016 - 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base model class with extended attribute support for microservices architecture.
 * Provides serialization support for message brokers and tracking of modified fields.
 */
public class ExtendedModel extends BaseModel implements Serializable {

    private static final long serialVersionUID = 1L;

    @NotNull
    private Map<String, Object> attributes = new LinkedHashMap<>();

    @JsonIgnore
    private Set<String> modifiedAttributes = new HashSet<>();

    /**
     * Checks if the model has a specific attribute.
     *
     * @param key Attribute key
     * @return True if attribute exists
     */
    public boolean hasAttribute(String key) {
        return attributes.containsKey(key);
    }

    /**
     * Gets all attributes as a map.
     *
     * @return Map of all attributes
     */
    @JsonAnyGetter
    public Map<String, Object> getAttributes() {
        return attributes;
    }

    /**
     * Sets all attributes from a map.
     *
     * @param attributes Map of attributes to set
     */
    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = Objects.requireNonNullElseGet(attributes, LinkedHashMap::new);
        // Mark all attributes as modified when setting the entire map
        if (attributes != null) {
            modifiedAttributes.addAll(attributes.keySet());
        }
    }

    /**
     * Sets a boolean attribute.
     *
     * @param key Attribute key
     * @param value Boolean value
     */
    public void set(String key, Boolean value) {
        if (value != null) {
            attributes.put(key, value);
            modifiedAttributes.add(key);
        }
    }

    /**
     * Sets a byte attribute (stored as integer).
     *
     * @param key Attribute key
     * @param value Byte value
     */
    public void set(String key, Byte value) {
        if (value != null) {
            attributes.put(key, value.intValue());
            modifiedAttributes.add(key);
        }
    }

    /**
     * Sets a short attribute (stored as integer).
     *
     * @param key Attribute key
     * @param value Short value
     */
    public void set(String key, Short value) {
        if (value != null) {
            attributes.put(key, value.intValue());
            modifiedAttributes.add(key);
        }
    }

    /**
     * Sets an integer attribute.
     *
     * @param key Attribute key
     * @param value Integer value
     */
    public void set(String key, Integer value) {
        if (value != null) {
            attributes.put(key, value);
            modifiedAttributes.add(key);
        }
    }

    /**
     * Sets a long attribute.
     *
     * @param key Attribute key
     * @param value Long value
     */
    public void set(String key, Long value) {
        if (value != null) {
            attributes.put(key, value);
            modifiedAttributes.add(key);
        }
    }

    /**
     * Sets a float attribute (stored as double).
     *
     * @param key Attribute key
     * @param value Float value
     */
    public void set(String key, Float value) {
        if (value != null) {
            attributes.put(key, value.doubleValue());
            modifiedAttributes.add(key);
        }
    }

    /**
     * Sets a double attribute.
     *
     * @param key Attribute key
     * @param value Double value
     */
    public void set(String key, Double value) {
        if (value != null) {
            attributes.put(key, value);
            modifiedAttributes.add(key);
        }
    }

    /**
     * Sets a string attribute.
     *
     * @param key Attribute key
     * @param value String value
     */
    public void set(String key, String value) {
        if (value != null && !value.isEmpty()) {
            attributes.put(key, value);
            modifiedAttributes.add(key);
        }
    }

    /**
     * Adds an attribute from a map entry.
     *
     * @param entry Map entry with key and value
     */
    public void add(Map.Entry<String, Object> entry) {
        if (entry != null && entry.getValue() != null) {
            attributes.put(entry.getKey(), entry.getValue());
            modifiedAttributes.add(entry.getKey());
        }
    }

    /**
     * Generic setter for any attribute from JSON deserialization.
     *
     * @param key Attribute key
     * @param value Attribute value
     */
    @JsonAnySetter
    public void setAttribute(String key, Object value) {
        if (value != null) {
            attributes.put(key, value);
            modifiedAttributes.add(key);
        }
    }

    /**
     * Gets a string attribute with default value.
     *
     * @param key Attribute key
     * @param defaultValue Default value if attribute not found
     * @return String value or default
     */
    public String getString(String key, String defaultValue) {
        return parseAsString(attributes.get(key), defaultValue);
    }

    /**
     * Gets a string attribute.
     *
     * @param key Attribute key
     * @return String value or null
     */
    public String getString(String key) {
        return parseAsString(attributes.get(key), null);
    }

    /**
     * Gets a double attribute.
     *
     * @param key Attribute key
     * @return Double value or 0.0
     */
    public double getDouble(String key) {
        return parseAsDouble(attributes.get(key), 0.0);
    }

    /**
     * Gets a boolean attribute.
     *
     * @param key Attribute key
     * @return Boolean value or false
     */
    public boolean getBoolean(String key) {
        return parseAsBoolean(attributes.get(key), false);
    }

    /**
     * Gets an integer attribute.
     *
     * @param key Attribute key
     * @return Integer value or 0
     */
    public int getInteger(String key) {
        return parseAsInteger(attributes.get(key), 0);
    }

    /**
     * Gets a long attribute.
     *
     * @param key Attribute key
     * @return Long value or 0L
     */
    public long getLong(String key) {
        return parseAsLong(attributes.get(key), 0L);
    }

    /**
     * Removes an attribute.
     *
     * @param key Attribute key
     * @return Removed value or null
     */
    public Object removeAttribute(String key) {
        modifiedAttributes.add(key);
        return attributes.remove(key);
    }

    /**
     * Removes a string attribute.
     *
     * @param key Attribute key
     * @return Removed string value or null
     */
    public String removeString(String key) {
        modifiedAttributes.add(key);
        return parseAsString(attributes.remove(key), null);
    }

    /**
     * Removes a double attribute.
     *
     * @param key Attribute key
     * @return Removed double value or null
     */
    public Double removeDouble(String key) {
        modifiedAttributes.add(key);
        return parseAsDouble(attributes.remove(key), null);
    }

    /**
     * Removes a boolean attribute.
     *
     * @param key Attribute key
     * @return Removed boolean value or null
     */
    public Boolean removeBoolean(String key) {
        modifiedAttributes.add(key);
        return parseAsBoolean(attributes.remove(key), null);
    }

    /**
     * Removes an integer attribute.
     *
     * @param key Attribute key
     * @return Removed integer value or null
     */
    public Integer removeInteger(String key) {
        modifiedAttributes.add(key);
        return parseAsInteger(attributes.remove(key), null);
    }

    /**
     * Removes a long attribute.
     *
     * @param key Attribute key
     * @return Removed long value or null
     */
    public Long removeLong(String key) {
        modifiedAttributes.add(key);
        return parseAsLong(attributes.remove(key), null);
    }

    /**
     * Gets the set of modified attribute keys.
     *
     * @return Set of modified attribute keys
     */
    @JsonIgnore
    public Set<String> getModifiedAttributes() {
        return modifiedAttributes;
    }

    /**
     * Clears the set of modified attribute keys.
     */
    public void clearModifiedAttributes() {
        modifiedAttributes.clear();
    }

    /**
     * Merges attributes from another ExtendedModel.
     *
     * @param other Other ExtendedModel to merge from
     * @param overwrite Whether to overwrite existing attributes
     */
    public void mergeAttributes(ExtendedModel other, boolean overwrite) {
        if (other == null) {
            return;
        }

        for (Map.Entry<String, Object> entry : other.getAttributes().entrySet()) {
            if (overwrite || !attributes.containsKey(entry.getKey())) {
                attributes.put(entry.getKey(), entry.getValue());
                modifiedAttributes.add(entry.getKey());
            }
        }
    }

    /**
     * Merges only modified attributes from another ExtendedModel.
     *
     * @param other Other ExtendedModel to merge from
     */
    public void mergeModifiedAttributes(ExtendedModel other) {
        if (other == null) {
            return;
        }

        for (String key : other.getModifiedAttributes()) {
            if (other.getAttributes().containsKey(key)) {
                attributes.put(key, other.getAttributes().get(key));
                modifiedAttributes.add(key);
            } else {
                attributes.remove(key);
                modifiedAttributes.add(key);
            }
        }
    }

    private String parseAsString(Object value, String defaultValue) {
        if (value == null) {
            return defaultValue;
        } else {
            return value.toString();
        }
    }

    private static Double parseAsDouble(Object value, Double defaultValue) {
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Number numberValue) {
            return numberValue.doubleValue();
        } else {
            return Double.parseDouble(value.toString());
        }
    }

    private static Boolean parseAsBoolean(Object value, Boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Boolean booleanValue) {
            return booleanValue;
        } else {
            return Boolean.parseBoolean(value.toString());
        }
    }

    private static Integer parseAsInteger(Object value, Integer defaultValue) {
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Number numberValue) {
            return numberValue.intValue();
        } else {
            return Integer.parseInt(value.toString());
        }
    }

    private static Long parseAsLong(Object value, Long defaultValue) {
        if (value == null) {
            return defaultValue;
        } else if (value instanceof Number numberValue) {
            return numberValue.longValue();
        } else {
            return Long.parseLong(value.toString());
        }
    }
}