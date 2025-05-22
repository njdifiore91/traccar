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
package org.traccar.config;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for validating configuration values against their defined rules.
 */
public class ConfigValidator {

    /**
     * Represents a validation error for a specific configuration key.
     */
    public static class ValidationError {
        private final String key;
        private final String message;

        /**
         * Creates a new validation error.
         *
         * @param key     The configuration key that failed validation
         * @param message The validation error message
         */
        public ValidationError(String key, String message) {
            this.key = key;
            this.message = message;
        }

        /**
         * @return The configuration key that failed validation
         */
        public String getKey() {
            return key;
        }

        /**
         * @return The validation error message
         */
        public String getMessage() {
            return message;
        }

        @Override
        public String toString() {
            return "Configuration error for '" + key + "': " + message;
        }
    }

    /**
     * Validates a configuration value against its key definition.
     *
     * @param key   The configuration key definition
     * @param value The value to validate
     * @param <T>   The type of the configuration value
     * @return A validation error if validation fails, or null if validation passes
     */
    public static <T> ValidationError validate(ConfigKey<T> key, T value) {
        if (key.hasValidation() && value != null && !key.validate(value)) {
            return new ValidationError(key.getKey(), key.getValidationMessage());
        }
        return null;
    }

    /**
     * Validates multiple configuration values against their key definitions.
     *
     * @param config The configuration object containing key-value pairs
     * @param keys   The configuration key definitions to validate
     * @return A list of validation errors, empty if all validations pass
     */
    public static List<ValidationError> validateAll(Config config, List<ConfigKey<?>> keys) {
        List<ValidationError> errors = new ArrayList<>();
        for (ConfigKey<?> key : keys) {
            if (key.hasValidation() && config.hasKey(key.getKey())) {
                Object value = config.getString(key.getKey());
                if (value != null) {
                    try {
                        // Convert the value to the correct type for validation
                        Object typedValue = config.getValue(key);
                        ValidationError error = validateTyped(key, typedValue);
                        if (error != null) {
                            errors.add(error);
                        }
                    } catch (Exception e) {
                        errors.add(new ValidationError(key.getKey(), 
                            "Invalid value type: " + e.getMessage()));
                    }
                }
            }
        }
        return errors;
    }

    @SuppressWarnings("unchecked")
    private static <T> ValidationError validateTyped(ConfigKey<T> key, Object value) {
        if (key.hasValidation() && value != null) {
            T typedValue = (T) value;
            if (!key.validate(typedValue)) {
                return new ValidationError(key.getKey(), key.getValidationMessage());
            }
        }
        return null;
    }
}