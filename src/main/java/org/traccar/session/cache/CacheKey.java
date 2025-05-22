/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.session.cache;

import org.traccar.model.BaseModel;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * A composite key for cache entries that combines a BaseModel's class type and unique ID.
 * This class is serializable for Redis compatibility and includes security measures
 * for safe serialization/deserialization of class references.
 */
record CacheKey(Class<? extends BaseModel> clazz, long id) implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    // Allowlist of permitted model package prefixes for security
    private static final String[] ALLOWED_PACKAGE_PREFIXES = {
        "org.traccar.model."
    };
    
    /**
     * Creates a cache key from a BaseModel object.
     * 
     * @param object The BaseModel object to create a key for
     */
    CacheKey(BaseModel object) {
        this(object.getClass(), object.getId());
    }
    
    /**
     * Custom serialization method to handle Class object serialization safely.
     * Stores the class name instead of serializing the Class object directly.
     * 
     * @param out The output stream to write to
     * @throws IOException If an I/O error occurs
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject(); // Write the default serializable fields (id)
        out.writeUTF(clazz.getName()); // Write the class name as a string
    }
    
    /**
     * Custom deserialization method to safely restore the Class object.
     * Validates the class name against an allowlist to prevent security issues.
     * 
     * @param in The input stream to read from
     * @throws IOException If an I/O error occurs
     * @throws ClassNotFoundException If the class cannot be found
     * @throws SecurityException If the class name is not in the allowlist
     */
    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject(); // Read the default serializable fields (id)
        String className = in.readUTF(); // Read the class name
        
        // Validate the class name against the allowlist for security
        validateClassName(className);
        
        // Load the class safely
        Class<?> loadedClass = Class.forName(className);
        
        // Verify it's a BaseModel subclass
        if (!BaseModel.class.isAssignableFrom(loadedClass)) {
            throw new SecurityException("Deserialized class is not a BaseModel subclass: " + className);
        }
        
        // Use reflection to set the clazz field
        try {
            java.lang.reflect.Field clazzField = CacheKey.class.getDeclaredField("clazz");
            clazzField.setAccessible(true);
            clazzField.set(this, loadedClass);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new IOException("Failed to set class field during deserialization", e);
        }
    }
    
    /**
     * Validates that the class name is from an allowed package for security.
     * 
     * @param className The class name to validate
     * @throws SecurityException If the class name is not from an allowed package
     */
    private void validateClassName(String className) {
        boolean allowed = false;
        for (String prefix : ALLOWED_PACKAGE_PREFIXES) {
            if (className.startsWith(prefix)) {
                allowed = true;
                break;
            }
        }
        
        if (!allowed) {
            throw new SecurityException("Deserialization not allowed for class: " + className);
        }
    }
}