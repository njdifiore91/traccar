/*
 * Copyright 2017 - 2022 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 Andrey Kunitsyn (andrey@traccar.org)
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

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Base model class for all entities in the system.
 * Provides common fields and functionality for serialization and tracing.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class BaseModel implements Serializable {

    private static final long serialVersionUID = 1L;
    
    /**
     * Current schema version for this model.
     * Should be incremented when the model structure changes.
     */
    private int schemaVersion = 1;
    
    /**
     * Tracing context for distributed tracing support.
     * Used to correlate requests across microservices.
     */
    private TracingContext tracingContext;
    
    private long id;

    /**
     * Gets the unique identifier of this entity.
     * 
     * @return The entity ID
     */
    public long getId() {
        return id;
    }

    /**
     * Sets the unique identifier of this entity.
     * 
     * @param id The entity ID to set
     */
    public void setId(long id) {
        this.id = id;
    }
    
    /**
     * Gets the schema version of this model.
     * 
     * @return The schema version
     */
    public int getSchemaVersion() {
        return schemaVersion;
    }
    
    /**
     * Sets the schema version of this model.
     * 
     * @param schemaVersion The schema version to set
     */
    public void setSchemaVersion(int schemaVersion) {
        this.schemaVersion = schemaVersion;
    }
    
    /**
     * Gets the tracing context for distributed tracing.
     * 
     * @return The tracing context
     */
    public TracingContext getTracingContext() {
        return tracingContext;
    }
    
    /**
     * Sets the tracing context for distributed tracing.
     * 
     * @param tracingContext The tracing context to set
     */
    public void setTracingContext(TracingContext tracingContext) {
        this.tracingContext = tracingContext;
    }
    
    /**
     * Validates if the provided schema version is compatible with this model's version.
     * 
     * @param version The version to validate against
     * @return true if the version is compatible, false otherwise
     */
    public boolean isVersionCompatible(int version) {
        return version <= schemaVersion;
    }
    
    /**
     * Validates the schema version before deserialization.
     * Throws an exception if the version is incompatible.
     * 
     * @param version The version to validate
     * @throws IllegalArgumentException if the version is incompatible
     */
    public void validateVersion(int version) {
        if (!isVersionCompatible(version)) {
            throw new IllegalArgumentException(
                "Incompatible schema version. Expected " + schemaVersion + ", got " + version);
        }
    }
}