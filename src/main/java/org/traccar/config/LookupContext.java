/*
 * Copyright 2020 - 2023 Anton Tananaev (anton@traccar.org)
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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Interface defining context for configuration lookup.
 * Provides context-specific configuration resolution across service boundaries.
 */
public interface LookupContext extends Serializable {

    /**
     * Get context type identifier.
     * Used for serialization and cross-service propagation.
     *
     * @return String identifier of the context type
     */
    default String getContextType() {
        return getClass().getSimpleName();
    }
    
    /**
     * Convert context to a map representation for transport across service boundaries.
     *
     * @return Map containing context data
     */
    default Map<String, String> toMap() {
        return new HashMap<>();
    }

    /**
     * Global context for system-wide configuration.
     */
    class Global implements LookupContext {
        
        @Override
        public Map<String, String> toMap() {
            return new HashMap<>();
        }
    }

    /**
     * User-specific context for user-level configuration.
     */
    class User implements LookupContext {

        private final long userId;

        public long getUserId() {
            return userId;
        }

        public User(long userId) {
            this.userId = userId;
        }
        
        @Override
        public Map<String, String> toMap() {
            Map<String, String> map = new HashMap<>();
            map.put("userId", String.valueOf(userId));
            return map;
        }
    }

    /**
     * Device-specific context for device-level configuration.
     */
    class Device implements LookupContext {

        private final long deviceId;

        public long getDeviceId() {
            return deviceId;
        }

        public Device(long deviceId) {
            this.deviceId = deviceId;
        }
        
        @Override
        public Map<String, String> toMap() {
            Map<String, String> map = new HashMap<>();
            map.put("deviceId", String.valueOf(deviceId));
            return map;
        }
    }
    
    /**
     * Service-specific context for service-level configuration.
     * Enables distributed configuration lookup across service boundaries.
     */
    class Service implements LookupContext {
        
        private final String serviceName;
        private final String instanceId;
        private final Map<String, String> attributes;
        
        public Service(String serviceName, String instanceId) {
            this(serviceName, instanceId, new HashMap<>());
        }
        
        public Service(String serviceName, String instanceId, Map<String, String> attributes) {
            this.serviceName = serviceName;
            this.instanceId = instanceId;
            this.attributes = attributes;
        }
        
        public String getServiceName() {
            return serviceName;
        }
        
        public String getInstanceId() {
            return instanceId;
        }
        
        public Map<String, String> getAttributes() {
            return attributes;
        }
        
        @Override
        public Map<String, String> toMap() {
            Map<String, String> map = new HashMap<>();
            map.put("serviceName", serviceName);
            map.put("instanceId", instanceId);
            map.putAll(attributes);
            return map;
        }
    }
    
    /**
     * Combined context that allows layered configuration resolution.
     * Supports configuration inheritance and overrides across multiple context types.
     */
    class Combined implements LookupContext {
        
        private final LookupContext[] contexts;
        
        public Combined(LookupContext... contexts) {
            this.contexts = contexts;
        }
        
        public LookupContext[] getContexts() {
            return contexts;
        }
        
        @Override
        public Map<String, String> toMap() {
            Map<String, String> map = new HashMap<>();
            for (LookupContext context : contexts) {
                map.put("context_" + context.getContextType(), context.getContextType());
                map.putAll(context.toMap());
            }
            return map;
        }
    }
    
    /**
     * Factory method to create context from map representation.
     * Used for deserializing context data received from other services.
     *
     * @param contextType Type of context to create
     * @param data Map containing context data
     * @return LookupContext instance
     */
    static LookupContext fromMap(String contextType, Map<String, String> data) {
        switch (contextType) {
            case "Global":
                return new Global();
            case "User":
                return new User(Long.parseLong(data.get("userId")));
            case "Device":
                return new Device(Long.parseLong(data.get("deviceId")));
            case "Service":
                Map<String, String> attributes = new HashMap<>(data);
                attributes.remove("serviceName");
                attributes.remove("instanceId");
                return new Service(data.get("serviceName"), data.get("instanceId"), attributes);
            default:
                throw new IllegalArgumentException("Unknown context type: " + contextType);
        }
    }
}