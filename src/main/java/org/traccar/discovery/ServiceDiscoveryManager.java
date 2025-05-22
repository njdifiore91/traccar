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
package org.traccar.discovery;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.ecwid.consul.v1.session.model.Session;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages service discovery and leader election using Consul.
 * This class is responsible for registering services and acquiring leadership for tasks.
 */
@Singleton
public class ServiceDiscoveryManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServiceDiscoveryManager.class);
    private static final String KEY_PREFIX = "traccar/leader/";
    private static final int SESSION_TTL_SECONDS = 30;

    private final ConsulClient consulClient;
    private final String serviceId;
    private final Map<String, String> sessionIds = new ConcurrentHashMap<>();

    @Inject
    public ServiceDiscoveryManager(Config config) {
        String consulHost = config.getString(Keys.SERVICE_DISCOVERY_CONSUL_HOST, "localhost");
        int consulPort = config.getInteger(Keys.SERVICE_DISCOVERY_CONSUL_PORT, 8500);
        this.consulClient = new ConsulClient(consulHost, consulPort);
        
        // Generate a unique service ID based on hostname and process ID
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            LOGGER.warn("Could not determine hostname", e);
            hostname = "unknown";
        }
        this.serviceId = hostname + "-" + ProcessHandle.current().pid();
        LOGGER.info("Service discovery initialized with service ID: {}", serviceId);
    }

    /**
     * Attempts to acquire leadership for a task.
     *
     * @param taskName the name of the task to acquire leadership for
     * @return true if leadership was acquired, false otherwise
     */
    public boolean acquireLeadership(String taskName) {
        try {
            String sessionId = createSession(taskName);
            if (sessionId == null) {
                return false;
            }
            
            sessionIds.put(taskName, sessionId);
            String key = KEY_PREFIX + taskName;
            PutParams putParams = new PutParams();
            putParams.setAcquireSession(sessionId);
            
            boolean success = consulClient.setKVValue(key, serviceId, putParams).getValue();
            if (success) {
                LOGGER.debug("Acquired leadership for task: {} with session: {}", taskName, sessionId);
            } else {
                LOGGER.debug("Failed to acquire leadership for task: {}", taskName);
            }
            return success;
        } catch (Exception e) {
            LOGGER.error("Error acquiring leadership for task: {}", taskName, e);
            return false;
        }
    }

    /**
     * Renews leadership for a task.
     *
     * @param taskName the name of the task to renew leadership for
     * @return true if leadership was renewed, false otherwise
     */
    public boolean renewLeadership(String taskName) {
        try {
            String sessionId = sessionIds.get(taskName);
            if (sessionId == null) {
                return false;
            }
            
            // Renew the session
            consulClient.sessionRenew(sessionId, null);
            
            // Verify we still hold the lock
            String key = KEY_PREFIX + taskName;
            var response = consulClient.getKVValue(key);
            if (response.getValue() != null && 
                response.getValue().getSession() != null && 
                response.getValue().getSession().equals(sessionId)) {
                return true;
            } else {
                LOGGER.debug("Lost leadership for task: {}", taskName);
                sessionIds.remove(taskName);
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Error renewing leadership for task: {}", taskName, e);
            sessionIds.remove(taskName);
            return false;
        }
    }

    /**
     * Releases leadership for a task.
     *
     * @param taskName the name of the task to release leadership for
     */
    public void releaseLeadership(String taskName) {
        try {
            String sessionId = sessionIds.remove(taskName);
            if (sessionId != null) {
                String key = KEY_PREFIX + taskName;
                PutParams putParams = new PutParams();
                putParams.setReleaseSession(sessionId);
                consulClient.setKVValue(key, "", putParams);
                consulClient.sessionDestroy(sessionId, null);
                LOGGER.debug("Released leadership for task: {}", taskName);
            }
        } catch (Exception e) {
            LOGGER.error("Error releasing leadership for task: {}", taskName, e);
        }
    }

    private String createSession(String taskName) {
        try {
            NewSession newSession = new NewSession();
            newSession.setName("traccar-" + taskName);
            newSession.setTtl(SESSION_TTL_SECONDS + "s");
            newSession.setBehavior(Session.Behavior.RELEASE);
            String sessionId = consulClient.sessionCreate(newSession, null).getValue();
            LOGGER.debug("Created session: {} for task: {}", sessionId, taskName);
            return sessionId;
        } catch (Exception e) {
            LOGGER.error("Error creating session for task: {}", taskName, e);
            return null;
        }
    }
}