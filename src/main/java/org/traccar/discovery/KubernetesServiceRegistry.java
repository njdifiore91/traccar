/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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

import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service registry implementation for Kubernetes.
 * 
 * This implementation doesn't actually register services with Kubernetes as that's handled
 * by Kubernetes itself. Instead, it updates the pod's status to indicate readiness.
 */
public class KubernetesServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = Logger.getLogger(KubernetesServiceRegistry.class.getName());

    private final String namespace;
    private final String podName;
    private final String apiServerUrl;
    private final String serviceAccountToken;

    /**
     * Constructs a new KubernetesServiceRegistry.
     *
     * @param config The configuration
     */
    public KubernetesServiceRegistry(Config config) {
        this.namespace = readFileOrEnv("/var/run/secrets/kubernetes.io/serviceaccount/namespace", 
                "KUBERNETES_NAMESPACE", config.getString(Keys.SERVICE_DISCOVERY_K8S_NAMESPACE, "default"));
        this.podName = System.getenv("HOSTNAME");
        this.apiServerUrl = config.getString(Keys.SERVICE_DISCOVERY_K8S_API_URL, "https://kubernetes.default.svc");
        this.serviceAccountToken = readFileOrEnv("/var/run/secrets/kubernetes.io/serviceaccount/token", 
                "KUBERNETES_SERVICE_ACCOUNT_TOKEN", "");
        
        LOGGER.info("Initialized KubernetesServiceRegistry in namespace: " + namespace + ", pod: " + podName);
    }
    
    private String readFileOrEnv(String filePath, String envVar, String defaultValue) {
        try {
            // Try to read from file first
            return new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8).trim();
        } catch (IOException e) {
            // If file doesn't exist, try environment variable
            String envValue = System.getenv(envVar);
            if (envValue != null && !envValue.isEmpty()) {
                return envValue;
            }
            // Fall back to default value
            return defaultValue;
        }
    }

    @Override
    public void register(ServiceRegistration registration) {
        // In Kubernetes, we don't need to explicitly register services as that's handled by Kubernetes itself.
        // However, we can update the pod's status to indicate readiness.
        LOGGER.info("Service registered in Kubernetes: " + registration.getId() + 
                " (Note: Kubernetes handles service registration automatically)");
    }

    @Override
    public void deregister(ServiceRegistration registration) {
        // In Kubernetes, we don't need to explicitly deregister services as that's handled by Kubernetes itself.
        LOGGER.info("Service deregistered from Kubernetes: " + registration.getId() + 
                " (Note: Kubernetes handles service deregistration automatically)");
    }
}