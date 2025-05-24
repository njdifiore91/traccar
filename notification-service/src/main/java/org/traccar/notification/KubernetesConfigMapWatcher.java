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
package org.traccar.notification;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Base64;
import java.util.Map;

/**
 * Watches Kubernetes ConfigMaps and Secrets for changes and updates the properties accordingly.
 * This component enables dynamic configuration updates from Kubernetes without requiring
 * application restarts.
 */
@Component
@ConditionalOnProperty(name = "kubernetes.config.enabled", havingValue = "true")
public class KubernetesConfigMapWatcher {

    private static final Logger LOGGER = LoggerFactory.getLogger(KubernetesConfigMapWatcher.class);

    @Value("${kubernetes.namespace:default}")
    private String namespace;

    @Value("${kubernetes.configmap.name:notification-service-config}")
    private String configMapName;

    @Value("${kubernetes.secret.name:notification-service-secrets}")
    private String secretName;

    private final PropertiesProvider propertiesProvider;
    private KubernetesClient kubernetesClient;
    private SharedIndexInformer<ConfigMap> configMapInformer;
    private SharedIndexInformer<Secret> secretInformer;

    /**
     * Constructor with required dependencies.
     *
     * @param propertiesProvider Properties provider for updating properties
     */
    @Autowired
    public KubernetesConfigMapWatcher(PropertiesProvider propertiesProvider) {
        this.propertiesProvider = propertiesProvider;
    }

    /**
     * Initializes the Kubernetes client and starts watching for ConfigMap and Secret changes.
     */
    @PostConstruct
    public void init() {
        try {
            LOGGER.info("Initializing Kubernetes ConfigMap and Secret watcher");
            kubernetesClient = new KubernetesClientBuilder().build();

            // Watch ConfigMaps
            configMapInformer = kubernetesClient.configMaps()
                    .inNamespace(namespace)
                    .withName(configMapName)
                    .inform();

            configMapInformer.addEventHandler(new ResourceEventHandler<ConfigMap>() {
                @Override
                public void onAdd(ConfigMap configMap) {
                    processConfigMap(configMap);
                }

                @Override
                public void onUpdate(ConfigMap oldConfigMap, ConfigMap newConfigMap) {
                    processConfigMap(newConfigMap);
                }

                @Override
                public void onDelete(ConfigMap configMap, boolean deletedFinalStateUnknown) {
                    LOGGER.info("ConfigMap {} deleted", configMap.getMetadata().getName());
                }
            });

            // Watch Secrets
            secretInformer = kubernetesClient.secrets()
                    .inNamespace(namespace)
                    .withName(secretName)
                    .inform();

            secretInformer.addEventHandler(new ResourceEventHandler<Secret>() {
                @Override
                public void onAdd(Secret secret) {
                    processSecret(secret);
                }

                @Override
                public void onUpdate(Secret oldSecret, Secret newSecret) {
                    processSecret(newSecret);
                }

                @Override
                public void onDelete(Secret secret, boolean deletedFinalStateUnknown) {
                    LOGGER.info("Secret {} deleted", secret.getMetadata().getName());
                }
            });

            LOGGER.info("Kubernetes ConfigMap and Secret watcher initialized successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Kubernetes ConfigMap and Secret watcher", e);
        }
    }

    /**
     * Processes a ConfigMap and updates properties accordingly.
     *
     * @param configMap Kubernetes ConfigMap
     */
    private void processConfigMap(ConfigMap configMap) {
        try {
            LOGGER.info("Processing ConfigMap: {}", configMap.getMetadata().getName());
            Map<String, String> data = configMap.getData();
            if (data != null) {
                for (Map.Entry<String, String> entry : data.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    LOGGER.debug("Setting property from ConfigMap: {} = {}", key, value);
                    propertiesProvider.setProperty(key, value);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing ConfigMap", e);
        }
    }

    /**
     * Processes a Secret and updates properties accordingly.
     *
     * @param secret Kubernetes Secret
     */
    private void processSecret(Secret secret) {
        try {
            LOGGER.info("Processing Secret: {}", secret.getMetadata().getName());
            Map<String, String> data = secret.getData();
            if (data != null) {
                for (Map.Entry<String, String> entry : data.entrySet()) {
                    String key = entry.getKey();
                    // Decode base64-encoded secret value
                    String value = new String(Base64.getDecoder().decode(entry.getValue()));
                    LOGGER.debug("Setting property from Secret: {}", key);
                    propertiesProvider.setProperty(key, value);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing Secret", e);
        }
    }

    /**
     * Cleans up resources when the application is shutting down.
     */
    @PreDestroy
    public void cleanup() {
        try {
            LOGGER.info("Shutting down Kubernetes ConfigMap and Secret watcher");
            if (configMapInformer != null) {
                configMapInformer.close();
            }
            if (secretInformer != null) {
                secretInformer.close();
            }
            if (kubernetesClient != null) {
                kubernetesClient.close();
            }
        } catch (Exception e) {
            LOGGER.error("Error during Kubernetes client cleanup", e);
        }
    }
}