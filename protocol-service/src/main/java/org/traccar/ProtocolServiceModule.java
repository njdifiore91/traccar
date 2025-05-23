/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.traccar.config.Config;
import org.traccar.config.ConfigKey;
import org.traccar.config.Keys;
import org.traccar.config.XMLConfig;
import org.traccar.handler.NetworkMessageHandler;
import org.traccar.health.HealthCheckManager;
import org.traccar.metrics.MetricsRegistry;
import org.traccar.protocol.ProtocolManager;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Guice module for the Protocol Service microservice.
 * Configures dependency injection for protocol-specific components.
 */
public class ProtocolServiceModule extends AbstractModule {

    private final String configFile;

    public ProtocolServiceModule(String configFile) {
        this.configFile = configFile;
    }

    @Override
    protected void configure() {
        // Bind singleton instances
        bind(ProtocolManager.class).in(Scopes.SINGLETON);
        bind(ServerManager.class).in(Scopes.SINGLETON);
        bind(HealthCheckManager.class).in(Scopes.SINGLETON);
        bind(MetricsRegistry.class).in(Scopes.SINGLETON);
        bind(NetworkMessageHandler.class).in(Scopes.SINGLETON);
    }

    /**
     * Provides the configuration object.
     *
     * @return the configuration object
     */
    @Provides
    @Singleton
    public Config provideConfig() {
        Config config = new XMLConfig(configFile);
        
        // Register protocol service specific configuration keys
        Keys.register(ConfigKey.SERVICE_DISCOVERY_ENABLED);
        Keys.register(ConfigKey.SERVICE_DISCOVERY_TYPE);
        Keys.register(ConfigKey.SERVICE_DISCOVERY_CONSUL_HOST);
        Keys.register(ConfigKey.SERVICE_DISCOVERY_CONSUL_PORT);
        Keys.register(ConfigKey.SERVICE_DISCOVERY_K8S_NAMESPACE);
        Keys.register(ConfigKey.SERVICE_DISCOVERY_K8S_SERVICE_NAME);
        Keys.register(ConfigKey.MESSAGE_BROKER_ENABLED);
        Keys.register(ConfigKey.MESSAGE_BROKER_TYPE);
        Keys.register(ConfigKey.MESSAGE_BROKER_KAFKA_BOOTSTRAP_SERVERS);
        Keys.register(ConfigKey.MESSAGE_BROKER_KAFKA_POSITIONS_TOPIC);
        Keys.register(ConfigKey.MESSAGE_BROKER_RABBITMQ_HOST);
        Keys.register(ConfigKey.MESSAGE_BROKER_RABBITMQ_PORT);
        Keys.register(ConfigKey.MESSAGE_BROKER_RABBITMQ_POSITIONS_QUEUE);
        Keys.register(ConfigKey.HEALTH_CHECK_ENABLED);
        Keys.register(ConfigKey.HEALTH_CHECK_PORT);
        Keys.register(ConfigKey.METRICS_ENABLED);
        Keys.register(ConfigKey.METRICS_PORT);
        
        return config;
    }

    /**
     * Provides the executor service for background tasks.
     *
     * @param config the configuration object
     * @return the executor service
     */
    @Provides
    @Singleton
    public ExecutorService provideExecutorService(Config config) {
        int threadCount = config.getInteger(ConfigKey.EXECUTOR_SERVICE_THREADS.getKey());
        if (threadCount > 0) {
            return Executors.newFixedThreadPool(threadCount);
        } else {
            return Executors.newCachedThreadPool();
        }
    }
}