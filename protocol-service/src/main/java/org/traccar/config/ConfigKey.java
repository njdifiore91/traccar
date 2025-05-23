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
package org.traccar.config;

/**
 * Configuration keys for the Protocol Service microservice.
 */
public enum ConfigKey {

    // Protocol service specific configuration keys
    SERVICE_DISCOVERY_ENABLED("service.discovery.enabled", Boolean.class, false),
    SERVICE_DISCOVERY_TYPE("service.discovery.type", String.class, "consul"),
    SERVICE_DISCOVERY_CONSUL_HOST("service.discovery.consul.host", String.class, "localhost"),
    SERVICE_DISCOVERY_CONSUL_PORT("service.discovery.consul.port", Integer.class, 8500),
    SERVICE_DISCOVERY_K8S_NAMESPACE("service.discovery.k8s.namespace", String.class, "default"),
    SERVICE_DISCOVERY_K8S_SERVICE_NAME("service.discovery.k8s.serviceName", String.class, "protocol-service"),
    
    MESSAGE_BROKER_ENABLED("message.broker.enabled", Boolean.class, true),
    MESSAGE_BROKER_TYPE("message.broker.type", String.class, "kafka"),
    MESSAGE_BROKER_KAFKA_BOOTSTRAP_SERVERS("message.broker.kafka.bootstrapServers", String.class, "localhost:9092"),
    MESSAGE_BROKER_KAFKA_POSITIONS_TOPIC("message.broker.kafka.positions.topic", String.class, "positions"),
    MESSAGE_BROKER_RABBITMQ_HOST("message.broker.rabbitmq.host", String.class, "localhost"),
    MESSAGE_BROKER_RABBITMQ_PORT("message.broker.rabbitmq.port", Integer.class, 5672),
    MESSAGE_BROKER_RABBITMQ_POSITIONS_QUEUE("message.broker.rabbitmq.positions.queue", String.class, "positions"),
    
    HEALTH_CHECK_ENABLED("health.check.enabled", Boolean.class, true),
    HEALTH_CHECK_PORT("health.check.port", Integer.class, 8082),
    
    METRICS_ENABLED("metrics.enabled", Boolean.class, true),
    METRICS_PORT("metrics.port", Integer.class, 8083),
    
    // Executor service configuration
    EXECUTOR_SERVICE_THREADS("executor.threads", Integer.class, 0),
    
    // Server configuration
    SERVER_TIMEOUT("server.timeout", Integer.class, 0),
    SERVER_WORKER_THREADS("server.workerThreads", Integer.class, 0),
    SERVER_BOSS_THREADS("server.bossThreads", Integer.class, 1);

    private final String key;
    private final Class<?> type;
    private final Object defaultValue;

    ConfigKey(String key, Class<?> type, Object defaultValue) {
        this.key = key;
        this.type = type;
        this.defaultValue = defaultValue;
    }

    public String getKey() {
        return key;
    }

    public Class<?> getType() {
        return type;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

}