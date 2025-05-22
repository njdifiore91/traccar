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

public final class Keys {

    /**
     * Connection timeout value in seconds. Because sometimes there is no way to detect lost TCP connection old
     * connections stay in open state. On most systems there is a limit on number of open connection, so this leads to
     * problems with establishing new connections when number of devices is high or devices data connections are
     * unstable.
     */
    public static final ConfigKey SERVER_TIMEOUT = new ConfigKey(
            "server.timeout", Integer.class);

    /**
     * Enables forwarding locations to other web server.
     */
    public static final ConfigKey FORWARD_ENABLE = new ConfigKey(
            "forward.enable", Boolean.class);

    /**
     * URL to forward locations.
     */
    public static final ConfigKey FORWARD_URL = new ConfigKey(
            "forward.url", String.class);

    /**
     * Additional attributes for forwarding.
     */
    public static final ConfigKey FORWARD_ATTRIBUTES = new ConfigKey(
            "forward.attributes", String.class);

    /**
     * Position forwarding retrying enable.
     */
    public static final ConfigKey FORWARD_RETRY_ENABLE = new ConfigKey(
            "forward.retry.enable", Boolean.class);

    /**
     * Position forwarding retrying delay in seconds.
     */
    public static final ConfigKey FORWARD_RETRY_DELAY = new ConfigKey(
            "forward.retry.delay", Integer.class);

    /**
     * Position forwarding retrying count.
     */
    public static final ConfigKey FORWARD_RETRY_COUNT = new ConfigKey(
            "forward.retry.count", Integer.class);
            
    /**
     * Position forwarding retrying limit.
     */
    public static final ConfigKey FORWARD_RETRY_LIMIT = new ConfigKey(
            "forward.retry.limit", Integer.class);
            
    /**
     * Enable message broker for position forwarding.
     */
    public static final ConfigKey FORWARD_USE_MESSAGE_BROKER = new ConfigKey(
            "forward.use.message.broker", Boolean.class);
            
    /**
     * Topic name for position forwarding via message broker.
     */
    public static final ConfigKey FORWARD_TOPIC = new ConfigKey(
            "forward.topic", String.class);

    /**
     * Forward positions to all devices. Might be useful in case of different devices types, e.g. to convert protocols.
     */
    public static final ConfigKey FORWARD_ALL = new ConfigKey(
            "forward.all", Boolean.class);

    /**
     * Enable positions forwarding to URL.
     */
    public static final ConfigKey EVENT_FORWARD_ENABLE = new ConfigKey(
            "event.forward.enable", Boolean.class);

    /**
     * URL to forward events.
     */
    public static final ConfigKey EVENT_FORWARD_URL = new ConfigKey(
            "event.forward.url", String.class);

    /**
     * Additional attributes for events forwarding.
     */
    public static final ConfigKey EVENT_FORWARD_ATTRIBUTES = new ConfigKey(
            "event.forward.attributes", String.class);

    /**
     * Event forwarding retrying enable.
     */
    public static final ConfigKey EVENT_FORWARD_RETRY_ENABLE = new ConfigKey(
            "event.forward.retry.enable", Boolean.class);

    /**
     * Event forwarding retrying delay in seconds.
     */
    public static final ConfigKey EVENT_FORWARD_RETRY_DELAY = new ConfigKey(
            "event.forward.retry.delay", Integer.class);

    /**
     * Event forwarding retrying count.
     */
    public static final ConfigKey EVENT_FORWARD_RETRY_COUNT = new ConfigKey(
            "event.forward.retry.count", Integer.class);

    /**
     * Enable user notifications on events.
     */
    public static final ConfigKey NOTIFICATION_ENABLE = new ConfigKey(
            "notification.enable", Boolean.class);

    /**
     * Enable user notifications on user expiration.
     */
    public static final ConfigKey NOTIFICATION_EXPIRATION_USER = new ConfigKey(
            "notification.expiration.user", Boolean.class);

    /**
     * User expiration reminder time in milliseconds.
     */
    public static final ConfigKey NOTIFICATION_EXPIRATION_USER_REMINDER = new ConfigKey(
            "notification.expiration.user.reminder", Long.class);

    /**
     * Enable user notifications on device expiration.
     */
    public static final ConfigKey NOTIFICATION_EXPIRATION_DEVICE = new ConfigKey(
            "notification.expiration.device", Boolean.class);

    /**
     * Device expiration reminder time in milliseconds.
     */
    public static final ConfigKey NOTIFICATION_EXPIRATION_DEVICE_REMINDER = new ConfigKey(
            "notification.expiration.device.reminder", Long.class);

    /**
     * Service discovery Consul host.
     */
    public static final ConfigKey SERVICE_DISCOVERY_CONSUL_HOST = new ConfigKey(
            "service.discovery.consul.host", String.class);

    /**
     * Service discovery Consul port.
     */
    public static final ConfigKey SERVICE_DISCOVERY_CONSUL_PORT = new ConfigKey(
            "service.discovery.consul.port", Integer.class);

    /**
     * Enable message broker integration.
     */
    public static final ConfigKey MESSAGE_BROKER_ENABLED = new ConfigKey(
            "message.broker.enabled", Boolean.class);

    /**
     * Kafka bootstrap servers for message broker.
     */
    public static final ConfigKey MESSAGE_BROKER_KAFKA_BOOTSTRAP_SERVERS = new ConfigKey(
            "message.broker.kafka.bootstrap.servers", String.class);

    private Keys() {
    }

}