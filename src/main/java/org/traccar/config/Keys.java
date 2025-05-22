/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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
     * Connection timeout value in seconds. Because sometimes there might be no connection for some time.
     */
    public static final ConfigKey SERVER_TIMEOUT = new ConfigKey(
            "server.timeout", Integer.class);

    /**
     * Reports storage configuration
     */
    public static final ConfigKey REPORTS_STORAGE_TYPE = new ConfigKey(
            "reports.storage.type", String.class);

    public static final ConfigKey REPORTS_STORAGE_ENDPOINT = new ConfigKey(
            "reports.storage.endpoint", String.class);

    public static final ConfigKey REPORTS_STORAGE_BUCKET = new ConfigKey(
            "reports.storage.bucket", String.class);

    public static final ConfigKey REPORTS_STORAGE_ACCESS_KEY = new ConfigKey(
            "reports.storage.accessKey", String.class);

    public static final ConfigKey REPORTS_STORAGE_SECRET_KEY = new ConfigKey(
            "reports.storage.secretKey", String.class);

    public static final ConfigKey REPORTS_STORAGE_REGION = new ConfigKey(
            "reports.storage.region", String.class);

    /**
     * OpenTelemetry configuration
     */
    public static final ConfigKey OPENTELEMETRY_SERVICE_NAME = new ConfigKey(
            "opentelemetry.service.name", String.class);

    /**
     * Message broker configuration
     */
    public static final ConfigKey MESSAGE_BROKER_TYPE = new ConfigKey(
            "message.broker.type", String.class);

    public static final ConfigKey MESSAGE_BROKER_URL = new ConfigKey(
            "message.broker.url", String.class);

    /**
     * Service discovery configuration
     */
    public static final ConfigKey SERVICE_DISCOVERY_TYPE = new ConfigKey(
            "service.discovery.type", String.class);

    public static final ConfigKey SERVICE_DISCOVERY_URL = new ConfigKey(
            "service.discovery.url", String.class);

    private Keys() {
    }

}