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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service registry implementation for Consul.
 */
public class ConsulServiceRegistry implements ServiceRegistry {

    private static final Logger LOGGER = Logger.getLogger(ConsulServiceRegistry.class.getName());

    private final String consulUrl;
    private final String healthCheckPath;
    private final int healthCheckInterval;

    /**
     * Constructs a new ConsulServiceRegistry.
     *
     * @param config The configuration
     */
    public ConsulServiceRegistry(Config config) {
        this.consulUrl = config.getString(Keys.SERVICE_DISCOVERY_CONSUL_URL, "http://localhost:8500");
        this.healthCheckPath = config.getString(Keys.SERVICE_DISCOVERY_HEALTH_PATH, "/health");
        this.healthCheckInterval = config.getInteger(Keys.SERVICE_DISCOVERY_HEALTH_INTERVAL, 10);
        
        LOGGER.info("Initialized ConsulServiceRegistry with URL: " + consulUrl);
    }

    @Override
    public void register(ServiceRegistration registration) {
        try {
            // Build the registration JSON
            StringBuilder json = new StringBuilder();
            json.append("{")
                .append("\"ID\":\"").append(registration.getId()).append("\",")
                .append("\"Name\":\"").append(registration.getName()).append("\",")
                .append("\"Address\":\"").append(registration.getHost()).append("\",")
                .append("\"Port\":").append(registration.getPort()).append(",")
                .append("\"Tags\":[")
                .append("\"protocol=").append(registration.getMetadata().get("protocol")).append("\",")
                .append("\"secure=").append(registration.getMetadata().get("secure")).append("\",")
                .append("\"version=").append(registration.getMetadata().get("version")).append("\"")
                .append("],")
                .append("\"Check\":{")
                .append("\"HTTP\":\"").append("http://").append(registration.getHost()).append(":")
                .append(registration.getPort()).append(healthCheckPath).append("\",")
                .append("\"Interval\":\"").append(healthCheckInterval).append("s\"")
                .append("}")
                .append("}");

            // Send the registration to Consul
            URL url = new URL(consulUrl + "/v1/agent/service/register");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);
            connection.getOutputStream().write(json.toString().getBytes(StandardCharsets.UTF_8));

            int responseCode = connection.getResponseCode();
            if (responseCode != 200) {
                LOGGER.warning("Failed to register service with Consul: " + responseCode);
            } else {
                LOGGER.info("Registered service with Consul: " + registration.getId());
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error registering service with Consul", e);
        }
    }

    @Override
    public void deregister(ServiceRegistration registration) {
        try {
            // Send the deregistration to Consul
            URL url = new URL(consulUrl + "/v1/agent/service/deregister/" + registration.getId());
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("PUT");

            int responseCode = connection.getResponseCode();
            if (responseCode != 200) {
                LOGGER.warning("Failed to deregister service from Consul: " + responseCode);
            } else {
                LOGGER.info("Deregistered service from Consul: " + registration.getId());
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Error deregistering service from Consul", e);
        }
    }
}