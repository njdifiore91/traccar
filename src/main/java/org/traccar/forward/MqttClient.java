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
package org.traccar.forward;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5Client;
import com.hivemq.client.mqtt.mqtt5.Mqtt5ClientBuilder;
import com.hivemq.client.mqtt.mqtt5.message.auth.Mqtt5SimpleAuth;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishBuilder;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;

public class MqttClient {

    private final Mqtt5AsyncClient client;

    MqttClient(String url) {
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        Mqtt5SimpleAuth simpleAuth = this.getSimpleAuth(uri);

        String host = uri.getHost();
        int port = uri.getPort();
        Mqtt5ClientBuilder builder = Mqtt5Client.builder().identifier("traccar-" + UUID.randomUUID())
                .serverHost(host).serverPort(port).simpleAuth(simpleAuth).automaticReconnectWithDefaultConfig();

        client = builder.buildAsync();
        client.connectWith().send().whenComplete((message, e) -> {
            if (e != null) {
                throw new RuntimeException(e);
            }
        });
    }

    private Mqtt5SimpleAuth getSimpleAuth(URI uri) {
        String userInfo = uri.getUserInfo();
        Mqtt5SimpleAuth simpleAuth = null;
        if (userInfo != null) {
            int delimiter = userInfo.indexOf(':');
            if (delimiter == -1) {
                throw new IllegalArgumentException("Wrong MQTT credentials. Should be in format \"username:password\"");
            } else {
                simpleAuth = Mqtt5SimpleAuth.builder().username(userInfo.substring(0, delimiter++))
                        .password(userInfo.substring(delimiter).getBytes()).build();
            }
        }
        return simpleAuth;
    }

    /**
     * Publish a message to an MQTT topic with default QoS level (AT_LEAST_ONCE)
     * 
     * @param pubTopic    The topic to publish to
     * @param payload     The message payload
     * @param whenComplete Callback for completion or error
     */
    public void publish(
            String pubTopic, String payload, BiConsumer<? super Mqtt5PublishResult, ? super Throwable> whenComplete) {
        publish(pubTopic, payload, MqttQos.AT_LEAST_ONCE, null, whenComplete);
    }
    
    /**
     * Publish a message to an MQTT topic with specified QoS level and user properties for context propagation
     * 
     * @param pubTopic     The topic to publish to
     * @param payload      The message payload
     * @param qos          The quality of service level
     * @param userProps    User properties for context propagation (can be null)
     * @param whenComplete Callback for completion or error
     */
    public void publish(
            String pubTopic, String payload, MqttQos qos, Map<String, String> userProps,
            BiConsumer<? super Mqtt5PublishResult, ? super Throwable> whenComplete) {
        
        Mqtt5PublishBuilder.Complete<Void> publishBuilder = client.publishWith()
                .topic(pubTopic)
                .qos(qos)
                .payload(payload.getBytes());
        
        // Add user properties for context propagation if provided
        if (userProps != null && !userProps.isEmpty()) {
            for (Map.Entry<String, String> entry : userProps.entrySet()) {
                publishBuilder = publishBuilder.userProperty(entry.getKey(), entry.getValue());
            }
        }
        
        publishBuilder.send().whenComplete(whenComplete);
    }
}