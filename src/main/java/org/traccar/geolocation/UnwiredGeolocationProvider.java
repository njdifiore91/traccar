/*
 * Copyright 2017 - 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.geolocation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import org.traccar.model.CellTower;
import org.traccar.model.Network;
import org.traccar.model.WifiAccessPoint;

import jakarta.json.JsonObject;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.InvocationCallback;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

public class UnwiredGeolocationProvider implements GeolocationProvider {

    private final Client client;
    private final String url;
    private final String key;
    private final ObjectMapper objectMapper;
    private final GeolocationCircuitBreakerFactory circuitBreakerFactory;
    private final GeolocationMetricsCollector metricsCollector;
    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;
    private final GeolocationServiceManager serviceManager;

    private abstract static class NetworkMixIn {
        @JsonProperty("mcc")
        abstract Integer getHomeMobileCountryCode();
        @JsonProperty("mnc")
        abstract Integer getHomeMobileNetworkCode();
        @JsonProperty("radio")
        abstract String getRadioType();
        @JsonIgnore
        abstract String getCarrier();
        @JsonIgnore
        abstract Boolean getConsiderIp();
        @JsonProperty("cells")
        abstract Collection<CellTower> getCellTowers();
        @JsonProperty("wifi")
        abstract Collection<WifiAccessPoint> getWifiAccessPoints();
    }

    private abstract static class CellTowerMixIn {
        @JsonProperty("radio")
        abstract String getRadioType();
        @JsonProperty("mcc")
        abstract Integer getMobileCountryCode();
        @JsonProperty("mnc")
        abstract Integer getMobileNetworkCode();
        @JsonProperty("lac")
        abstract Integer getLocationAreaCode();
        @JsonProperty("cid")
        abstract Long getCellId();
    }

    private abstract static class WifiAccessPointMixIn {
        @JsonProperty("bssid")
        abstract String getMacAddress();
        @JsonProperty("signal")
        abstract Integer getSignalStrength();
    }

    @Inject
    public UnwiredGeolocationProvider(
            Client client,
            @Named("geolocation.unwired.url") String url,
            @Named("geolocation.unwired.key") String key,
            GeolocationCircuitBreakerFactory circuitBreakerFactory,
            GeolocationMetricsCollector metricsCollector,
            Tracer tracer,
            GeolocationServiceManager serviceManager) {
        this.client = client;
        this.url = getConfiguredUrl(url);
        this.key = getConfiguredKey(key);
        this.circuitBreakerFactory = circuitBreakerFactory;
        this.metricsCollector = metricsCollector;
        this.tracer = tracer;
        this.serviceManager = serviceManager;
        this.circuitBreaker = circuitBreakerFactory.create("unwired-geolocation");

        objectMapper = new ObjectMapper();
        objectMapper.addMixIn(Network.class, NetworkMixIn.class);
        objectMapper.addMixIn(CellTower.class, CellTowerMixIn.class);
        objectMapper.addMixIn(WifiAccessPoint.class, WifiAccessPointMixIn.class);
        
        // Register with service manager
        serviceManager.registerProvider("unwired", this);
    }
    
    private String getConfiguredUrl(String defaultUrl) {
        String envUrl = System.getenv("UNWIRED_GEOLOCATION_URL");
        return envUrl != null ? envUrl : defaultUrl;
    }
    
    private String getConfiguredKey(String defaultKey) {
        String envKey = System.getenv("UNWIRED_GEOLOCATION_KEY");
        return envKey != null ? envKey : defaultKey;
    }

    @Override
    public void getLocation(Network network, final LocationProviderCallback callback) {
        // Create a span for this geolocation request
        Span span = tracer.spanBuilder("unwired.geolocation.request")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("geolocation.provider", "unwired")
                .setAttribute("geolocation.network.cells", network.getCellTowers() != null ? network.getCellTowers().size() : 0)
                .setAttribute("geolocation.network.wifi", network.getWifiAccessPoints() != null ? network.getWifiAccessPoints().size() : 0)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Start metrics timer
            long startTime = System.currentTimeMillis();
            
            // Use circuit breaker to handle failures
            CompletableFuture<Void> future = circuitBreakerFactory.executeSupplier(circuitBreaker, () -> {
                ObjectNode json = objectMapper.valueToTree(network);
                json.put("token", key);
                
                span.setAttribute("geolocation.request.url", url);
                
                client.target(url).request().async().post(Entity.json(json), new InvocationCallback<JsonObject>() {
                    @Override
                    public void completed(JsonObject json) {
                        try (Scope innerScope = Context.current().with(span).makeCurrent()) {
                            long duration = System.currentTimeMillis() - startTime;
                            span.setAttribute("geolocation.response.time_ms", duration);
                            
                            if (json.getString("status").equals("error")) {
                                String errorMessage = json.getString("message");
                                span.setStatus(StatusCode.ERROR, errorMessage);
                                span.setAttribute("geolocation.error", errorMessage);
                                
                                GeolocationException exception = new GeolocationException(errorMessage);
                                metricsCollector.recordFailure("unwired", duration, exception);
                                callback.onFailure(exception);
                            } else {
                                double latitude = json.getJsonNumber("lat").doubleValue();
                                double longitude = json.getJsonNumber("lon").doubleValue();
                                double accuracy = json.getJsonNumber("accuracy").doubleValue();
                                
                                span.setAttribute("geolocation.result.latitude", latitude);
                                span.setAttribute("geolocation.result.longitude", longitude);
                                span.setAttribute("geolocation.result.accuracy", accuracy);
                                span.setStatus(StatusCode.OK);
                                
                                metricsCollector.recordSuccess("unwired", duration);
                                callback.onSuccess(latitude, longitude, accuracy);
                            }
                        } finally {
                            span.end();
                        }
                    }

                    @Override
                    public void failed(Throwable throwable) {
                        try (Scope innerScope = Context.current().with(span).makeCurrent()) {
                            long duration = System.currentTimeMillis() - startTime;
                            span.setStatus(StatusCode.ERROR, throwable.getMessage());
                            span.setAttribute("geolocation.error", throwable.getMessage());
                            span.recordException(throwable);
                            
                            metricsCollector.recordFailure("unwired", duration, throwable);
                            callback.onFailure(throwable);
                        } finally {
                            span.end();
                        }
                    }
                });
                
                return CompletableFuture.completedFuture(null);
            }).exceptionally(throwable -> {
                try (Scope innerScope = Context.current().with(span).makeCurrent()) {
                    long duration = System.currentTimeMillis() - startTime;
                    span.setStatus(StatusCode.ERROR, throwable.getMessage());
                    span.setAttribute("geolocation.error", throwable.getMessage());
                    span.setAttribute("geolocation.circuit_breaker.state", circuitBreaker.getState().name());
                    span.recordException(throwable);
                    
                    metricsCollector.recordFailure("unwired", duration, throwable);
                    callback.onFailure(throwable);
                } finally {
                    span.end();
                }
                return null;
            });
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            span.end();
            callback.onFailure(e);
        }
    }
}