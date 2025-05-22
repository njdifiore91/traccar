/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.reports;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.helper.DateUtil;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Position;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;

import jakarta.inject.Inject;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CsvExportProvider {

    private final Storage storage;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final ServiceDiscovery serviceDiscovery;

    @Inject
    public CsvExportProvider(Storage storage, ServiceDiscovery serviceDiscovery, Tracer tracer) {
        this.storage = storage;
        this.serviceDiscovery = serviceDiscovery;
        this.tracer = tracer;

        // Configure circuit breaker for position service calls
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("csvExportProvider");
    }

    public void generate(
            OutputStream outputStream, long deviceId, Date from, Date to) throws StorageException {

        // Create a span for this operation
        Span span = tracer.spanBuilder("csv_export_generate")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", deviceId);
            if (from != null) span.setAttribute("fromDate", from.toString());
            if (to != null) span.setAttribute("toDate", to.toString());

            circuitBreaker.executeRunnable(() -> {
                try {
                    var positions = PositionUtil.getPositions(storage, deviceId, from, to);

                    var attributes = positions.stream()
                            .flatMap((position -> position.getAttributes().keySet().stream()))
                            .collect(Collectors.toUnmodifiableSet());

                    var properties = new LinkedHashMap<String, Function<Position, Object>>();
                    properties.put("id", Position::getId);
                    properties.put("deviceId", Position::getDeviceId);
                    properties.put("protocol", Position::getProtocol);
                    properties.put("serverTime", position -> DateUtil.formatDate(position.getServerTime()));
                    properties.put("deviceTime", position -> DateUtil.formatDate(position.getDeviceTime()));
                    properties.put("fixTime", position -> DateUtil.formatDate(position.getFixTime()));
                    properties.put("valid", Position::getValid);
                    properties.put("latitude", Position::getLatitude);
                    properties.put("longitude", Position::getLongitude);
                    properties.put("altitude", Position::getAltitude);
                    properties.put("speed", Position::getSpeed);
                    properties.put("course", Position::getCourse);
                    properties.put("address", Position::getAddress);
                    properties.put("accuracy", Position::getAccuracy);
                    attributes.forEach(key -> properties.put(key, position -> position.getAttributes().get(key)));

                    try (PrintWriter writer = new PrintWriter(outputStream)) {
                        writer.println(String.join(",", properties.keySet()));
                        positions.forEach(position -> writer.println(properties.values().stream()
                                .map(f -> Objects.toString(f.apply(position), ""))
                                .collect(Collectors.joining(","))));
                    }
                } catch (StorageException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            if (e instanceof StorageException) {
                throw (StorageException) e;
            } else {
                throw new StorageException(e);
            }
        } finally {
            span.end();
        }
    }

}