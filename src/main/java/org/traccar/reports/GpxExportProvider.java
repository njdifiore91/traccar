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
import org.traccar.model.Device;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.Date;

public class GpxExportProvider {

    private final Storage storage;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final ServiceDiscovery serviceDiscovery;

    @Inject
    public GpxExportProvider(Storage storage, ServiceDiscovery serviceDiscovery, Tracer tracer) {
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
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("gpxExportProvider");
    }

    public void generate(
            OutputStream outputStream, long deviceId, Date from, Date to) throws StorageException {

        // Create a span for this operation
        Span span = tracer.spanBuilder("gpx_export_generate")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", deviceId);
            if (from != null) span.setAttribute("fromDate", from.toString());
            if (to != null) span.setAttribute("toDate", to.toString());

            circuitBreaker.executeRunnable(() -> {
                try {
                    var device = storage.getObject(Device.class, new Request(
                            new Columns.All(), new Condition.Equals("id", deviceId)));
                    var positions = PositionUtil.getPositions(storage, deviceId, from, to);

                    try (PrintWriter writer = new PrintWriter(outputStream)) {
                        writer.print("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
                        writer.print("<gpx version=\"1.0\">");
                        writer.print("<trk>");
                        writer.print("<n>");
                        writer.print(device.getName());
                        writer.print("</n>");
                        writer.print("<trkseg>");
                        positions.forEach(position -> {
                            writer.print("<trkpt lat=\"");
                            writer.print(position.getLatitude());
                            writer.print("\" lon=\"");
                            writer.print(position.getLongitude());
                            writer.print("\">");
                            writer.print("<ele>");
                            writer.print(position.getAltitude());
                            writer.print("</ele>");
                            writer.print("<time>");
                            writer.print(DateUtil.formatDate(position.getFixTime()));
                            writer.print("</time>");
                            writer.print("</trkpt>");
                        });
                        writer.print("</trkseg>");
                        writer.print("</trk>");
                        writer.print("</gpx>");
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