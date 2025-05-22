/*
 * Copyright 2015 - 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.api.resource;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import org.traccar.api.BaseResource;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceInstance;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.model.UserRestrictions;
import org.traccar.reports.CsvExportProvider;
import org.traccar.reports.GpxExportProvider;
import org.traccar.reports.KmlExportProvider;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.StreamingOutput;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.LinkedList;
import java.util.function.Supplier;

@Path("positions")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class PositionResource extends BaseResource {

    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscovery serviceDiscovery;

    @Inject
    private KmlExportProvider kmlExportProvider;

    @Inject
    private CsvExportProvider csvExportProvider;

    @Inject
    private GpxExportProvider gpxExportProvider;

    @Inject
    public PositionResource(ServiceDiscovery serviceDiscovery, Tracer tracer, MeterRegistry meterRegistry) {
        this.serviceDiscovery = serviceDiscovery;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Configure circuit breaker for position service calls
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();

        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker("positionService");

        // Register circuit breaker events for monitoring
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    // Log state transitions for monitoring
                    System.out.println("Circuit breaker state changed from " + 
                            event.getStateTransition().getFromState() + " to " + 
                            event.getStateTransition().getToState());
                });
    }

    @GET
    public Collection<Position> getJson(
            @QueryParam("deviceId") long deviceId, @QueryParam("id") List<Long> positionIds,
            @QueryParam("from") Date from, @QueryParam("to") Date to)
            throws StorageException {
        
        // Create a span for this operation
        Span span = tracer.spanBuilder("get_positions")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        // Start a timer for metrics collection
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            // Add attributes to the span for better tracing
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("positionIdsCount", positionIds.size());
            if (from != null) span.setAttribute("fromDate", from.toString());
            if (to != null) span.setAttribute("toDate", to.toString());
            
            return circuitBreaker.executeSupplier(() -> {
                try {
                    if (!positionIds.isEmpty()) {
                        var positions = new ArrayList<Position>();
                        for (long positionId : positionIds) {
                            Position position = storage.getObject(Position.class, new Request(
                                    new Columns.All(), new Condition.Equals("id", positionId)));
                            permissionsService.checkPermission(Device.class, getUserId(), position.getDeviceId());
                            positions.add(position);
                        }
                        return positions;
                    } else if (deviceId > 0) {
                        permissionsService.checkPermission(Device.class, getUserId(), deviceId);
                        if (from != null && to != null) {
                            permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
                            return PositionUtil.getPositions(storage, deviceId, from, to);
                        } else {
                            return storage.getObjects(Position.class, new Request(
                                    new Columns.All(), new Condition.LatestPositions(deviceId)));
                        }
                    } else {
                        return PositionUtil.getLatestPositions(storage, getUserId());
                    }
                } catch (StorageException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            // Record metrics
            sample.stop(Timer.builder("api.positions.get")
                    .description("Time taken to retrieve positions")
                    .tag("deviceId", String.valueOf(deviceId))
                    .tag("hasPositionIds", String.valueOf(!positionIds.isEmpty()))
                    .tag("hasDateRange", String.valueOf(from != null && to != null))
                    .register(meterRegistry));
            
            span.end();
        }
    }

    @Path("{id}")
    @DELETE
    public Response removeById(@PathParam("id") long positionId) throws StorageException {
        // Create a span for this operation
        Span span = tracer.spanBuilder("delete_position_by_id")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        // Start a timer for metrics collection
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("positionId", positionId);
            
            return circuitBreaker.executeSupplier(() -> {
                try {
                    permissionsService.checkRestriction(getUserId(), UserRestrictions::getReadonly);

                    Request request = new Request(new Columns.All(), new Condition.Equals("id", positionId));
                    Position position = storage.getObject(Position.class, request);
                    if (position == null) {
                        return Response.status(Response.Status.NOT_FOUND).build();
                    }

                    permissionsService.checkPermission(Device.class, getUserId(), position.getDeviceId());

                    storage.removeObject(Position.class, request);
                    return Response.status(Response.Status.NO_CONTENT).build();
                } catch (StorageException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            // Record metrics
            sample.stop(Timer.builder("api.positions.delete.byId")
                    .description("Time taken to delete a position by ID")
                    .tag("positionId", String.valueOf(positionId))
                    .register(meterRegistry));
            
            span.end();
        }
    }

    @DELETE
    public Response remove(
            @QueryParam("deviceId") long deviceId,
            @QueryParam("from") Date from, @QueryParam("to") Date to) throws StorageException {
        
        // Create a span for this operation
        Span span = tracer.spanBuilder("delete_positions_by_range")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        // Start a timer for metrics collection
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", deviceId);
            if (from != null) span.setAttribute("fromDate", from.toString());
            if (to != null) span.setAttribute("toDate", to.toString());
            
            return circuitBreaker.executeSupplier(() -> {
                try {
                    permissionsService.checkPermission(Device.class, getUserId(), deviceId);
                    permissionsService.checkRestriction(getUserId(), UserRestrictions::getReadonly);

                    var conditions = new LinkedList<Condition>();
                    conditions.add(new Condition.Equals("deviceId", deviceId));
                    conditions.add(new Condition.Between("fixTime", "from", from, "to", to));
                    storage.removeObject(Position.class, new Request(Condition.merge(conditions)));

                    return Response.status(Response.Status.NO_CONTENT).build();
                } catch (StorageException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            // Record metrics
            sample.stop(Timer.builder("api.positions.delete.byRange")
                    .description("Time taken to delete positions by date range")
                    .tag("deviceId", String.valueOf(deviceId))
                    .register(meterRegistry));
            
            span.end();
        }
    }

    @Path("kml")
    @GET
    @Produces("application/vnd.google-earth.kml+xml")
    public Response getKml(
            @QueryParam("deviceId") long deviceId,
            @QueryParam("from") Date from, @QueryParam("to") Date to) throws StorageException {
        
        // Create a span for this operation
        Span span = tracer.spanBuilder("export_positions_kml")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        // Start a timer for metrics collection
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("format", "kml");
            if (from != null) span.setAttribute("fromDate", from.toString());
            if (to != null) span.setAttribute("toDate", to.toString());
            
            return circuitBreaker.executeSupplier(() -> {
                try {
                    permissionsService.checkPermission(Device.class, getUserId(), deviceId);
                    StreamingOutput stream = output -> {
                        try {
                            kmlExportProvider.generate(output, deviceId, from, to);
                        } catch (StorageException e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw new WebApplicationException(e);
                        }
                    };
                    return Response.ok(stream)
                            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=positions.kml").build();
                } catch (StorageException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            // Record metrics
            sample.stop(Timer.builder("api.positions.export.kml")
                    .description("Time taken to export positions as KML")
                    .tag("deviceId", String.valueOf(deviceId))
                    .register(meterRegistry));
            
            span.end();
        }
    }

    @Path("csv")
    @GET
    @Produces("text/csv")
    public Response getCsv(
            @QueryParam("deviceId") long deviceId,
            @QueryParam("from") Date from, @QueryParam("to") Date to) throws StorageException {
        
        // Create a span for this operation
        Span span = tracer.spanBuilder("export_positions_csv")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        // Start a timer for metrics collection
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("format", "csv");
            if (from != null) span.setAttribute("fromDate", from.toString());
            if (to != null) span.setAttribute("toDate", to.toString());
            
            return circuitBreaker.executeSupplier(() -> {
                try {
                    permissionsService.checkPermission(Device.class, getUserId(), deviceId);
                    StreamingOutput stream = output -> {
                        try {
                            csvExportProvider.generate(output, deviceId, from, to);
                        } catch (StorageException e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw new WebApplicationException(e);
                        }
                    };
                    return Response.ok(stream)
                            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=positions.csv").build();
                } catch (StorageException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            // Record metrics
            sample.stop(Timer.builder("api.positions.export.csv")
                    .description("Time taken to export positions as CSV")
                    .tag("deviceId", String.valueOf(deviceId))
                    .register(meterRegistry));
            
            span.end();
        }
    }

    @Path("gpx")
    @GET
    @Produces("application/gpx+xml")
    public Response getGpx(
            @QueryParam("deviceId") long deviceId,
            @QueryParam("from") Date from, @QueryParam("to") Date to) throws StorageException {
        
        // Create a span for this operation
        Span span = tracer.spanBuilder("export_positions_gpx")
                .setSpanKind(SpanKind.SERVER)
                .startSpan();
        
        // Start a timer for metrics collection
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("deviceId", deviceId);
            span.setAttribute("format", "gpx");
            if (from != null) span.setAttribute("fromDate", from.toString());
            if (to != null) span.setAttribute("toDate", to.toString());
            
            return circuitBreaker.executeSupplier(() -> {
                try {
                    permissionsService.checkPermission(Device.class, getUserId(), deviceId);
                    StreamingOutput stream = output -> {
                        try {
                            gpxExportProvider.generate(output, deviceId, from, to);
                        } catch (StorageException e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw new WebApplicationException(e);
                        }
                    };
                    return Response.ok(stream)
                            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=positions.gpx").build();
                } catch (StorageException e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    throw e;
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            // Record metrics
            sample.stop(Timer.builder("api.positions.export.gpx")
                    .description("Time taken to export positions as GPX")
                    .tag("deviceId", String.valueOf(deviceId))
                    .register(meterRegistry));
            
            span.end();
        }
    }

}