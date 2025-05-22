/*
 * Copyright 2016 - 2023 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Context;
import org.traccar.api.SimpleObjectResource;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.LogAction;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.Report;
import org.traccar.model.UserRestrictions;
import org.traccar.reports.CombinedReportProvider;
import org.traccar.reports.DevicesReportProvider;
import org.traccar.reports.EventsReportProvider;
import org.traccar.reports.RouteReportProvider;
import org.traccar.reports.StopsReportProvider;
import org.traccar.reports.SummaryReportProvider;
import org.traccar.reports.TripsReportProvider;
import org.traccar.reports.common.ReportExecutor;
import org.traccar.reports.common.ReportMailer;
import org.traccar.reports.model.CombinedReportItem;
import org.traccar.reports.model.StopReportItem;
import org.traccar.reports.model.SummaryReportItem;
import org.traccar.reports.model.TripReportItem;
import org.traccar.storage.StorageException;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
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
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.function.Supplier;

@Path("reports")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ReportResource extends SimpleObjectResource<Report> {

    private static final String EXCEL = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    private static final String REPORTING_SERVICE = "reporting-service";
    private static final String CIRCUIT_BREAKER_NAME = "reportingServiceCircuitBreaker";

    @Inject
    private CombinedReportProvider combinedReportProvider;

    @Inject
    private EventsReportProvider eventsReportProvider;

    @Inject
    private RouteReportProvider routeReportProvider;

    @Inject
    private StopsReportProvider stopsReportProvider;

    @Inject
    private SummaryReportProvider summaryReportProvider;

    @Inject
    private TripsReportProvider tripsReportProvider;

    @Inject
    private DevicesReportProvider devicesReportProvider;

    @Inject
    private ReportMailer reportMailer;

    @Inject
    private LogAction actionLogger;

    @Inject
    private ServiceDiscovery serviceDiscovery;

    @Inject
    private MeterRegistry meterRegistry;

    private final Tracer tracer;
    private final CircuitBreaker circuitBreaker;

    @Context
    private HttpServletRequest request;

    public ReportResource() {
        super(Report.class, "description");
        
        // Initialize OpenTelemetry tracer
        tracer = GlobalOpenTelemetry.getTracer("org.traccar.api.resource.ReportResource");
        
        // Configure and create circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
    }

    private Response executeReport(long userId, boolean mail, ReportExecutor executor) {
        // Create a span for the report execution
        Span span = tracer.spanBuilder("executeReport")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("userId", userId)
                .setAttribute("mail", mail)
                .startSpan();
        
        // Create a timer for metrics collection
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            // Check if Reporting Service is available via service discovery
            if (!serviceDiscovery.isServiceAvailable(REPORTING_SERVICE)) {
                span.setStatus(StatusCode.ERROR, "Reporting service unavailable");
                span.end();
                throw new WebApplicationException("Reporting service unavailable", Response.Status.SERVICE_UNAVAILABLE);
            }
            
            // Execute the report with circuit breaker pattern
            return circuitBreaker.executeSupplier(() -> {
                if (mail) {
                    reportMailer.sendAsync(userId, executor);
                    return Response.noContent().build();
                } else {
                    StreamingOutput stream = output -> {
                        try {
                            executor.execute(output);
                        } catch (StorageException e) {
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw new WebApplicationException(e);
                        }
                    };
                    return Response.ok(stream)
                            .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=report.xlsx").build();
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            
            // Record metrics for failed report execution
            sample.stop(meterRegistry.timer("report.execution", "status", "error"));
            
            if (e instanceof WebApplicationException) {
                throw (WebApplicationException) e;
            } else {
                throw new WebApplicationException("Error executing report: " + e.getMessage(), e, 
                        Response.Status.INTERNAL_SERVER_ERROR);
            }
        } finally {
            if (span != null && !span.isRecording()) {
                span.end();
            }
            
            // Record metrics for successful report execution
            sample.stop(meterRegistry.timer("report.execution", "status", "success"));
        }
    }

    @Path("combined")
    @GET
    public Collection<CombinedReportItem> getCombined(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        actionLogger.report(request, getUserId(), false, "combined", from, to, deviceIds, groupIds);
        
        Span span = tracer.spanBuilder("getCombined")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("userId", getUserId())
                .setAttribute("reportType", "combined")
                .startSpan();
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            return executeWithCircuitBreaker(() -> 
                combinedReportProvider.getObjects(getUserId(), deviceIds, groupIds, from, to),
                "getCombined");
        } finally {
            span.end();
            sample.stop(meterRegistry.timer("report.combined"));
        }
    }

    @Path("route")
    @GET
    public Collection<Position> getRoute(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        actionLogger.report(request, getUserId(), false, "route", from, to, deviceIds, groupIds);
        
        Span span = tracer.spanBuilder("getRoute")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("userId", getUserId())
                .setAttribute("reportType", "route")
                .startSpan();
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            return executeWithCircuitBreaker(() -> 
                routeReportProvider.getObjects(getUserId(), deviceIds, groupIds, from, to),
                "getRoute");
        } finally {
            span.end();
            sample.stop(meterRegistry.timer("report.route"));
        }
    }

    @Path("route")
    @GET
    @Produces(EXCEL)
    public Response getRouteExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @QueryParam("mail") boolean mail) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        return executeReport(getUserId(), mail, stream -> {
            actionLogger.report(request, getUserId(), false, "route", from, to, deviceIds, groupIds);
            routeReportProvider.getExcel(stream, getUserId(), deviceIds, groupIds, from, to);
        });
    }

    @Path("route/{type:xlsx|mail}")
    @GET
    @Produces(EXCEL)
    public Response getRouteExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") final List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @PathParam("type") String type) throws StorageException {
        return getRouteExcel(deviceIds, groupIds, from, to, type.equals("mail"));
    }

    @Path("events")
    @GET
    public Collection<Event> getEvents(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("type") List<String> types,
            @QueryParam("alarm") List<String> alarms,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        actionLogger.report(request, getUserId(), false, "events", from, to, deviceIds, groupIds);
        
        Span span = tracer.spanBuilder("getEvents")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("userId", getUserId())
                .setAttribute("reportType", "events")
                .startSpan();
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            return executeWithCircuitBreaker(() -> 
                eventsReportProvider.getObjects(getUserId(), deviceIds, groupIds, types, alarms, from, to),
                "getEvents");
        } finally {
            span.end();
            sample.stop(meterRegistry.timer("report.events"));
        }
    }

    @Path("events")
    @GET
    @Produces(EXCEL)
    public Response getEventsExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("type") List<String> types,
            @QueryParam("alarm") List<String> alarms,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @QueryParam("mail") boolean mail) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        return executeReport(getUserId(), mail, stream -> {
            actionLogger.report(request, getUserId(), false, "events", from, to, deviceIds, groupIds);
            eventsReportProvider.getExcel(stream, getUserId(), deviceIds, groupIds, types, alarms, from, to);
        });
    }

    @Path("events/{type:xlsx|mail}")
    @GET
    @Produces(EXCEL)
    public Response getEventsExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("type") List<String> types,
            @QueryParam("alarm") List<String> alarms,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @PathParam("type") String type) throws StorageException {
        return getEventsExcel(deviceIds, groupIds, types, alarms, from, to, type.equals("mail"));
    }

    @Path("summary")
    @GET
    public Collection<SummaryReportItem> getSummary(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @QueryParam("daily") boolean daily) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        actionLogger.report(request, getUserId(), false, "summary", from, to, deviceIds, groupIds);
        
        Span span = tracer.spanBuilder("getSummary")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("userId", getUserId())
                .setAttribute("reportType", "summary")
                .setAttribute("daily", daily)
                .startSpan();
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            return executeWithCircuitBreaker(() -> 
                summaryReportProvider.getObjects(getUserId(), deviceIds, groupIds, from, to, daily),
                "getSummary");
        } finally {
            span.end();
            sample.stop(meterRegistry.timer("report.summary"));
        }
    }

    @Path("summary")
    @GET
    @Produces(EXCEL)
    public Response getSummaryExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @QueryParam("daily") boolean daily,
            @QueryParam("mail") boolean mail) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        return executeReport(getUserId(), mail, stream -> {
            actionLogger.report(request, getUserId(), false, "summary", from, to, deviceIds, groupIds);
            summaryReportProvider.getExcel(stream, getUserId(), deviceIds, groupIds, from, to, daily);
        });
    }

    @Path("summary/{type:xlsx|mail}")
    @GET
    @Produces(EXCEL)
    public Response getSummaryExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @QueryParam("daily") boolean daily,
            @PathParam("type") String type) throws StorageException {
        return getSummaryExcel(deviceIds, groupIds, from, to, daily, type.equals("mail"));
    }

    @Path("trips")
    @GET
    public Collection<TripReportItem> getTrips(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        actionLogger.report(request, getUserId(), false, "trips", from, to, deviceIds, groupIds);
        
        Span span = tracer.spanBuilder("getTrips")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("userId", getUserId())
                .setAttribute("reportType", "trips")
                .startSpan();
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            return executeWithCircuitBreaker(() -> 
                tripsReportProvider.getObjects(getUserId(), deviceIds, groupIds, from, to),
                "getTrips");
        } finally {
            span.end();
            sample.stop(meterRegistry.timer("report.trips"));
        }
    }

    @Path("trips")
    @GET
    @Produces(EXCEL)
    public Response getTripsExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @QueryParam("mail") boolean mail) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        return executeReport(getUserId(), mail, stream -> {
            actionLogger.report(request, getUserId(), false, "trips", from, to, deviceIds, groupIds);
            tripsReportProvider.getExcel(stream, getUserId(), deviceIds, groupIds, from, to);
        });
    }

    @Path("trips/{type:xlsx|mail}")
    @GET
    @Produces(EXCEL)
    public Response getTripsExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @PathParam("type") String type) throws StorageException {
        return getTripsExcel(deviceIds, groupIds, from, to, type.equals("mail"));
    }

    @Path("stops")
    @GET
    public Collection<StopReportItem> getStops(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        actionLogger.report(request, getUserId(), false, "stops", from, to, deviceIds, groupIds);
        
        Span span = tracer.spanBuilder("getStops")
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("userId", getUserId())
                .setAttribute("reportType", "stops")
                .startSpan();
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try (Scope scope = span.makeCurrent()) {
            return executeWithCircuitBreaker(() -> 
                stopsReportProvider.getObjects(getUserId(), deviceIds, groupIds, from, to),
                "getStops");
        } finally {
            span.end();
            sample.stop(meterRegistry.timer("report.stops"));
        }
    }

    @Path("stops")
    @GET
    @Produces(EXCEL)
    public Response getStopsExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @QueryParam("mail") boolean mail) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        return executeReport(getUserId(), mail, stream -> {
            actionLogger.report(request, getUserId(), false, "stops", from, to, deviceIds, groupIds);
            stopsReportProvider.getExcel(stream, getUserId(), deviceIds, groupIds, from, to);
        });
    }

    @Path("stops/{type:xlsx|mail}")
    @GET
    @Produces(EXCEL)
    public Response getStopsExcel(
            @QueryParam("deviceId") List<Long> deviceIds,
            @QueryParam("groupId") List<Long> groupIds,
            @QueryParam("from") Date from,
            @QueryParam("to") Date to,
            @PathParam("type") String type) throws StorageException {
        return getStopsExcel(deviceIds, groupIds, from, to, type.equals("mail"));
    }

    @Path("devices/{type:xlsx|mail}")
    @GET
    @Produces(EXCEL)
    public Response getDevicesExcel(
            @PathParam("type") String type) throws StorageException {
        permissionsService.checkRestriction(getUserId(), UserRestrictions::getDisableReports);
        return executeReport(getUserId(), type.equals("mail"), stream -> {
            devicesReportProvider.getExcel(stream, getUserId());
        });
    }
    
    /**
     * Helper method to execute a supplier with circuit breaker pattern
     * 
     * @param supplier The supplier to execute
     * @param operationName The name of the operation for metrics and tracing
     * @return The result of the supplier
     * @param <T> The return type of the supplier
     */
    private <T> T executeWithCircuitBreaker(Supplier<T> supplier, String operationName) {
        Span span = tracer.spanBuilder("circuitBreaker." + operationName)
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("circuitBreaker.name", CIRCUIT_BREAKER_NAME)
                .setAttribute("circuitBreaker.state", circuitBreaker.getState().name())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Check if Reporting Service is available via service discovery
            if (!serviceDiscovery.isServiceAvailable(REPORTING_SERVICE)) {
                span.setStatus(StatusCode.ERROR, "Reporting service unavailable");
                throw new WebApplicationException("Reporting service unavailable", Response.Status.SERVICE_UNAVAILABLE);
            }
            
            return circuitBreaker.executeSupplier(() -> {
                try {
                    return supplier.get();
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    if (e instanceof WebApplicationException) {
                        throw (WebApplicationException) e;
                    } else {
                        throw new WebApplicationException("Error executing operation: " + e.getMessage(), e, 
                                Response.Status.INTERNAL_SERVER_ERROR);
                    }
                }
            });
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            
            // If circuit is open, provide a fallback response
            if (circuitBreaker.getState() == CircuitBreaker.State.OPEN) {
                meterRegistry.counter("circuitbreaker.fallback", "operation", operationName).increment();
                throw new WebApplicationException("Service temporarily unavailable, please try again later", 
                        Response.Status.SERVICE_UNAVAILABLE);
            }
            
            if (e instanceof WebApplicationException) {
                throw (WebApplicationException) e;
            } else {
                throw new WebApplicationException("Error executing operation: " + e.getMessage(), e, 
                        Response.Status.INTERNAL_SERVER_ERROR);
            }
        } finally {
            span.end();
        }
    }
}