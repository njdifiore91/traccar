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
package org.traccar.schedule;

import com.google.inject.Injector;
import com.google.inject.servlet.RequestScoper;
import com.google.inject.servlet.ServletScopes;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import net.fortuna.ical4j.model.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.helper.LogAction;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageSerializer;
import org.traccar.model.BaseModel;
import org.traccar.model.Calendar;
import org.traccar.model.Device;
import org.traccar.model.Group;
import org.traccar.model.Report;
import org.traccar.model.User;
import org.traccar.observability.TracerFactory;
import org.traccar.observability.MeterFactory;
import org.traccar.reports.EventsReportProvider;
import org.traccar.reports.RouteReportProvider;
import org.traccar.reports.StopsReportProvider;
import org.traccar.reports.SummaryReportProvider;
import org.traccar.reports.TripsReportProvider;
import org.traccar.reports.common.ReportMailer;
import org.traccar.reports.common.ReportStorageManager;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class TaskReports extends ServiceDiscoveryScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskReports.class);

    private static final long CHECK_PERIOD_MINUTES = 15;
    private static final String REPORT_TOPIC = "reports";
    private static final String REPORT_GENERATION_SPAN_NAME = "report.generation";
    private static final String REPORT_STORAGE_SPAN_NAME = "report.storage";
    private static final String REPORT_EMAIL_SPAN_NAME = "report.email";

    private final LogAction actionLogger;
    private final Storage storage;
    private final Injector injector;
    private final MessageProducer messageProducer;
    private final MessageSerializer messageSerializer;
    private final ReportStorageManager reportStorageManager;
    private final Tracer tracer;
    private final LongCounter reportGeneratedCounter;
    private final LongCounter reportFailedCounter;

    @Inject
    public TaskReports(
            LogAction actionLogger,
            Storage storage,
            Injector injector,
            ServiceDiscovery serviceDiscovery,
            MessageProducer messageProducer,
            MessageSerializer messageSerializer,
            ReportStorageManager reportStorageManager,
            TracerFactory tracerFactory,
            MeterFactory meterFactory) {
        super("report-scheduler", serviceDiscovery);
        this.actionLogger = actionLogger;
        this.storage = storage;
        this.injector = injector;
        this.messageProducer = messageProducer;
        this.messageSerializer = messageSerializer;
        this.reportStorageManager = reportStorageManager;
        this.tracer = tracerFactory.getTracer(TaskReports.class.getName());
        
        Meter meter = meterFactory.getMeter(TaskReports.class.getName());
        this.reportGeneratedCounter = meter.counterBuilder("reports.generated")
                .setDescription("Number of reports generated")
                .build();
        this.reportFailedCounter = meter.counterBuilder("reports.failed")
                .setDescription("Number of reports that failed to generate")
                .build();
    }

    @Override
    public void schedule(ScheduledExecutorService executor) {
        executor.scheduleAtFixedRate(this, CHECK_PERIOD_MINUTES, CHECK_PERIOD_MINUTES, TimeUnit.MINUTES);
    }

    @Override
    public void run() {
        if (!isLeader()) {
            LOGGER.debug("Not the leader, skipping report generation check");
            return;
        }

        Span span = tracer.spanBuilder("scheduled.reports.check").startSpan();
        try (var scope = span.makeCurrent()) {
            Date currentCheck = new Date();
            Date lastCheck = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(CHECK_PERIOD_MINUTES));
            span.setAttribute("from", lastCheck.getTime());
            span.setAttribute("to", currentCheck.getTime());

            processReports(lastCheck, currentCheck, span);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Scheduled reports error", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }

    private void processReports(Date lastCheck, Date currentCheck, Span parentSpan) throws StorageException {
        List<Report> reports = storage.getObjects(Report.class, new Request(new Columns.All()));
        parentSpan.setAttribute("reports.count", reports.size());

        for (Report report : reports) {
            Calendar calendar = storage.getObject(Calendar.class, new Request(
                    new Columns.All(), new Condition.Equals("id", report.getCalendarId())));

            var lastEvents = calendar.findPeriods(lastCheck);
            var currentEvents = calendar.findPeriods(currentCheck);

            Set<Period<Instant>> finishedEvents = new HashSet<>(lastEvents);
            finishedEvents.removeAll(currentEvents);
            parentSpan.setAttribute("finished_events.count", finishedEvents.size());

            for (Period<Instant> period : finishedEvents) {
                Date from = Date.from(period.getStart());
                Date to = Date.from(period.getEnd());
                
                // Publish report generation event to message broker
                publishReportEvent(report, from, to);
                
                // Also execute locally to maintain backward compatibility
                RequestScoper scope = ServletScopes.scopeRequest(Collections.emptyMap());
                try (RequestScoper.CloseableScope ignored = scope.open()) {
                    executeReport(report, from, to, Context.current());
                }
            }
        }
    }

    private void publishReportEvent(Report report, Date from, Date to) {
        try {
            Map<String, Object> eventData = new HashMap<>();
            eventData.put("reportId", report.getId());
            eventData.put("from", from.getTime());
            eventData.put("to", to.getTime());
            
            byte[] serializedData = messageSerializer.serialize(eventData);
            messageProducer.send(REPORT_TOPIC, serializedData);
            LOGGER.debug("Published report event for report {}, period {} to {}", 
                    report.getId(), from, to);
        } catch (Exception e) {
            LOGGER.warn("Failed to publish report event", e);
        }
    }

    public void executeReport(Report report, Date from, Date to, Context parentContext) throws StorageException {
        Span span = tracer.spanBuilder(REPORT_GENERATION_SPAN_NAME)
                .setParent(parentContext)
                .setAttribute("report.id", report.getId())
                .setAttribute("report.type", report.getType())
                .setAttribute("report.from", from.getTime())
                .setAttribute("report.to", to.getTime())
                .startSpan();

        try (var scope = span.makeCurrent()) {
            var deviceIds = storage.getObjects(Device.class, new Request(
                    new Columns.Include("id"),
                    new Condition.Permission(Device.class, Report.class, report.getId())))
                    .stream().map(BaseModel::getId).collect(Collectors.toList());
            var groupIds = storage.getObjects(Group.class, new Request(
                    new Columns.Include("id"),
                    new Condition.Permission(Group.class, Report.class, report.getId())))
                    .stream().map(BaseModel::getId).collect(Collectors.toList());
            var users = storage.getObjects(User.class, new Request(
                    new Columns.Include("id"),
                    new Condition.Permission(User.class, Report.class, report.getId())));

            span.setAttribute("devices.count", deviceIds.size());
            span.setAttribute("groups.count", groupIds.size());
            span.setAttribute("users.count", users.size());

            ReportMailer reportMailer = injector.getInstance(ReportMailer.class);

            for (User user : users) {
                actionLogger.report(null, user.getId(), true, report.getType(), from, to, deviceIds, groupIds);
                generateAndSendReport(report, user, deviceIds, groupIds, from, to, reportMailer, span);
            }
            
            reportGeneratedCounter.add(1, Attributes.of(
                AttributeKey.stringKey("report.type"), report.getType()));
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Report generation error", e);
            reportFailedCounter.add(1, Attributes.of(
                AttributeKey.stringKey("report.type"), report.getType()));
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }

    private void generateAndSendReport(
            Report report, User user, List<Long> deviceIds, List<Long> groupIds, 
            Date from, Date to, ReportMailer reportMailer, Span parentSpan) {
        
        try {
            ByteArrayOutputStream reportStream = new ByteArrayOutputStream();
            String reportType = report.getType();
            String reportFileName = String.format("%s_%s_%s.xlsx", 
                    reportType, from.getTime(), to.getTime());
            
            switch (reportType) {
                case "events" -> {
                    var eventsReportProvider = injector.getInstance(EventsReportProvider.class);
                    eventsReportProvider.getExcel(reportStream, user.getId(), deviceIds, groupIds, 
                            List.of(), List.of(), from, to);
                }
                case "route" -> {
                    var routeReportProvider = injector.getInstance(RouteReportProvider.class);
                    routeReportProvider.getExcel(reportStream, user.getId(), deviceIds, groupIds, from, to);
                }
                case "summary" -> {
                    var summaryReportProvider = injector.getInstance(SummaryReportProvider.class);
                    summaryReportProvider.getExcel(reportStream, user.getId(), deviceIds, groupIds, from, to, false);
                }
                case "trips" -> {
                    var tripsReportProvider = injector.getInstance(TripsReportProvider.class);
                    tripsReportProvider.getExcel(reportStream, user.getId(), deviceIds, groupIds, from, to);
                }
                case "stops" -> {
                    var stopsReportProvider = injector.getInstance(StopsReportProvider.class);
                    stopsReportProvider.getExcel(reportStream, user.getId(), deviceIds, groupIds, from, to);
                }
                default -> {
                    LOGGER.warn("Unsupported report type {}", reportType);
                    return;
                }
            }
            
            // Store report in object storage
            storeReportInObjectStorage(user.getId(), reportType, reportFileName, reportStream.toByteArray(), parentSpan);
            
            // Send report via email
            sendReportEmail(user.getId(), reportType, reportStream.toByteArray(), parentSpan);
            
        } catch (Exception e) {
            LOGGER.warn("Failed to generate and send report", e);
            parentSpan.recordException(e);
        }
    }
    
    private void storeReportInObjectStorage(long userId, String reportType, String fileName, 
                                          byte[] reportData, Span parentSpan) {
        Span span = tracer.spanBuilder(REPORT_STORAGE_SPAN_NAME)
                .setParent(Context.current())
                .setAttribute("user.id", userId)
                .setAttribute("report.type", reportType)
                .setAttribute("report.size", reportData.length)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            String objectKey = String.format("reports/%d/%s/%s", userId, reportType, fileName);
            reportStorageManager.storeReport(objectKey, reportData);
            span.setAttribute("storage.key", objectKey);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Failed to store report in object storage", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
    
    private void sendReportEmail(long userId, String reportType, byte[] reportData, Span parentSpan) {
        Span span = tracer.spanBuilder(REPORT_EMAIL_SPAN_NAME)
                .setParent(Context.current())
                .setAttribute("user.id", userId)
                .setAttribute("report.type", reportType)
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            ReportMailer reportMailer = injector.getInstance(ReportMailer.class);
            reportMailer.sendReportEmail(userId, reportType, reportData);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            LOGGER.warn("Failed to send report email", e);
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }
}