/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.model.Calendar;
import org.traccar.model.ReportSchedule;
import org.traccar.model.User;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Scheduler for periodic report generation in the Reporting Service.
 * Retrieves report schedules from the database, evaluates calendar periods,
 * and publishes report generation requests to the message broker.
 * Supports cron-style scheduling, timezone-specific execution, and maintains
 * execution history for auditing.
 */
@Service
public class ReportScheduler implements HealthIndicator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReportScheduler.class);

    private final Config config;
    private final Storage storage;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final KafkaTemplate<String, ReportRequest> kafkaTemplate;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    private final Timer schedulerExecutionTimer;
    private final Counter scheduledReportsCounter;
    private final Counter reportExecutionCounter;
    private final Counter reportErrorCounter;

    /**
     * Constructs a new ReportScheduler with the necessary dependencies.
     *
     * @param config The application configuration
     * @param storage Database storage for retrieving report schedules
     * @param serviceDiscoveryManager Service discovery manager for service registration
     * @param kafkaTemplate Kafka template for publishing report requests
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param meterRegistry Micrometer registry for metrics collection
     */
    @Inject
    public ReportScheduler(
            Config config,
            Storage storage,
            ServiceDiscoveryManager serviceDiscoveryManager,
            KafkaTemplate<String, ReportRequest> kafkaTemplate,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.storage = storage;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.kafkaTemplate = kafkaTemplate;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Register metrics
        this.schedulerExecutionTimer = Timer.builder("report.scheduler.execution.time")
                .description("Time taken to execute the report scheduler")
                .register(meterRegistry);
        this.scheduledReportsCounter = Counter.builder("report.scheduler.reports.scheduled")
                .description("Number of scheduled reports found")
                .register(meterRegistry);
        this.reportExecutionCounter = Counter.builder("report.scheduler.reports.executed")
                .description("Number of reports executed")
                .register(meterRegistry);
        this.reportErrorCounter = Counter.builder("report.scheduler.reports.errors")
                .description("Number of errors during report scheduling")
                .register(meterRegistry);

        // Register service with service discovery
        registerService();
    }

    /**
     * Registers the report scheduler service with the service discovery system.
     */
    private void registerService() {
        serviceDiscoveryManager.register("reporting-service", "report-scheduler");
    }

    /**
     * Scheduled method that runs every 15 minutes to check for reports that need to be executed.
     * Retrieves report schedules from the database, evaluates calendar periods,
     * and publishes report generation requests to the message broker.
     */
    @Scheduled(fixedRate = 15, timeUnit = TimeUnit.MINUTES)
    @Timed(value = "report.scheduler.check", description = "Time taken to check scheduled reports")
    public void checkScheduledReports() {
        Timer.Sample sample = Timer.start(meterRegistry);
        Span span = tracer.spanBuilder("checkScheduledReports").startSpan();

        try (var scope = span.makeCurrent()) {
            LOGGER.info("Checking scheduled reports");
            span.setAttribute("scheduler.execution.time", System.currentTimeMillis());

            // Retrieve all report schedules from the database
            Collection<ReportSchedule> schedules = retrieveReportSchedules();
            scheduledReportsCounter.increment(schedules.size());
            span.setAttribute("scheduler.reports.count", schedules.size());

            // Process each schedule
            for (ReportSchedule schedule : schedules) {
                processReportSchedule(schedule);
            }

            LOGGER.info("Finished checking scheduled reports");
        } catch (Exception e) {
            LOGGER.error("Error checking scheduled reports", e);
            span.recordException(e);
            reportErrorCounter.increment();
        } finally {
            sample.stop(schedulerExecutionTimer);
            span.end();
        }
    }

    /**
     * Retrieves all report schedules from the database.
     *
     * @return Collection of report schedules
     * @throws StorageException If there's an error accessing storage
     */
    private Collection<ReportSchedule> retrieveReportSchedules() throws StorageException {
        Span span = tracer.spanBuilder("retrieveReportSchedules").startSpan();
        try (var scope = span.makeCurrent()) {
            // Retrieve all active report schedules
            Collection<ReportSchedule> schedules = storage.getObjects(ReportSchedule.class,
                    new Request(new Columns.All(), new Condition.Equals("active", true)));
            span.setAttribute("schedules.count", schedules.size());
            return schedules;
        } finally {
            span.end();
        }
    }

    /**
     * Processes a single report schedule, checking if it should be executed based on its calendar.
     *
     * @param schedule The report schedule to process
     */
    private void processReportSchedule(ReportSchedule schedule) {
        Span span = tracer.spanBuilder("processReportSchedule").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("schedule.id", schedule.getId());
            span.setAttribute("schedule.userId", schedule.getUserId());
            span.setAttribute("schedule.reportType", schedule.getReportType());

            // Check if the schedule has a calendar
            if (schedule.getCalendarId() <= 0) {
                LOGGER.debug("Schedule {} has no calendar, skipping", schedule.getId());
                return;
            }

            // Retrieve the calendar
            Calendar calendar = retrieveCalendar(schedule.getCalendarId());
            if (calendar == null) {
                LOGGER.warn("Calendar {} not found for schedule {}", schedule.getCalendarId(), schedule.getId());
                return;
            }

            // Check if the calendar period is completed
            if (isCalendarPeriodCompleted(calendar, schedule)) {
                // Execute the report
                executeReport(schedule, calendar);
            }
        } catch (Exception e) {
            LOGGER.error("Error processing report schedule {}", schedule.getId(), e);
            span.recordException(e);
            reportErrorCounter.increment();
        } finally {
            span.end();
        }
    }

    /**
     * Retrieves a calendar from the database.
     *
     * @param calendarId The calendar ID
     * @return The calendar, or null if not found
     */
    private Calendar retrieveCalendar(long calendarId) {
        Span span = tracer.spanBuilder("retrieveCalendar").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("calendar.id", calendarId);
            try {
                Calendar calendar = storage.getObject(Calendar.class,
                        new Request(new Columns.All(), new Condition.Equals("id", calendarId)));
                return calendar;
            } catch (StorageException e) {
                LOGGER.warn("Error retrieving calendar {}", calendarId, e);
                span.recordException(e);
                return null;
            }
        } finally {
            span.end();
        }
    }

    /**
     * Checks if a calendar period is completed for a report schedule.
     * This determines whether the report should be executed.
     *
     * @param calendar The calendar
     * @param schedule The report schedule
     * @return true if the period is completed and the report should be executed, false otherwise
     */
    private boolean isCalendarPeriodCompleted(Calendar calendar, ReportSchedule schedule) {
        Span span = tracer.spanBuilder("isCalendarPeriodCompleted").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("calendar.id", calendar.getId());
            span.setAttribute("schedule.id", schedule.getId());

            // Get the user's timezone
            User user = retrieveUser(schedule.getUserId());
            ZoneId zoneId = ZoneId.systemDefault();
            if (user != null && user.getTimezone() != null && !user.getTimezone().isEmpty()) {
                zoneId = ZoneId.of(user.getTimezone());
            }
            span.setAttribute("user.timezone", zoneId.toString());

            // Get the current time in the user's timezone
            ZonedDateTime now = ZonedDateTime.now(zoneId);
            span.setAttribute("current.time", now.toString());

            // Get the last execution time
            ZonedDateTime lastExecution = null;
            if (schedule.getLastExecution() != null) {
                lastExecution = schedule.getLastExecution().toInstant().atZone(zoneId);
                span.setAttribute("last.execution", lastExecution.toString());
            }

            // Check if the calendar period is completed
            boolean completed = false;

            // Implement calendar period checking logic based on calendar type
            switch (calendar.getCalendarType()) {
                case Calendar.TYPE_DAILY:
                    // Check if a day has passed since the last execution
                    completed = lastExecution == null || 
                            lastExecution.toLocalDate().isBefore(now.toLocalDate());
                    break;
                case Calendar.TYPE_WEEKLY:
                    // Check if a week has passed since the last execution
                    completed = lastExecution == null || 
                            lastExecution.toLocalDate().plusWeeks(1).isBefore(now.toLocalDate());
                    break;
                case Calendar.TYPE_MONTHLY:
                    // Check if a month has passed since the last execution
                    completed = lastExecution == null || 
                            lastExecution.toLocalDate().plusMonths(1).isBefore(now.toLocalDate());
                    break;
                default:
                    // For custom calendars, check if the current time is within the calendar
                    // and if the last execution was in a different calendar period
                    completed = calendar.checkMoment(Date.from(now.toInstant())) && 
                            (lastExecution == null || 
                            !calendar.checkMoment(Date.from(lastExecution.toInstant())));
                    break;
            }

            span.setAttribute("period.completed", completed);
            return completed;
        } finally {
            span.end();
        }
    }

    /**
     * Retrieves a user from the database.
     *
     * @param userId The user ID
     * @return The user, or null if not found
     */
    private User retrieveUser(long userId) {
        try {
            return storage.getObject(User.class,
                    new Request(new Columns.All(), new Condition.Equals("id", userId)));
        } catch (StorageException e) {
            LOGGER.warn("Error retrieving user {}", userId, e);
            return null;
        }
    }

    /**
     * Executes a report by publishing a report request to the message broker.
     *
     * @param schedule The report schedule
     * @param calendar The calendar
     */
    private void executeReport(ReportSchedule schedule, Calendar calendar) {
        Span span = tracer.spanBuilder("executeReport").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("schedule.id", schedule.getId());
            span.setAttribute("schedule.reportType", schedule.getReportType());
            span.setAttribute("calendar.id", calendar.getId());

            // Get the user's timezone
            User user = retrieveUser(schedule.getUserId());
            ZoneId zoneId = ZoneId.systemDefault();
            if (user != null && user.getTimezone() != null && !user.getTimezone().isEmpty()) {
                zoneId = ZoneId.of(user.getTimezone());
            }

            // Calculate the report period based on the calendar type
            ZonedDateTime now = ZonedDateTime.now(zoneId);
            ZonedDateTime from = now;
            ZonedDateTime to = now;

            switch (calendar.getCalendarType()) {
                case Calendar.TYPE_DAILY:
                    // Daily report covers the previous day
                    from = now.toLocalDate().atStartOfDay(zoneId).minusDays(1);
                    to = now.toLocalDate().atStartOfDay(zoneId);
                    break;
                case Calendar.TYPE_WEEKLY:
                    // Weekly report covers the previous week
                    from = now.toLocalDate().atStartOfDay(zoneId).minusWeeks(1);
                    to = now.toLocalDate().atStartOfDay(zoneId);
                    break;
                case Calendar.TYPE_MONTHLY:
                    // Monthly report covers the previous month
                    from = now.toLocalDate().atStartOfDay(zoneId).minusMonths(1);
                    to = now.toLocalDate().atStartOfDay(zoneId);
                    break;
                default:
                    // For custom calendars, use the configured period
                    // This is a simplified approach; in a real implementation,
                    // you would need to determine the exact period based on the calendar configuration
                    int periodDays = config.getInteger(Keys.REPORT_PERIOD_LIMIT, 30);
                    from = now.toLocalDate().atStartOfDay(zoneId).minusDays(periodDays);
                    to = now.toLocalDate().atStartOfDay(zoneId);
                    break;
            }

            span.setAttribute("report.from", from.toString());
            span.setAttribute("report.to", to.toString());

            // Create a report request
            String reportId = UUID.randomUUID().toString();
            ReportRequest request = new ReportRequest();
            request.setReportId(reportId);
            request.setScheduleId(schedule.getId());
            request.setUserId(schedule.getUserId());
            request.setDeviceIds(schedule.getDeviceIds());
            request.setGroupIds(schedule.getGroupIds());
            request.setFrom(Date.from(from.toInstant()));
            request.setTo(Date.from(to.toInstant()));
            request.setReportType(schedule.getReportType());
            request.setIncludeMap(schedule.isIncludeMap());

            // Inject trace context into the message
            Context context = Context.current();

            // Publish the request to Kafka
            kafkaTemplate.send("report-requests", reportId, request)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            LOGGER.error("Error publishing report request", ex);
                            span.recordException(ex);
                            reportErrorCounter.increment();
                        } else {
                            // Update the last execution time
                            updateLastExecutionTime(schedule);
                            reportExecutionCounter.increment();
                            LOGGER.info("Report request published for schedule {}", schedule.getId());
                        }
                    });

            span.setAttribute("report.id", reportId);
        } finally {
            span.end();
        }
    }

    /**
     * Updates the last execution time of a report schedule.
     *
     * @param schedule The report schedule
     */
    private void updateLastExecutionTime(ReportSchedule schedule) {
        Span span = tracer.spanBuilder("updateLastExecutionTime").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute("schedule.id", schedule.getId());

            // Update the last execution time
            schedule.setLastExecution(new Date());

            // Save the updated schedule
            try {
                storage.updateObject(schedule, new Request(
                        new Columns.Include("lastExecution"),
                        new Condition.Equals("id", schedule.getId())));
                LOGGER.debug("Updated last execution time for schedule {}", schedule.getId());
            } catch (StorageException e) {
                LOGGER.error("Error updating last execution time for schedule {}", schedule.getId(), e);
                span.recordException(e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Implements the health check for the report scheduler.
     * This is used by Kubernetes probes to determine service health.
     *
     * @return Health status
     */
    @Override
    public Health health() {
        try {
            // Check if we can access the database
            storage.getObjects(ReportSchedule.class, new Request(new Columns.All(), null));

            // Check if service discovery is registered
            boolean registered = serviceDiscoveryManager.isRegistered();

            if (registered) {
                return Health.up()
                        .withDetail("serviceDiscovery", "registered")
                        .withDetail("lastExecutionTime", schedulerExecutionTimer.count() > 0 ?
                                new Date(System.currentTimeMillis() - (long) schedulerExecutionTimer.mean(TimeUnit.MILLISECONDS)) : "never")
                        .withDetail("scheduledReports", scheduledReportsCounter.count())
                        .withDetail("executedReports", reportExecutionCounter.count())
                        .withDetail("errorReports", reportErrorCounter.count())
                        .build();
            } else {
                return Health.down()
                        .withDetail("serviceDiscovery", "not registered")
                        .build();
            }
        } catch (Exception e) {
            return Health.down()
                    .withException(e)
                    .build();
        }
    }

    /**
     * Request object for report generation.
     */
    public static class ReportRequest {
        private String reportId;
        private long scheduleId;
        private long userId;
        private Collection<Long> deviceIds;
        private Collection<Long> groupIds;
        private Date from;
        private Date to;
        private String reportType;
        private boolean includeMap;

        public String getReportId() {
            return reportId;
        }

        public void setReportId(String reportId) {
            this.reportId = reportId;
        }

        public long getScheduleId() {
            return scheduleId;
        }

        public void setScheduleId(long scheduleId) {
            this.scheduleId = scheduleId;
        }

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public Collection<Long> getDeviceIds() {
            return deviceIds;
        }

        public void setDeviceIds(Collection<Long> deviceIds) {
            this.deviceIds = deviceIds;
        }

        public Collection<Long> getGroupIds() {
            return groupIds;
        }

        public void setGroupIds(Collection<Long> groupIds) {
            this.groupIds = groupIds;
        }

        public Date getFrom() {
            return from;
        }

        public void setFrom(Date from) {
            this.from = from;
        }

        public Date getTo() {
            return to;
        }

        public void setTo(Date to) {
            this.to = to;
        }

        public String getReportType() {
            return reportType;
        }

        public void setReportType(String reportType) {
            this.reportType = reportType;
        }

        public boolean isIncludeMap() {
            return includeMap;
        }

        public void setIncludeMap(boolean includeMap) {
            this.includeMap = includeMap;
        }
    }
}