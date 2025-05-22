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
package org.traccar.schedule;

import jakarta.inject.Inject;
import jakarta.mail.MessagingException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.discovery.ServiceDiscoveryScheduler;
import org.traccar.mail.MailManager;
import org.traccar.messaging.MessageBroker;
import org.traccar.model.Device;
import org.traccar.model.Disableable;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.notification.TextTemplateFormatter;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TaskExpirations extends ServiceDiscoveryScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExpirations.class);

    private static final long CHECK_PERIOD_HOURS = 1;
    private static final String TASK_NAME = "expiration-check";
    private static final int PARTITION_SIZE = 100; // Number of users/devices to process in each batch

    private final Config config;
    private final Storage storage;
    private final TextTemplateFormatter textTemplateFormatter;
    private final MailManager mailManager;
    private final MessageBroker messageBroker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    // Metrics
    private final Timer userExpirationCheckTimer;
    private final Timer deviceExpirationCheckTimer;
    private final Counter userExpirationCounter;
    private final Counter deviceExpirationCounter;
    private final Counter userExpirationReminderCounter;
    private final Counter deviceExpirationReminderCounter;
    private final Counter errorCounter;

    @Inject
    public TaskExpirations(
            Config config, Storage storage, TextTemplateFormatter textTemplateFormatter, 
            MailManager mailManager, MessageBroker messageBroker, Tracer tracer, MeterRegistry meterRegistry) {
        super(TASK_NAME);
        this.config = config;
        this.storage = storage;
        this.textTemplateFormatter = textTemplateFormatter;
        this.mailManager = mailManager;
        this.messageBroker = messageBroker;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Initialize metrics
        userExpirationCheckTimer = Timer.builder("traccar.expirations.user.check")
                .description("Time spent checking user expirations")
                .register(meterRegistry);
        deviceExpirationCheckTimer = Timer.builder("traccar.expirations.device.check")
                .description("Time spent checking device expirations")
                .register(meterRegistry);
        userExpirationCounter = Counter.builder("traccar.expirations.user.expired")
                .description("Number of user expirations detected")
                .register(meterRegistry);
        deviceExpirationCounter = Counter.builder("traccar.expirations.device.expired")
                .description("Number of device expirations detected")
                .register(meterRegistry);
        userExpirationReminderCounter = Counter.builder("traccar.expirations.user.reminder")
                .description("Number of user expiration reminders sent")
                .register(meterRegistry);
        deviceExpirationReminderCounter = Counter.builder("traccar.expirations.device.reminder")
                .description("Number of device expiration reminders sent")
                .register(meterRegistry);
        errorCounter = Counter.builder("traccar.expirations.errors")
                .description("Number of errors during expiration checks")
                .register(meterRegistry);
    }

    @Override
    public void schedule(ScheduledExecutorService executor) {
        executor.scheduleAtFixedRate(this, CHECK_PERIOD_HOURS, CHECK_PERIOD_HOURS, TimeUnit.HOURS);
    }

    private boolean checkTimeTrigger(Disableable disableable, long currentTime, long offsetTime) {
        if (disableable.getExpirationTime() != null) {
            long previousTime = currentTime - TimeUnit.HOURS.toMillis(CHECK_PERIOD_HOURS);
            long expirationTime = disableable.getExpirationTime().getTime() + offsetTime;
            return previousTime < expirationTime && currentTime >= expirationTime;
        }
        return false;
    }

    private void sendUserExpiration(
            Server server, User user, String template) throws MessagingException {
        var velocityContext = textTemplateFormatter.prepareContext(server, user);
        velocityContext.put("expiration", user.getExpirationTime());
        var fullMessage = textTemplateFormatter.formatMessage(velocityContext, template, "full");
        mailManager.sendMessage(user, true, fullMessage.getSubject(), fullMessage.getBody());

        // Publish expiration event to message broker
        messageBroker.publishUserExpirationEvent(user.getId(), template.equals("userExpiration"));
    }

    private void sendDeviceExpiration(
            Server server, Device device, String template) throws MessagingException, StorageException {
        var users = storage.getObjects(User.class, new Request(
                new Columns.All(), new Condition.Permission(User.class, Device.class, device.getId())));
        for (User user : users) {
            var velocityContext = textTemplateFormatter.prepareContext(server, user);
            velocityContext.put("expiration", device.getExpirationTime());
            velocityContext.put("device", device);
            var fullMessage = textTemplateFormatter.formatMessage(velocityContext, template, "full");
            mailManager.sendMessage(user, true, fullMessage.getSubject(), fullMessage.getBody());
        }

        // Publish expiration event to message broker
        messageBroker.publishDeviceExpirationEvent(device.getId(), template.equals("deviceExpiration"));
    }

    @Override
    public void run() {
        // Only run if this instance is the leader
        if (!isLeader()) {
            LOGGER.debug("Not the leader, skipping expiration check");
            return;
        }

        Span span = tracer.spanBuilder("expirations.check").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("task", TASK_NAME);
            
            long currentTime = System.currentTimeMillis();
            Server server = storage.getObject(Server.class, new Request(new Columns.All()));
            span.setAttribute("server.id", server.getId());

            if (config.getBoolean(Keys.NOTIFICATION_EXPIRATION_USER)) {
                checkUserExpirations(server, currentTime, span);
            }

            if (config.getBoolean(Keys.NOTIFICATION_EXPIRATION_DEVICE)) {
                checkDeviceExpirations(server, currentTime, span);
            }

            span.setStatus(StatusCode.OK);
        } catch (StorageException | MessagingException e) {
            LOGGER.warn("Failed to check expirations", e);
            errorCounter.increment();
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
        } finally {
            span.end();
        }
    }

    private void checkUserExpirations(Server server, long currentTime, Span parentSpan) throws StorageException, MessagingException {
        Span span = tracer.spanBuilder("expirations.check.users")
                .setParent(parentSpan.getSpanContext())
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            long reminder = config.getLong(Keys.NOTIFICATION_EXPIRATION_USER_REMINDER);
            span.setAttribute("reminder.time.ms", reminder);

            // Process users in batches to avoid memory issues with large datasets
            int offset = 0;
            List<User> userBatch;
            int totalProcessed = 0;
            int expiredCount = 0;
            int reminderCount = 0;

            do {
                userBatch = userExpirationCheckTimer.record(() -> {
                    try {
                        return storage.getObjects(User.class, new Request(
                                new Columns.All(), null, PARTITION_SIZE, offset));
                    } catch (StorageException e) {
                        throw new RuntimeException(e);
                    }
                });

                for (User user : userBatch) {
                    if (checkTimeTrigger(user, currentTime, 0)) {
                        sendUserExpiration(server, user, "userExpiration");
                        userExpirationCounter.increment();
                        expiredCount++;
                    } else if (reminder > 0 && checkTimeTrigger(user, currentTime, -reminder)) {
                        sendUserExpiration(server, user, "userExpirationReminder");
                        userExpirationReminderCounter.increment();
                        reminderCount++;
                    }
                }

                totalProcessed += userBatch.size();
                offset += PARTITION_SIZE;
            } while (userBatch.size() == PARTITION_SIZE);

            span.setAttribute("users.processed", totalProcessed);
            span.setAttribute("users.expired", expiredCount);
            span.setAttribute("users.reminded", reminderCount);
        } finally {
            span.end();
        }
    }

    private void checkDeviceExpirations(Server server, long currentTime, Span parentSpan) throws StorageException, MessagingException {
        Span span = tracer.spanBuilder("expirations.check.devices")
                .setParent(parentSpan.getSpanContext())
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            long reminder = config.getLong(Keys.NOTIFICATION_EXPIRATION_DEVICE_REMINDER);
            span.setAttribute("reminder.time.ms", reminder);

            // Process devices in batches to avoid memory issues with large datasets
            int offset = 0;
            List<Device> deviceBatch;
            int totalProcessed = 0;
            int expiredCount = 0;
            int reminderCount = 0;

            do {
                deviceBatch = deviceExpirationCheckTimer.record(() -> {
                    try {
                        return storage.getObjects(Device.class, new Request(
                                new Columns.All(), null, PARTITION_SIZE, offset));
                    } catch (StorageException e) {
                        throw new RuntimeException(e);
                    }
                });

                for (Device device : deviceBatch) {
                    if (checkTimeTrigger(device, currentTime, 0)) {
                        sendDeviceExpiration(server, device, "deviceExpiration");
                        deviceExpirationCounter.increment();
                        expiredCount++;
                    } else if (reminder > 0 && checkTimeTrigger(device, currentTime, -reminder)) {
                        sendDeviceExpiration(server, device, "deviceExpirationReminder");
                        deviceExpirationReminderCounter.increment();
                        reminderCount++;
                    }
                }

                totalProcessed += deviceBatch.size();
                offset += PARTITION_SIZE;
            } while (deviceBatch.size() == PARTITION_SIZE);

            span.setAttribute("devices.processed", totalProcessed);
            span.setAttribute("devices.expired", expiredCount);
            span.setAttribute("devices.reminded", reminderCount);
        } finally {
            span.end();
        }
    }
}