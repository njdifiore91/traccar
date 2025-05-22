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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.User;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TaskDeleteTemporary extends SingleScheduleTask {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskDeleteTemporary.class);

    private static final long CHECK_PERIOD_HOURS = 1;
    private static final String TASK_NAME = "delete-temporary-users";
    private static final String SPAN_NAME = "deleteTemporaryUsers";
    private static final String TOPIC_NAME = "user-deletion";

    private final Storage storage;
    private final ServiceDiscoveryScheduler serviceDiscoveryScheduler;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final LongCounter deletionCounter;

    @Inject
    public TaskDeleteTemporary(
            Storage storage,
            ServiceDiscovery serviceDiscovery,
            MessageProducer messageProducer,
            Tracer tracer,
            Meter meter) {
        this.storage = storage;
        this.serviceDiscoveryScheduler = new ServiceDiscoveryScheduler(serviceDiscovery, TASK_NAME);
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.deletionCounter = meter.counterBuilder("traccar.temporary_users.deleted")
                .setDescription("Number of temporary users deleted")
                .build();
    }

    @Override
    public void schedule(ScheduledExecutorService executor) {
        // Use ServiceDiscoveryScheduler to ensure only one instance runs this task
        serviceDiscoveryScheduler.scheduleWithLeaderElection(
                executor, this, CHECK_PERIOD_HOURS, CHECK_PERIOD_HOURS, TimeUnit.HOURS);
    }

    @Override
    public void run() {
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder(SPAN_NAME)
                .setParent(Context.current())
                .setAttribute("task.name", TASK_NAME)
                .startSpan();

        try {
            span.addEvent("Starting temporary user deletion check");
            LOGGER.debug("Checking for expired temporary users");

            // Create the query to find expired temporary users
            Request request = new Request(
                    new Condition.And(
                            new Condition.Equals("temporary", true),
                            new Condition.Compare("expirationTime", "<", "time", new Date())));

            // Add the query details to the span for better tracing
            span.setAttribute("query.condition", request.getCondition().toString());

            // Execute the deletion operation
            int deletedCount = storage.removeObject(User.class, request);

            // Record metrics for the deletion operation
            deletionCounter.add(deletedCount);

            // Add the result to the span
            span.setAttribute("users.deleted.count", deletedCount);
            span.addEvent("Temporary user deletion completed");

            // Log the result
            LOGGER.info("Deleted {} expired temporary users", deletedCount);

            // Publish deletion event to message broker if any users were deleted
            if (deletedCount > 0) {
                publishDeletionEvent(deletedCount, span);
            }

            span.setStatus(StatusCode.OK);
        } catch (StorageException e) {
            // Record the error in the span
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            
            LOGGER.warn("Failed to delete temporary users", e);
        } finally {
            span.end();
        }
    }

    private void publishDeletionEvent(int count, Span parentSpan) {
        Span span = tracer.spanBuilder("publishDeletionEvent")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        
        try {
            // Create attributes for the event
            Attributes eventAttributes = Attributes.of(
                    AttributeKey.stringKey("event.type"), "user.deletion",
                    AttributeKey.longKey("users.count"), count,
                    AttributeKey.stringKey("users.type"), "temporary");
            
            span.addEvent("Publishing user deletion event");
            span.setAttribute("topic.name", TOPIC_NAME);
            
            // Publish the event to the message broker
            messageProducer.send(TOPIC_NAME, eventAttributes);
            
            span.addEvent("User deletion event published");
            span.setStatus(StatusCode.OK);
            
            LOGGER.debug("Published deletion event for {} temporary users", count);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            
            LOGGER.warn("Failed to publish temporary user deletion event", e);
        } finally {
            span.end();
        }
    }
}