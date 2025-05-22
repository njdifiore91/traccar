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
package org.traccar.reports.common;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import jakarta.activation.DataHandler;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.security.PermissionsService;
import org.traccar.mail.MailManager;
import org.traccar.model.User;
import org.traccar.observability.TracerFactory;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.mail.util.ByteArrayDataSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@Singleton
public class ReportMailer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReportMailer.class);
    private static final String SPAN_NAME = "report.email.send";

    private final PermissionsService permissionsService;
    private final MailManager mailManager;
    private final Storage storage;
    private final Tracer tracer;

    @Inject
    public ReportMailer(
            PermissionsService permissionsService,
            MailManager mailManager,
            Storage storage,
            TracerFactory tracerFactory) {
        this.permissionsService = permissionsService;
        this.mailManager = mailManager;
        this.storage = storage;
        this.tracer = tracerFactory.getTracer(ReportMailer.class.getName());
    }

    public void sendAsync(long userId, ReportExecutor executor) {
        new Thread(() -> {
            try {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                executor.execute(stream);
                sendReportEmail(userId, "report", stream.toByteArray());
            } catch (StorageException | IOException e) {
                LOGGER.warn("Report failed", e);
            }
        }).start();
    }

    public void sendReportEmail(long userId, String reportType, byte[] attachment) {
        Span span = tracer.spanBuilder(SPAN_NAME)
                .setParent(Context.current())
                .setAttribute("user.id", userId)
                .setAttribute("report.type", reportType)
                .setAttribute("attachment.size", attachment.length)
                .startSpan();

        try (var scope = span.makeCurrent()) {
            User user = storage.getObject(User.class, new Request(
                    new Columns.All(), new Condition.Equals("id", userId)));

            if (user != null && user.getEmail() != null && !user.getEmail().isEmpty()) {
                span.setAttribute("user.email", user.getEmail());

                MimeBodyPart attachmentPart = new MimeBodyPart();
                String fileName = reportType + ".xlsx";
                attachmentPart.setDataHandler(new DataHandler(
                        new ByteArrayDataSource(attachment, "application/vnd.ms-excel")));
                attachmentPart.setFileName(fileName);

                mailManager.sendMessage(user, "Report", "The report is attached.", attachmentPart);
                span.setStatus(StatusCode.OK);
                LOGGER.info("Report email sent to user {}", user.getEmail());
            } else {
                span.setStatus(StatusCode.ERROR, "User not found or has no email");
                LOGGER.warn("No user or email address for report");
            }
        } catch (StorageException | MessagingException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.warn("Failed to send report email", e);
        } finally {
            span.end();
        }
    }

    public interface ReportExecutor {
        void execute(OutputStream outputStream) throws StorageException, IOException;
    }
}