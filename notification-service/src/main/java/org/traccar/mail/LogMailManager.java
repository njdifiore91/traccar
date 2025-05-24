/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.mail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.model.User;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeBodyPart;

/**
 * A MailManager implementation that logs email details instead of sending them.
 * This implementation is useful for development, testing, and environments
 * where actual email sending is not required or configured.
 * 
 * In the microservices architecture, this implementation integrates with the
 * distributed logging system and includes correlation IDs for request tracing
 * across services.
 */
public class LogMailManager implements MailManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogMailManager.class);
    
    // Standard keys for distributed tracing - will check both common formats
    private static final String TRACE_ID_KEY = "traceId";
    private static final String X_B3_TRACE_ID_KEY = "X-B3-TraceId";
    private static final String CORRELATION_ID_KEY = "Correlation-ID";

    @Override
    public boolean getEmailEnabled() {
        return true;
    }

    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body) throws MessagingException {
        sendMessage(user, system, subject, body, null);
    }

    @Override
    public void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment) throws MessagingException {
        
        // Get correlation ID from MDC, checking multiple possible keys
        String correlationId = MDC.get(TRACE_ID_KEY);
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = MDC.get(X_B3_TRACE_ID_KEY);
        }
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = MDC.get(CORRELATION_ID_KEY);
        }
        if (correlationId == null || correlationId.isEmpty()) {
            correlationId = "no-trace-id";
        }
        
        // Log the email details with structured format including correlation ID for centralized logging
        LOGGER.info(
                "Email notification [correlationId={}] - To: {}, System: {}, Subject: {}, Attachment: {}, Body:\n{}",
                correlationId,
                user.getEmail(),
                system,
                subject,
                attachment != null ? attachment.getFileName() : "none",
                body);
    }
}