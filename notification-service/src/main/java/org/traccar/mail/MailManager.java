/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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

import org.traccar.model.User;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeBodyPart;

/**
 * The MailManager interface defines the contract for email delivery services within the
 * Traccar notification microservice architecture. It provides methods for sending email
 * notifications to users with optional attachments.
 * <p>
 * In the microservices architecture, the MailManager implementations handle the final delivery
 * of email notifications after they have been processed by the notification pipeline. Email
 * requests typically arrive via the message broker from other services that generate events
 * requiring notification.
 * <p>
 * Implementations must support distributed tracing through correlation IDs to enable
 * end-to-end tracking of notification requests across service boundaries.
 */
public interface MailManager {

    /**
     * Checks if email functionality is enabled in the current environment.
     *
     * @return true if email sending is enabled, false otherwise
     */
    boolean getEmailEnabled();

    /**
     * Sends a plain text email message to a user.
     *
     * @param user    the recipient user
     * @param system  flag indicating if this is a system notification
     * @param subject the email subject
     * @param body    the email body content
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body) throws MessagingException;

    /**
     * Sends an email message with an attachment to a user.
     *
     * @param user       the recipient user
     * @param system     flag indicating if this is a system notification
     * @param subject    the email subject
     * @param body       the email body content
     * @param attachment the email attachment
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment) throws MessagingException;

    /**
     * Sends a plain text email message to a user with correlation ID for distributed tracing.
     *
     * @param correlationId unique identifier for tracing this request across services
     * @param user          the recipient user
     * @param system        flag indicating if this is a system notification
     * @param subject       the email subject
     * @param body          the email body content
     * @throws MessagingException if there is an error sending the email
     */
    default void sendMessage(
            String correlationId, User user, boolean system, String subject, String body) throws MessagingException {
        sendMessage(user, system, subject, body);
    }

    /**
     * Sends an email message with an attachment to a user with correlation ID for distributed tracing.
     *
     * @param correlationId unique identifier for tracing this request across services
     * @param user          the recipient user
     * @param system        flag indicating if this is a system notification
     * @param subject       the email subject
     * @param body          the email body content
     * @param attachment    the email attachment
     * @throws MessagingException if there is an error sending the email
     */
    default void sendMessage(
            String correlationId, User user, boolean system, String subject, String body, MimeBodyPart attachment)
            throws MessagingException {
        sendMessage(user, system, subject, body, attachment);
    }

    /**
     * Sends a plain text email message to a user with notification metadata.
     *
     * @param correlationId    unique identifier for tracing this request across services
     * @param sourceService    the service that originated this notification request
     * @param notificationType the type of notification being sent
     * @param user             the recipient user
     * @param system           flag indicating if this is a system notification
     * @param subject          the email subject
     * @param body             the email body content
     * @throws MessagingException if there is an error sending the email
     */
    default void sendMessage(
            String correlationId, String sourceService, String notificationType,
            User user, boolean system, String subject, String body) throws MessagingException {
        sendMessage(correlationId, user, system, subject, body);
    }

    /**
     * Sends an email message with an attachment to a user with notification metadata.
     *
     * @param correlationId    unique identifier for tracing this request across services
     * @param sourceService    the service that originated this notification request
     * @param notificationType the type of notification being sent
     * @param user             the recipient user
     * @param system           flag indicating if this is a system notification
     * @param subject          the email subject
     * @param body             the email body content
     * @param attachment       the email attachment
     * @throws MessagingException if there is an error sending the email
     */
    default void sendMessage(
            String correlationId, String sourceService, String notificationType,
            User user, boolean system, String subject, String body, MimeBodyPart attachment)
            throws MessagingException {
        sendMessage(correlationId, user, system, subject, body, attachment);
    }
}