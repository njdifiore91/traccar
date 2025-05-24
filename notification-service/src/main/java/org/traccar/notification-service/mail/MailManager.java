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
package org.traccar.notification.mail;

import org.traccar.model.User;

import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeBodyPart;

/**
 * The MailManager interface defines the contract for email notification services
 * within the Traccar notification microservice architecture. It provides methods
 * for sending email notifications to users with optional attachments.
 * <p>
 * This interface supports distributed tracing through correlation IDs and integrates
 * with the notification service's asynchronous messaging architecture.
 */
public interface MailManager {

    /**
     * Checks if email functionality is enabled in the system.
     *
     * @return true if email sending is enabled, false otherwise
     */
    boolean getEmailEnabled();

    /**
     * Sends a plain text email message to a user.
     * <p>
     * This method maintains backward compatibility with existing implementations.
     *
     * @param user    the recipient user
     * @param system  indicates if this is a system notification (true) or user notification (false)
     * @param subject the email subject
     * @param body    the email body content
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body) throws MessagingException;

    /**
     * Sends an email message with an attachment to a user.
     * <p>
     * This method maintains backward compatibility with existing implementations.
     *
     * @param user       the recipient user
     * @param system     indicates if this is a system notification (true) or user notification (false)
     * @param subject    the email subject
     * @param body       the email body content
     * @param attachment the MIME attachment to include with the email
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment) throws MessagingException;

    /**
     * Sends a plain text email message to a user with correlation ID for distributed tracing.
     *
     * @param user          the recipient user
     * @param system        indicates if this is a system notification (true) or user notification (false)
     * @param subject       the email subject
     * @param body          the email body content
     * @param correlationId the unique identifier for tracing this request across microservices
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, String correlationId) throws MessagingException;

    /**
     * Sends an email message with an attachment to a user with correlation ID for distributed tracing.
     *
     * @param user          the recipient user
     * @param system        indicates if this is a system notification (true) or user notification (false)
     * @param subject       the email subject
     * @param body          the email body content
     * @param attachment    the MIME attachment to include with the email
     * @param correlationId the unique identifier for tracing this request across microservices
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment, String correlationId) 
            throws MessagingException;

    /**
     * Sends a plain text email message to a user with notification context for message broker integration.
     *
     * @param user              the recipient user
     * @param system            indicates if this is a system notification (true) or user notification (false)
     * @param subject           the email subject
     * @param body              the email body content
     * @param notificationContext additional context for the notification processing pipeline
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, 
            NotificationContext notificationContext) throws MessagingException;

    /**
     * Sends an email message with an attachment to a user with notification context for message broker integration.
     *
     * @param user               the recipient user
     * @param system             indicates if this is a system notification (true) or user notification (false)
     * @param subject            the email subject
     * @param body               the email body content
     * @param attachment         the MIME attachment to include with the email
     * @param notificationContext additional context for the notification processing pipeline
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment,
            NotificationContext notificationContext) throws MessagingException;
}