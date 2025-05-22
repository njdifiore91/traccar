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

import java.util.concurrent.CompletableFuture;

/**
 * Interface for email notification services in the Traccar microservices architecture.
 * Responsible for sending email notifications to users and providing health status
 * for container orchestration environments.
 * <p>
 * This service integrates with the distributed tracing system through correlation IDs
 * and exposes metrics for monitoring email delivery performance.
 */
public interface MailManager {

    /**
     * Checks if email functionality is enabled in the configuration.
     *
     * @return true if email sending is enabled, false otherwise
     */
    boolean getEmailEnabled();

    /**
     * Sends an email message to a user synchronously.
     *
     * @param user    recipient user with email information
     * @param system  whether it's a system-generated message
     * @param subject email subject
     * @param body    email body content
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body) throws MessagingException;

    /**
     * Sends an email message with attachment to a user synchronously.
     *
     * @param user       recipient user with email information
     * @param system     whether it's a system-generated message
     * @param subject    email subject
     * @param body       email body content
     * @param attachment file attachment to include in the email
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment) throws MessagingException;

    /**
     * Sends an email message to a user synchronously with correlation ID for distributed tracing.
     *
     * @param user          recipient user with email information
     * @param system        whether it's a system-generated message
     * @param subject       email subject
     * @param body          email body content
     * @param correlationId unique identifier for tracing this request across services
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, String correlationId) throws MessagingException;

    /**
     * Sends an email message with attachment to a user synchronously with correlation ID for distributed tracing.
     *
     * @param user          recipient user with email information
     * @param system        whether it's a system-generated message
     * @param subject       email subject
     * @param body          email body content
     * @param attachment    file attachment to include in the email
     * @param correlationId unique identifier for tracing this request across services
     * @throws MessagingException if there is an error sending the email
     */
    void sendMessage(
            User user, boolean system, String subject, String body, MimeBodyPart attachment, String correlationId) 
            throws MessagingException;

    /**
     * Sends an email message to a user asynchronously with correlation ID for distributed tracing.
     *
     * @param user          recipient user with email information
     * @param system        whether it's a system-generated message
     * @param subject       email subject
     * @param body          email body content
     * @param correlationId unique identifier for tracing this request across services
     * @return CompletableFuture that completes when the email is sent or fails with MessagingException
     */
    CompletableFuture<Void> sendMessageAsync(
            User user, boolean system, String subject, String body, String correlationId);

    /**
     * Sends an email message with attachment to a user asynchronously with correlation ID for distributed tracing.
     *
     * @param user          recipient user with email information
     * @param system        whether it's a system-generated message
     * @param subject       email subject
     * @param body          email body content
     * @param attachment    file attachment to include in the email
     * @param correlationId unique identifier for tracing this request across services
     * @return CompletableFuture that completes when the email is sent or fails with MessagingException
     */
    CompletableFuture<Void> sendMessageAsync(
            User user, boolean system, String subject, String body, MimeBodyPart attachment, String correlationId);

    /**
     * Checks if the mail service is healthy and can send emails.
     * Used by container orchestration for liveness probes.
     *
     * @return true if the service is healthy, false otherwise
     */
    boolean isHealthy();

    /**
     * Checks if the mail service is ready to accept requests.
     * Used by container orchestration for readiness probes.
     *
     * @return true if the service is ready to accept requests, false otherwise
     */
    boolean isReady();

    /**
     * Registers this mail service instance with the service discovery system.
     * This allows other services to discover and use this mail service.
     *
     * @param instanceId unique identifier for this service instance
     * @param port       port on which this service is running
     * @return true if registration was successful, false otherwise
     */
    boolean registerWithServiceDiscovery(String instanceId, int port);

    /**
     * Deregisters this mail service instance from the service discovery system.
     * Should be called during graceful shutdown.
     *
     * @param instanceId unique identifier for this service instance
     * @return true if deregistration was successful, false otherwise
     */
    boolean deregisterFromServiceDiscovery(String instanceId);

    /**
     * Gets metrics about email sending operations.
     * Includes counts of sent emails, failures, and performance statistics.
     *
     * @return a string representation of the metrics (typically JSON format)
     */
    String getMetrics();

    /**
     * Resets the metrics counters.
     * Typically used during testing or after metrics collection.
     */
    void resetMetrics();
}