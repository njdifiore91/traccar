/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2017 - 2018 Andrey Kunitsyn (andrey@traccar.org)
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
package org.traccar.notificators;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.mail.MessagingException;
import org.traccar.mail.MailManager;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;

@Singleton
public class NotificatorMail extends Notificator {

    private final MailManager mailManager;
    private final NotificatorCircuitBreaker circuitBreaker;

    @Inject
    public NotificatorMail(MailManager mailManager, NotificationFormatter notificationFormatter,
                          NotificatorCircuitBreaker circuitBreaker) {
        super(notificationFormatter, "full");
        this.mailManager = mailManager;
        this.circuitBreaker = circuitBreaker;
    }

    @Override
    public void send(User user, NotificationMessage message, Event event, Position position) throws MessageException {
        // Use circuit breaker to protect against mail service failures
        circuitBreaker.executeNotification(this, user, message, event, position);
    }
    
    /**
     * Internal method to send notification through mail service.
     * This is called by the circuit breaker.
     */
    void sendInternal(User user, NotificationMessage message, Event event, Position position) throws MessageException {
        try {
            mailManager.sendMessage(user, false, message.getSubject(), message.getBody());
        } catch (MessagingException e) {
            throw new MessageException(e);
        }
    }

}