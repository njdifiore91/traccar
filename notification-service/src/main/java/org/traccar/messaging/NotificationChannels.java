/*
 * Copyright 2012 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar.messaging;

import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.SubscribableChannel;

/**
 * Defines the message channels for the notification service.
 * This interface configures the input and output channels for the message broker integration.
 */
public interface NotificationChannels {

    /**
     * Channel name for incoming events from the event service.
     */
    String EVENTS_INPUT = "events-input";
    
    /**
     * Channel name for outgoing email notifications.
     */
    String EMAIL_OUTPUT = "email-out";
    
    /**
     * Channel name for outgoing SMS notifications.
     */
    String SMS_OUTPUT = "sms-out";
    
    /**
     * Channel name for outgoing push notifications.
     */
    String PUSH_OUTPUT = "push-out";
    
    /**
     * Channel name for outgoing web notifications.
     */
    String WEB_OUTPUT = "web-out";
    
    /**
     * Channel name for notification status updates.
     */
    String STATUS_OUTPUT = "notification-status";
    
    /**
     * Channel name for dead letter queue.
     */
    String DEAD_LETTER_OUTPUT = "dead-letter-queue";
    
    /**
     * Input channel for consuming events from the event service.
     * 
     * @return The subscribable channel for incoming events
     */
    @Input(EVENTS_INPUT)
    SubscribableChannel eventsInput();
    
    /**
     * Output channel for sending email notifications.
     * 
     * @return The message channel for outgoing email notifications
     */
    @Output(EMAIL_OUTPUT)
    MessageChannel emailOutput();
    
    /**
     * Output channel for sending SMS notifications.
     * 
     * @return The message channel for outgoing SMS notifications
     */
    @Output(SMS_OUTPUT)
    MessageChannel smsOutput();
    
    /**
     * Output channel for sending push notifications.
     * 
     * @return The message channel for outgoing push notifications
     */
    @Output(PUSH_OUTPUT)
    MessageChannel pushOutput();
    
    /**
     * Output channel for sending web notifications.
     * 
     * @return The message channel for outgoing web notifications
     */
    @Output(WEB_OUTPUT)
    MessageChannel webOutput();
    
    /**
     * Output channel for sending notification status updates.
     * 
     * @return The message channel for notification status updates
     */
    @Output(STATUS_OUTPUT)
    MessageChannel statusOutput();
    
    /**
     * Output channel for sending failed notifications to the dead letter queue.
     * 
     * @return The message channel for the dead letter queue
     */
    @Output(DEAD_LETTER_OUTPUT)
    MessageChannel deadLetterOutput();
}