/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
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

/**
 * Enumeration of notification priority levels used for message broker routing
 * and delivery prioritization in the notification service.
 */
public enum NotificationPriority {
    
    /**
     * High priority notifications that should be delivered immediately.
     * Used for critical alerts and time-sensitive notifications.
     */
    HIGH,
    
    /**
     * Normal priority notifications with standard delivery expectations.
     * Used for most regular notifications.
     */
    NORMAL,
    
    /**
     * Low priority notifications that can be delayed if system is under load.
     * Used for informational and non-time-sensitive notifications.
     */
    LOW
}