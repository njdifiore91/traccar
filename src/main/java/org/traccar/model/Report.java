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
package org.traccar.model;

import org.traccar.storage.StorageName;

import java.util.Date;

/**
 * Represents a generated report in the system.
 * Stores metadata about reports including their location in object storage.
 */
@StorageName("tc_reports")
public class Report extends BaseModel {

    private long userId;
    private long deviceId;
    private String type;
    private Date createdAt;
    private Date fromTime;
    private Date toTime;
    private String url;
    private String status;

    /**
     * Gets the ID of the user who requested the report.
     *
     * @return The user ID
     */
    public long getUserId() {
        return userId;
    }

    /**
     * Sets the ID of the user who requested the report.
     *
     * @param userId The user ID
     */
    public void setUserId(long userId) {
        this.userId = userId;
    }

    /**
     * Gets the ID of the device for which the report was generated.
     *
     * @return The device ID
     */
    public long getDeviceId() {
        return deviceId;
    }

    /**
     * Sets the ID of the device for which the report was generated.
     *
     * @param deviceId The device ID
     */
    public void setDeviceId(long deviceId) {
        this.deviceId = deviceId;
    }

    /**
     * Gets the type of report (e.g., "gpx", "csv", "xlsx").
     *
     * @return The report type
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the type of report.
     *
     * @param type The report type
     */
    public void setType(String type) {
        this.type = type;
    }

    /**
     * Gets the date and time when the report was created.
     *
     * @return The creation date
     */
    public Date getCreatedAt() {
        return createdAt;
    }

    /**
     * Sets the date and time when the report was created.
     *
     * @param createdAt The creation date
     */
    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    /**
     * Gets the start date for the report data.
     *
     * @return The from date
     */
    public Date getFromTime() {
        return fromTime;
    }

    /**
     * Sets the start date for the report data.
     *
     * @param fromTime The from date
     */
    public void setFromTime(Date fromTime) {
        this.fromTime = fromTime;
    }

    /**
     * Gets the end date for the report data.
     *
     * @return The to date
     */
    public Date getToTime() {
        return toTime;
    }

    /**
     * Sets the end date for the report data.
     *
     * @param toTime The to date
     */
    public void setToTime(Date toTime) {
        this.toTime = toTime;
    }

    /**
     * Gets the URL where the report can be accessed.
     *
     * @return The report URL
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets the URL where the report can be accessed.
     *
     * @param url The report URL
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Gets the status of the report (e.g., "pending", "completed", "failed").
     *
     * @return The report status
     */
    public String getStatus() {
        return status;
    }

    /**
     * Sets the status of the report.
     *
     * @param status The report status
     */
    public void setStatus(String status) {
        this.status = status;
    }
}