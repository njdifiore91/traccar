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
package org.traccar.reports;

/**
 * Represents a request for asynchronous report generation.
 * This class is used for message broker communication between services.
 */
public class ReportRequest {

    private String reportId;
    private String reportType;
    private long deviceId;
    private long userId;
    private long fromTime;
    private long toTime;
    private String format;
    private String callbackUrl;

    /**
     * Gets the unique identifier for this report request.
     *
     * @return The report ID
     */
    public String getReportId() {
        return reportId;
    }

    /**
     * Sets the unique identifier for this report request.
     *
     * @param reportId The report ID
     */
    public void setReportId(String reportId) {
        this.reportId = reportId;
    }

    /**
     * Gets the type of report to generate (e.g., "gpx", "csv", "xlsx").
     *
     * @return The report type
     */
    public String getReportType() {
        return reportType;
    }

    /**
     * Sets the type of report to generate.
     *
     * @param reportType The report type
     */
    public void setReportType(String reportType) {
        this.reportType = reportType;
    }

    /**
     * Gets the ID of the device for which to generate the report.
     *
     * @return The device ID
     */
    public long getDeviceId() {
        return deviceId;
    }

    /**
     * Sets the ID of the device for which to generate the report.
     *
     * @param deviceId The device ID
     */
    public void setDeviceId(long deviceId) {
        this.deviceId = deviceId;
    }

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
     * Gets the start time for the report data in milliseconds since epoch.
     *
     * @return The from time
     */
    public long getFromTime() {
        return fromTime;
    }

    /**
     * Sets the start time for the report data in milliseconds since epoch.
     *
     * @param fromTime The from time
     */
    public void setFromTime(long fromTime) {
        this.fromTime = fromTime;
    }

    /**
     * Gets the end time for the report data in milliseconds since epoch.
     *
     * @return The to time
     */
    public long getToTime() {
        return toTime;
    }

    /**
     * Sets the end time for the report data in milliseconds since epoch.
     *
     * @param toTime The to time
     */
    public void setToTime(long toTime) {
        this.toTime = toTime;
    }

    /**
     * Gets the output format for the report (if applicable).
     *
     * @return The format
     */
    public String getFormat() {
        return format;
    }

    /**
     * Sets the output format for the report (if applicable).
     *
     * @param format The format
     */
    public void setFormat(String format) {
        this.format = format;
    }

    /**
     * Gets the callback URL for notifying when the report is complete (if applicable).
     *
     * @return The callback URL
     */
    public String getCallbackUrl() {
        return callbackUrl;
    }

    /**
     * Sets the callback URL for notifying when the report is complete (if applicable).
     *
     * @param callbackUrl The callback URL
     */
    public void setCallbackUrl(String callbackUrl) {
        this.callbackUrl = callbackUrl;
    }
}