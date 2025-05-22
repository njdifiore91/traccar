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
package org.traccar.reports.common;

import java.time.Instant;

/**
 * Represents the status of a report execution, including its current state,
 * progress, start and end times, and any error information.
 */
public class ReportExecutionStatus {
    
    /**
     * Possible states of a report execution.
     */
    public enum State {
        QUEUED,      // Report is queued for execution
        RUNNING,     // Report is currently being executed
        COMPLETED,   // Report execution completed successfully
        FAILED,      // Report execution failed
        CANCELLED    // Report execution was cancelled
    }
    
    private final String executionId;
    private State state;
    private double progress; // 0.0 to 1.0
    private Instant startTime;
    private Instant endTime;
    private String errorMessage;
    private String resultUrl;
    
    /**
     * Creates a new report execution status.
     * 
     * @param executionId The unique ID of the report execution
     * @param state The initial state of the report execution
     */
    public ReportExecutionStatus(String executionId, State state) {
        this.executionId = executionId;
        this.state = state;
        this.progress = 0.0;
        this.startTime = Instant.now();
    }
    
    /**
     * Gets the unique ID of the report execution.
     * 
     * @return The execution ID
     */
    public String getExecutionId() {
        return executionId;
    }
    
    /**
     * Gets the current state of the report execution.
     * 
     * @return The current state
     */
    public State getState() {
        return state;
    }
    
    /**
     * Sets the current state of the report execution.
     * 
     * @param state The new state
     */
    public void setState(State state) {
        this.state = state;
        if (state == State.COMPLETED || state == State.FAILED || state == State.CANCELLED) {
            this.endTime = Instant.now();
        }
    }
    
    /**
     * Gets the current progress of the report execution (0.0 to 1.0).
     * 
     * @return The current progress
     */
    public double getProgress() {
        return progress;
    }
    
    /**
     * Sets the current progress of the report execution.
     * 
     * @param progress The new progress (0.0 to 1.0)
     */
    public void setProgress(double progress) {
        this.progress = Math.max(0.0, Math.min(1.0, progress));
    }
    
    /**
     * Gets the time when the report execution started.
     * 
     * @return The start time
     */
    public Instant getStartTime() {
        return startTime;
    }
    
    /**
     * Gets the time when the report execution ended (completed, failed, or cancelled).
     * 
     * @return The end time, or null if the execution is still in progress
     */
    public Instant getEndTime() {
        return endTime;
    }
    
    /**
     * Gets the error message if the report execution failed.
     * 
     * @return The error message, or null if the execution did not fail
     */
    public String getErrorMessage() {
        return errorMessage;
    }
    
    /**
     * Sets the error message for a failed report execution.
     * 
     * @param errorMessage The error message
     */
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
    
    /**
     * Gets the URL where the report result can be accessed.
     * 
     * @return The result URL, or null if the execution is not completed
     */
    public String getResultUrl() {
        return resultUrl;
    }
    
    /**
     * Sets the URL where the report result can be accessed.
     * 
     * @param resultUrl The result URL
     */
    public void setResultUrl(String resultUrl) {
        this.resultUrl = resultUrl;
    }
    
    /**
     * Gets the duration of the report execution.
     * 
     * @return The duration in milliseconds, or -1 if the execution is still in progress
     */
    public long getDurationMillis() {
        if (endTime == null) {
            return Instant.now().toEpochMilli() - startTime.toEpochMilli();
        }
        return endTime.toEpochMilli() - startTime.toEpochMilli();
    }
}