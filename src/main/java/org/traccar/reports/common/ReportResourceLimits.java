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

/**
 * Represents resource limits for report execution, including memory, CPU,
 * execution time, and row limits.
 */
public class ReportResourceLimits {
    
    private long maxMemoryBytes;
    private int maxCpuPercent;
    private long maxExecutionTimeMillis;
    private long maxRowsProcessed;
    private long maxOutputSizeBytes;
    
    /**
     * Creates a new report resource limits object with default values.
     */
    public ReportResourceLimits() {
        // Default limits
        this.maxMemoryBytes = 256 * 1024 * 1024; // 256 MB
        this.maxCpuPercent = 50; // 50% of one CPU core
        this.maxExecutionTimeMillis = 5 * 60 * 1000; // 5 minutes
        this.maxRowsProcessed = 1000000; // 1 million rows
        this.maxOutputSizeBytes = 50 * 1024 * 1024; // 50 MB
    }
    
    /**
     * Gets the maximum memory usage allowed for a report execution in bytes.
     * 
     * @return The maximum memory usage in bytes
     */
    public long getMaxMemoryBytes() {
        return maxMemoryBytes;
    }
    
    /**
     * Sets the maximum memory usage allowed for a report execution.
     * 
     * @param maxMemoryBytes The maximum memory usage in bytes
     * @return This object for method chaining
     */
    public ReportResourceLimits setMaxMemoryBytes(long maxMemoryBytes) {
        this.maxMemoryBytes = maxMemoryBytes;
        return this;
    }
    
    /**
     * Gets the maximum CPU usage allowed for a report execution as a percentage.
     * 
     * @return The maximum CPU usage percentage (0-100)
     */
    public int getMaxCpuPercent() {
        return maxCpuPercent;
    }
    
    /**
     * Sets the maximum CPU usage allowed for a report execution.
     * 
     * @param maxCpuPercent The maximum CPU usage percentage (0-100)
     * @return This object for method chaining
     */
    public ReportResourceLimits setMaxCpuPercent(int maxCpuPercent) {
        this.maxCpuPercent = maxCpuPercent;
        return this;
    }
    
    /**
     * Gets the maximum execution time allowed for a report in milliseconds.
     * 
     * @return The maximum execution time in milliseconds
     */
    public long getMaxExecutionTimeMillis() {
        return maxExecutionTimeMillis;
    }
    
    /**
     * Sets the maximum execution time allowed for a report.
     * 
     * @param maxExecutionTimeMillis The maximum execution time in milliseconds
     * @return This object for method chaining
     */
    public ReportResourceLimits setMaxExecutionTimeMillis(long maxExecutionTimeMillis) {
        this.maxExecutionTimeMillis = maxExecutionTimeMillis;
        return this;
    }
    
    /**
     * Gets the maximum number of database rows that can be processed by a report.
     * 
     * @return The maximum number of rows
     */
    public long getMaxRowsProcessed() {
        return maxRowsProcessed;
    }
    
    /**
     * Sets the maximum number of database rows that can be processed by a report.
     * 
     * @param maxRowsProcessed The maximum number of rows
     * @return This object for method chaining
     */
    public ReportResourceLimits setMaxRowsProcessed(long maxRowsProcessed) {
        this.maxRowsProcessed = maxRowsProcessed;
        return this;
    }
    
    /**
     * Gets the maximum output size allowed for a report in bytes.
     * 
     * @return The maximum output size in bytes
     */
    public long getMaxOutputSizeBytes() {
        return maxOutputSizeBytes;
    }
    
    /**
     * Sets the maximum output size allowed for a report.
     * 
     * @param maxOutputSizeBytes The maximum output size in bytes
     * @return This object for method chaining
     */
    public ReportResourceLimits setMaxOutputSizeBytes(long maxOutputSizeBytes) {
        this.maxOutputSizeBytes = maxOutputSizeBytes;
        return this;
    }
    
    /**
     * Creates a builder for ReportResourceLimits.
     * 
     * @return A new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }
    
    /**
     * Builder class for ReportResourceLimits.
     */
    public static class Builder {
        private final ReportResourceLimits limits;
        
        private Builder() {
            limits = new ReportResourceLimits();
        }
        
        public Builder withMaxMemoryBytes(long maxMemoryBytes) {
            limits.setMaxMemoryBytes(maxMemoryBytes);
            return this;
        }
        
        public Builder withMaxCpuPercent(int maxCpuPercent) {
            limits.setMaxCpuPercent(maxCpuPercent);
            return this;
        }
        
        public Builder withMaxExecutionTimeMillis(long maxExecutionTimeMillis) {
            limits.setMaxExecutionTimeMillis(maxExecutionTimeMillis);
            return this;
        }
        
        public Builder withMaxRowsProcessed(long maxRowsProcessed) {
            limits.setMaxRowsProcessed(maxRowsProcessed);
            return this;
        }
        
        public Builder withMaxOutputSizeBytes(long maxOutputSizeBytes) {
            limits.setMaxOutputSizeBytes(maxOutputSizeBytes);
            return this;
        }
        
        public ReportResourceLimits build() {
            return limits;
        }
    }
}