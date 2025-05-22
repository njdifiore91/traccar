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
 * Represents the resource usage of a report execution, including memory, CPU,
 * and I/O usage.
 */
public class ReportResourceUsage {
    
    private final String executionId;
    private long memoryUsageBytes;
    private double cpuUsagePercent;
    private long diskIoBytes;
    private long networkIoBytes;
    private long rowsProcessed;
    
    /**
     * Creates a new report resource usage tracker.
     * 
     * @param executionId The unique ID of the report execution
     */
    public ReportResourceUsage(String executionId) {
        this.executionId = executionId;
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
     * Gets the current memory usage of the report execution in bytes.
     * 
     * @return The memory usage in bytes
     */
    public long getMemoryUsageBytes() {
        return memoryUsageBytes;
    }
    
    /**
     * Sets the current memory usage of the report execution.
     * 
     * @param memoryUsageBytes The memory usage in bytes
     */
    public void setMemoryUsageBytes(long memoryUsageBytes) {
        this.memoryUsageBytes = memoryUsageBytes;
    }
    
    /**
     * Gets the current CPU usage of the report execution as a percentage.
     * 
     * @return The CPU usage percentage (0-100)
     */
    public double getCpuUsagePercent() {
        return cpuUsagePercent;
    }
    
    /**
     * Sets the current CPU usage of the report execution.
     * 
     * @param cpuUsagePercent The CPU usage percentage (0-100)
     */
    public void setCpuUsagePercent(double cpuUsagePercent) {
        this.cpuUsagePercent = cpuUsagePercent;
    }
    
    /**
     * Gets the total disk I/O performed by the report execution in bytes.
     * 
     * @return The disk I/O in bytes
     */
    public long getDiskIoBytes() {
        return diskIoBytes;
    }
    
    /**
     * Sets the total disk I/O performed by the report execution.
     * 
     * @param diskIoBytes The disk I/O in bytes
     */
    public void setDiskIoBytes(long diskIoBytes) {
        this.diskIoBytes = diskIoBytes;
    }
    
    /**
     * Gets the total network I/O performed by the report execution in bytes.
     * 
     * @return The network I/O in bytes
     */
    public long getNetworkIoBytes() {
        return networkIoBytes;
    }
    
    /**
     * Sets the total network I/O performed by the report execution.
     * 
     * @param networkIoBytes The network I/O in bytes
     */
    public void setNetworkIoBytes(long networkIoBytes) {
        this.networkIoBytes = networkIoBytes;
    }
    
    /**
     * Gets the number of database rows processed by the report execution.
     * 
     * @return The number of rows processed
     */
    public long getRowsProcessed() {
        return rowsProcessed;
    }
    
    /**
     * Sets the number of database rows processed by the report execution.
     * 
     * @param rowsProcessed The number of rows processed
     */
    public void setRowsProcessed(long rowsProcessed) {
        this.rowsProcessed = rowsProcessed;
    }
    
    /**
     * Increments the number of database rows processed by the report execution.
     * 
     * @param count The number of additional rows processed
     */
    public void incrementRowsProcessed(long count) {
        this.rowsProcessed += count;
    }
}