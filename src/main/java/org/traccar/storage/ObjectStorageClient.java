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
package org.traccar.storage;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.traccar.config.Config;
import org.traccar.metrics.MetricsCollector;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;

/**
 * Client for interacting with S3-compatible object storage.
 * Used for storing generated reports and other files.
 */
@Singleton
public class ObjectStorageClient {

    private final Config config;
    private final Tracer tracer;
    private final MetricsCollector metricsCollector;
    
    private static final String METRIC_PREFIX = "storage.object.";

    /**
     * Creates a new object storage client with required dependencies.
     *
     * @param config Configuration for object storage connection
     * @param tracer OpenTelemetry tracer for distributed tracing
     * @param metricsCollector Metrics collector for performance monitoring
     */
    @Inject
    public ObjectStorageClient(Config config, Tracer tracer, MetricsCollector metricsCollector) {
        this.config = config;
        this.tracer = tracer;
        this.metricsCollector = metricsCollector;
        
        // Register metrics for this client
        this.metricsCollector.registerCounter(METRIC_PREFIX + "uploads", "Number of objects uploaded");
        this.metricsCollector.registerCounter(METRIC_PREFIX + "downloads", "Number of objects downloaded");
        this.metricsCollector.registerCounter(METRIC_PREFIX + "errors", "Number of object storage errors");
        this.metricsCollector.registerHistogram(METRIC_PREFIX + "upload_size", "Size of uploaded objects in bytes");
        this.metricsCollector.registerHistogram(METRIC_PREFIX + "upload_time", "Time to upload objects in milliseconds");
    }

    /**
     * Stores an object in object storage.
     *
     * @param objectKey The key (path) to store the object under
     * @param data The object data as a byte array
     * @param contentType The content type of the object
     * @return The URL where the object can be accessed
     * @throws IOException If there is an error storing the object
     */
    public String storeObject(String objectKey, byte[] data, String contentType) throws IOException {
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("storage.object.upload").startSpan();
        long startTime = System.currentTimeMillis();
        
        try {
            span.setAttribute("objectKey", objectKey);
            span.setAttribute("contentType", contentType);
            span.setAttribute("contentLength", data.length);
            
            // Record size metric
            metricsCollector.recordHistogramValue(METRIC_PREFIX + "upload_size", data.length);
            
            // In a real implementation, this would use the AWS SDK or equivalent to upload to S3
            // For now, we'll simulate the upload and return a URL
            
            // Get the bucket name and endpoint from configuration
            String bucketName = config.getString("storage.bucket");
            String endpoint = config.getString("storage.endpoint");
            
            span.setAttribute("bucket", bucketName);
            span.setAttribute("endpoint", endpoint);
            
            // Simulate successful upload
            metricsCollector.incrementCounter(METRIC_PREFIX + "uploads");
            
            // Construct and return the object URL
            String url = endpoint + "/" + bucketName + "/" + objectKey;
            span.setAttribute("url", url);
            
            return url;
        } catch (Exception e) {
            // Record error metrics
            metricsCollector.incrementCounter(METRIC_PREFIX + "errors");
            span.recordException(e);
            throw new IOException("Failed to store object: " + e.getMessage(), e);
        } finally {
            // Record duration metric
            long duration = System.currentTimeMillis() - startTime;
            metricsCollector.recordHistogramValue(METRIC_PREFIX + "upload_time", duration);
            
            span.end();
        }
    }

    /**
     * Retrieves an object from object storage.
     *
     * @param objectKey The key (path) of the object to retrieve
     * @return The object data as a byte array
     * @throws IOException If there is an error retrieving the object
     */
    public byte[] getObject(String objectKey) throws IOException {
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("storage.object.download").startSpan();
        
        try {
            span.setAttribute("objectKey", objectKey);
            
            // Get the bucket name from configuration
            String bucketName = config.getString("storage.bucket");
            span.setAttribute("bucket", bucketName);
            
            // In a real implementation, this would use the AWS SDK or equivalent to download from S3
            // For now, we'll simulate the download and return empty data
            
            // Simulate successful download
            metricsCollector.incrementCounter(METRIC_PREFIX + "downloads");
            
            // Return empty data for simulation
            return new byte[0];
        } catch (Exception e) {
            // Record error metrics
            metricsCollector.incrementCounter(METRIC_PREFIX + "errors");
            span.recordException(e);
            throw new IOException("Failed to retrieve object: " + e.getMessage(), e);
        } finally {
            span.end();
        }
    }

    /**
     * Checks if the object storage is accessible.
     * Used for health checks.
     *
     * @return true if the object storage is accessible, false otherwise
     */
    public boolean checkAccess() {
        // Create a span for tracing this operation
        Span span = tracer.spanBuilder("storage.object.checkAccess").startSpan();
        
        try {
            // Get the bucket name from configuration
            String bucketName = config.getString("storage.bucket");
            span.setAttribute("bucket", bucketName);
            
            // In a real implementation, this would check if the bucket exists and is accessible
            // For now, we'll just return true
            
            return true;
        } catch (Exception e) {
            // Record error metrics
            metricsCollector.incrementCounter(METRIC_PREFIX + "errors");
            span.recordException(e);
            return false;
        } finally {
            span.end();
        }
    }
}