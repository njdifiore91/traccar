/*
 * Copyright 2023-2025 Anton Tananaev (anton@traccar.org)
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

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.config.ConfigKey;

/**
 * Configuration keys for the ObjectStorageClient.
 */
class ObjectStorageKeys {
    public static final ConfigKey REPORT_STORAGE_ENDPOINT = new ConfigKey(
            "report.storage.endpoint", String.class);
    
    public static final ConfigKey REPORT_STORAGE_REGION = new ConfigKey(
            "report.storage.region", String.class);
    
    public static final ConfigKey REPORT_STORAGE_ACCESS_KEY = new ConfigKey(
            "report.storage.accessKey", String.class);
    
    public static final ConfigKey REPORT_STORAGE_SECRET_KEY = new ConfigKey(
            "report.storage.secretKey", String.class);
    
    public static final ConfigKey REPORT_STORAGE_BUCKET = new ConfigKey(
            "report.storage.bucket", String.class);
    
    public static final ConfigKey REPORT_STORAGE_KEY_PREFIX = new ConfigKey(
            "report.storage.keyPrefix", String.class, "reports/");
    
    public static final ConfigKey REPORT_STORAGE_PUBLIC_ACCESS = new ConfigKey(
            "report.storage.publicAccess", Boolean.class, false);
    
    public static final ConfigKey REPORT_STORAGE_RETENTION_DAYS = new ConfigKey(
            "report.storage.retentionDays", Integer.class, 30);
    
    public static final ConfigKey REPORT_STORAGE_MAX_RETRIES = new ConfigKey(
            "report.storage.maxRetries", Integer.class, 3);
    
    public static final ConfigKey REPORT_STORAGE_RETRY_DELAY_MS = new ConfigKey(
            "report.storage.retryDelayMs", Integer.class, 1000);
    
    public static final ConfigKey REPORT_STORAGE_CONNECTION_TIMEOUT = new ConfigKey(
            "report.storage.connectionTimeout", Integer.class, 10000);
    
    public static final ConfigKey REPORT_STORAGE_SOCKET_TIMEOUT = new ConfigKey(
            "report.storage.socketTimeout", Integer.class, 30000);
}

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Client for interacting with S3-compatible object storage services to store and retrieve generated report files.
 * This component abstracts the underlying storage implementation details, providing a unified interface for
 * report storage operations.
 * 
 * Features:
 * - S3-compatible API client for report file storage
 * - Support for signed URLs for secure report access
 * - Configurable retention policies for report files
 * - Support for multi-part uploads for large reports
 * - Error handling and retry logic for storage operations
 * - Instrumentation for monitoring and tracing
 */
@Singleton
public class ObjectStorageClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageClient.class);
    
    private static final int DEFAULT_RETRY_ATTEMPTS = 3;
    private static final int DEFAULT_RETRY_DELAY_MS = 1000;
    private static final long DEFAULT_PART_SIZE = 5 * 1024 * 1024; // 5MB
    private static final int DEFAULT_URL_EXPIRATION_HOURS = 24;

    private final Config config;
    private final AmazonS3 s3Client;
    private final String bucketName;
    private final Tracer tracer;

    /**
     * Constructs a new ObjectStorageClient with the specified configuration and tracer.
     *
     * @param config The application configuration
     * @param tracer The OpenTelemetry tracer for instrumentation
     */
    @Inject
    public ObjectStorageClient(Config config, Tracer tracer) {
        this.config = config;
        this.tracer = tracer;
        this.bucketName = config.getString(ObjectStorageKeys.REPORT_STORAGE_BUCKET);
        
        String endpoint = config.getString(ObjectStorageKeys.REPORT_STORAGE_ENDPOINT);
        String region = config.getString(ObjectStorageKeys.REPORT_STORAGE_REGION);
        String accessKey = config.getString(ObjectStorageKeys.REPORT_STORAGE_ACCESS_KEY);
        String secretKey = config.getString(ObjectStorageKeys.REPORT_STORAGE_SECRET_KEY);
        
        // Configure client with retry and timeout settings
        ClientConfiguration clientConfig = new ClientConfiguration()
                .withMaxErrorRetry(config.getInteger(ObjectStorageKeys.REPORT_STORAGE_MAX_RETRIES))
                .withConnectionTimeout(config.getInteger(ObjectStorageKeys.REPORT_STORAGE_CONNECTION_TIMEOUT))
                .withSocketTimeout(config.getInteger(ObjectStorageKeys.REPORT_STORAGE_SOCKET_TIMEOUT));
        
        // Force signature V4 for compatibility with most S3-compatible storage providers
        clientConfig.setSignerOverride("AWSS3V4SignerType");
        
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        
        this.s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .withClientConfiguration(clientConfig)
                .withPathStyleAccessEnabled(true) // Required for most S3-compatible storage providers
                .build();
        
        // Ensure bucket exists
        ensureBucketExists();
    }

    /**
     * Ensures that the configured bucket exists, creating it if necessary.
     */
    private void ensureBucketExists() {
        Span span = tracer.spanBuilder("ObjectStorageClient.ensureBucketExists").startSpan();
        try (Scope scope = span.makeCurrent()) {
            if (!s3Client.doesBucketExistV2(bucketName)) {
                LOGGER.info("Creating bucket: {}", bucketName);
                s3Client.createBucket(bucketName);
                LOGGER.info("Bucket created successfully: {}", bucketName);
            }
        } catch (AmazonServiceException e) {
            LOGGER.error("Error ensuring bucket exists: {}", e.getMessage(), e);
            span.recordException(e);
        } finally {
            span.end();
        }
    }

    /**
     * Uploads a report file to the object storage.
     *
     * @param reportId The unique identifier for the report
     * @param data The report data as a byte array
     * @param contentType The content type of the report (e.g., "application/pdf")
     * @return The object key of the uploaded report
     * @throws IOException If an I/O error occurs during upload
     */
    public String uploadReport(String reportId, byte[] data, String contentType) throws IOException {
        Span span = tracer.spanBuilder("ObjectStorageClient.uploadReport").startSpan();
        span.setAttribute("reportId", reportId);
        span.setAttribute("contentType", contentType);
        span.setAttribute("size", data.length);
        
        String objectKey = generateObjectKey(reportId);
        
        try (Scope scope = span.makeCurrent()) {
            if (data.length > DEFAULT_PART_SIZE) {
                uploadLargeReport(objectKey, data, contentType);
            } else {
                uploadSmallReport(objectKey, data, contentType);
            }
            return objectKey;
        } catch (Exception e) {
            LOGGER.error("Error uploading report: {}", e.getMessage(), e);
            span.recordException(e);
            throw new IOException("Failed to upload report to object storage", e);
        } finally {
            span.end();
        }
    }

    /**
     * Uploads a small report file (less than 5MB) in a single operation.
     *
     * @param objectKey The object key to use for the report
     * @param data The report data as a byte array
     * @param contentType The content type of the report
     */
    private void uploadSmallReport(String objectKey, byte[] data, String contentType) {
        Span span = tracer.spanBuilder("ObjectStorageClient.uploadSmallReport").startSpan();
        span.setAttribute("objectKey", objectKey);
        span.setAttribute("size", data.length);
        
        try (Scope scope = span.makeCurrent()) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(data.length);
            metadata.setContentType(contentType);
            
            // Set retention period if configured
            int retentionDays = config.getInteger(ObjectStorageKeys.REPORT_STORAGE_RETENTION_DAYS);
            if (retentionDays > 0) {
                metadata.setExpirationTime(new Date(System.currentTimeMillis() + 
                        TimeUnit.DAYS.toMillis(retentionDays)));
            }
            
            PutObjectRequest request = new PutObjectRequest(
                    bucketName,
                    objectKey,
                    new ByteArrayInputStream(data),
                    metadata);
            
            // Make reports publicly accessible if configured
            if (config.getBoolean(ObjectStorageKeys.REPORT_STORAGE_PUBLIC_ACCESS)) {
                request.withCannedAcl(CannedAccessControlList.PublicRead);
            }
            
            PutObjectResult result = executeWithRetry(() -> s3Client.putObject(request), "putObject");
            LOGGER.debug("Report uploaded successfully: {}, ETag: {}", objectKey, result.getETag());
        } catch (Exception e) {
            LOGGER.error("Error uploading small report: {}", e.getMessage(), e);
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Uploads a large report file (greater than 5MB) using multipart upload.
     *
     * @param objectKey The object key to use for the report
     * @param data The report data as a byte array
     * @param contentType The content type of the report
     * @throws IOException If an I/O error occurs during upload
     */
    private void uploadLargeReport(String objectKey, byte[] data, String contentType) throws IOException {
        Span span = tracer.spanBuilder("ObjectStorageClient.uploadLargeReport").startSpan();
        span.setAttribute("objectKey", objectKey);
        span.setAttribute("size", data.length);
        
        try (Scope scope = span.makeCurrent()) {
            // Initialize multipart upload
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(contentType);
            
            // Set retention period if configured
            int retentionDays = config.getInteger(ObjectStorageKeys.REPORT_STORAGE_RETENTION_DAYS);
            if (retentionDays > 0) {
                metadata.setExpirationTime(new Date(System.currentTimeMillis() + 
                        TimeUnit.DAYS.toMillis(retentionDays)));
            }
            
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, objectKey, metadata);
            
            // Make reports publicly accessible if configured
            if (config.getBoolean(ObjectStorageKeys.REPORT_STORAGE_PUBLIC_ACCESS)) {
                initRequest.withCannedACL(CannedAccessControlList.PublicRead);
            }
            
            InitiateMultipartUploadResult initResponse = executeWithRetry(
                    () -> s3Client.initiateMultipartUpload(initRequest), "initiateMultipartUpload");
            
            String uploadId = initResponse.getUploadId();
            LOGGER.debug("Initiated multipart upload for {}, uploadId: {}", objectKey, uploadId);
            
            // Upload parts
            List<PartETag> partETags = new ArrayList<>();
            long filePosition = 0;
            long partSize = DEFAULT_PART_SIZE;
            long fileLength = data.length;
            
            for (int partNumber = 1; filePosition < fileLength; partNumber++) {
                long lastByte = Math.min(filePosition + partSize - 1, fileLength - 1);
                long partLength = lastByte - filePosition + 1;
                
                Span partSpan = tracer.spanBuilder("ObjectStorageClient.uploadPart").startSpan();
                partSpan.setAttribute("partNumber", partNumber);
                partSpan.setAttribute("partSize", partLength);
                
                try (Scope partScope = partSpan.makeCurrent()) {
                    byte[] partData = new byte[(int) partLength];
                    System.arraycopy(data, (int) filePosition, partData, 0, (int) partLength);
                    
                    UploadPartRequest uploadPartRequest = new UploadPartRequest()
                            .withBucketName(bucketName)
                            .withKey(objectKey)
                            .withUploadId(uploadId)
                            .withPartNumber(partNumber)
                            .withPartSize(partLength)
                            .withInputStream(new ByteArrayInputStream(partData));
                    
                    UploadPartResult uploadPartResult = executeWithRetry(
                            () -> s3Client.uploadPart(uploadPartRequest), "uploadPart");
                    
                    partETags.add(new PartETag(partNumber, uploadPartResult.getETag()));
                    LOGGER.debug("Uploaded part {} for {}, ETag: {}", partNumber, objectKey, uploadPartResult.getETag());
                } catch (Exception e) {
                    LOGGER.error("Error uploading part {} for {}: {}", partNumber, objectKey, e.getMessage(), e);
                    partSpan.recordException(e);
                    throw e;
                } finally {
                    partSpan.end();
                    filePosition += partLength;
                }
            }
            
            // Complete multipart upload
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    bucketName, objectKey, uploadId, partETags);
            
            CompleteMultipartUploadResult completeResult = executeWithRetry(
                    () -> s3Client.completeMultipartUpload(completeRequest), "completeMultipartUpload");
            
            LOGGER.debug("Completed multipart upload for {}, ETag: {}", objectKey, completeResult.getETag());
        } catch (Exception e) {
            LOGGER.error("Error in multipart upload for {}: {}", objectKey, e.getMessage(), e);
            span.recordException(e);
            throw new IOException("Failed to complete multipart upload", e);
        } finally {
            span.end();
        }
    }

    /**
     * Generates a signed URL for accessing a report file.
     *
     * @param objectKey The object key of the report
     * @param expirationHours The number of hours until the URL expires (default: 24)
     * @return A pre-signed URL for accessing the report
     */
    public URL generateSignedUrl(String objectKey, int expirationHours) {
        Span span = tracer.spanBuilder("ObjectStorageClient.generateSignedUrl").startSpan();
        span.setAttribute("objectKey", objectKey);
        span.setAttribute("expirationHours", expirationHours);
        
        try (Scope scope = span.makeCurrent()) {
            Date expiration = new Date();
            long expTimeMillis = expiration.getTime();
            expTimeMillis += TimeUnit.HOURS.toMillis(expirationHours > 0 ? expirationHours : DEFAULT_URL_EXPIRATION_HOURS);
            expiration.setTime(expTimeMillis);
            
            GeneratePresignedUrlRequest generatePresignedUrlRequest = new GeneratePresignedUrlRequest(bucketName, objectKey)
                    .withMethod(com.amazonaws.HttpMethod.GET)
                    .withExpiration(expiration);
            
            URL url = executeWithRetry(() -> s3Client.generatePresignedUrl(generatePresignedUrlRequest), "generatePresignedUrl");
            LOGGER.debug("Generated signed URL for {} with expiration {}", objectKey, expiration);
            return url;
        } catch (Exception e) {
            LOGGER.error("Error generating signed URL for {}: {}", objectKey, e.getMessage(), e);
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Deletes a report file from the object storage.
     *
     * @param objectKey The object key of the report to delete
     */
    public void deleteReport(String objectKey) {
        Span span = tracer.spanBuilder("ObjectStorageClient.deleteReport").startSpan();
        span.setAttribute("objectKey", objectKey);
        
        try (Scope scope = span.makeCurrent()) {
            executeWithRetry(() -> {
                s3Client.deleteObject(bucketName, objectKey);
                return null;
            }, "deleteObject");
            LOGGER.debug("Deleted report: {}", objectKey);
        } catch (Exception e) {
            LOGGER.error("Error deleting report {}: {}", objectKey, e.getMessage(), e);
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Uploads a report file from a local file.
     *
     * @param reportId The unique identifier for the report
     * @param file The local file containing the report data
     * @param contentType The content type of the report
     * @return The object key of the uploaded report
     * @throws IOException If an I/O error occurs during upload
     */
    public String uploadReportFromFile(String reportId, File file, String contentType) throws IOException {
        Span span = tracer.spanBuilder("ObjectStorageClient.uploadReportFromFile").startSpan();
        span.setAttribute("reportId", reportId);
        span.setAttribute("fileName", file.getName());
        span.setAttribute("fileSize", file.length());
        span.setAttribute("contentType", contentType);
        
        String objectKey = generateObjectKey(reportId);
        
        try (Scope scope = span.makeCurrent()) {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(file.length());
            metadata.setContentType(contentType);
            
            // Set retention period if configured
            int retentionDays = config.getInteger(ObjectStorageKeys.REPORT_STORAGE_RETENTION_DAYS);
            if (retentionDays > 0) {
                metadata.setExpirationTime(new Date(System.currentTimeMillis() + 
                        TimeUnit.DAYS.toMillis(retentionDays)));
            }
            
            PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, file)
                    .withMetadata(metadata);
            
            // Make reports publicly accessible if configured
            if (config.getBoolean(ObjectStorageKeys.REPORT_STORAGE_PUBLIC_ACCESS)) {
                request.withCannedAcl(CannedAccessControlList.PublicRead);
            }
            
            PutObjectResult result = executeWithRetry(() -> s3Client.putObject(request), "putObject");
            LOGGER.debug("Report uploaded successfully from file: {}, ETag: {}", objectKey, result.getETag());
            return objectKey;
        } catch (Exception e) {
            LOGGER.error("Error uploading report from file: {}", e.getMessage(), e);
            span.recordException(e);
            throw new IOException("Failed to upload report from file to object storage", e);
        } finally {
            span.end();
        }
    }

    /**
     * Downloads a report file from the object storage.
     *
     * @param objectKey The object key of the report to download
     * @return The report data as a byte array
     * @throws IOException If an I/O error occurs during download
     */
    public byte[] downloadReport(String objectKey) throws IOException {
        Span span = tracer.spanBuilder("ObjectStorageClient.downloadReport").startSpan();
        span.setAttribute("objectKey", objectKey);
        
        try (Scope scope = span.makeCurrent()) {
            return executeWithRetry(() -> {
                try (InputStream is = s3Client.getObject(bucketName, objectKey).getObjectContent()) {
                    return is.readAllBytes();
                }
            }, "getObject");
        } catch (Exception e) {
            LOGGER.error("Error downloading report {}: {}", objectKey, e.getMessage(), e);
            span.recordException(e);
            throw new IOException("Failed to download report from object storage", e);
        } finally {
            span.end();
        }
    }

    /**
     * Generates a unique object key for a report.
     *
     * @param reportId The report identifier
     * @return A unique object key
     */
    private String generateObjectKey(String reportId) {
        String prefix = config.getString(ObjectStorageKeys.REPORT_STORAGE_KEY_PREFIX);
        return prefix + reportId;
    }

    /**
     * Executes an operation with retry logic.
     *
     * @param operation The operation to execute
     * @param operationName The name of the operation for logging and tracing
     * @param <T> The return type of the operation
     * @return The result of the operation
     */
    private <T> T executeWithRetry(RetryableOperation<T> operation, String operationName) {
        Span span = tracer.spanBuilder("ObjectStorageClient.executeWithRetry").startSpan();
        span.setAttribute("operation", operationName);
        
        try (Scope scope = span.makeCurrent()) {
            int maxRetries = config.getInteger(ObjectStorageKeys.REPORT_STORAGE_MAX_RETRIES);
            int retryDelayMs = config.getInteger(ObjectStorageKeys.REPORT_STORAGE_RETRY_DELAY_MS);
            
            Exception lastException = null;
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {
                    if (attempt > 0) {
                        LOGGER.debug("Retry attempt {} for operation {}", attempt, operationName);
                        Thread.sleep(retryDelayMs * attempt); // Exponential backoff
                    }
                    return operation.execute();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Operation interrupted", e);
                } catch (Exception e) {
                    lastException = e;
                    LOGGER.warn("Operation {} failed on attempt {}: {}", operationName, attempt, e.getMessage());
                    if (attempt >= maxRetries) {
                        LOGGER.error("Operation {} failed after {} attempts", operationName, maxRetries + 1);
                        break;
                    }
                }
            }
            throw new RuntimeException("Operation " + operationName + " failed after retries", lastException);
        } finally {
            span.end();
        }
    }

    /**
     * Functional interface for operations that can be retried.
     *
     * @param <T> The return type of the operation
     */
    @FunctionalInterface
    private interface RetryableOperation<T> {
        T execute() throws Exception;
    }
}