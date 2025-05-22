/*
 * Copyright 2017 - 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.database;

import io.netty.buffer.ByteBuf;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Singleton
public class MediaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MediaManager.class);
    
    // OpenTelemetry instrumentation
    private static final String INSTRUMENTATION_NAME = "org.traccar.database.MediaManager";
    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
    private static final Meter METER = GlobalOpenTelemetry.getMeter(INSTRUMENTATION_NAME);
    
    // Metrics
    private final LongCounter writeOperationsCounter;
    private final LongCounter writeFailuresCounter;
    private final LongCounter bytesWrittenCounter;
    
    // Attribute keys for spans and metrics
    private static final AttributeKey<String> STORAGE_TYPE_KEY = AttributeKey.stringKey("storage.type");
    private static final AttributeKey<String> OPERATION_KEY = AttributeKey.stringKey("operation");
    private static final AttributeKey<String> UNIQUE_ID_KEY = AttributeKey.stringKey("uniqueId");
    private static final AttributeKey<String> FILE_NAME_KEY = AttributeKey.stringKey("fileName");
    private static final AttributeKey<String> FILE_EXTENSION_KEY = AttributeKey.stringKey("fileExtension");
    private static final AttributeKey<Long> FILE_SIZE_KEY = AttributeKey.longKey("fileSize");
    
    // Circuit breaker for media operations
    private final CircuitBreaker circuitBreaker;
    
    // Retry policy with exponential backoff
    private final Retry retry;
    
    private final String path;
    private final boolean useS3Storage;
    private final String s3Bucket;
    private final String s3Endpoint;
    private final String s3Region;
    private final String s3AccessKey;
    private final String s3SecretKey;
    private S3Client s3Client;
    
    @Inject
    public MediaManager(Config config) {
        this.path = config.getString(Keys.MEDIA_PATH);
        this.useS3Storage = config.getBoolean(Keys.MEDIA_S3_ENABLED, false);
        this.s3Bucket = config.getString(Keys.MEDIA_S3_BUCKET, "traccar-media");
        this.s3Endpoint = config.getString(Keys.MEDIA_S3_ENDPOINT);
        this.s3Region = config.getString(Keys.MEDIA_S3_REGION, "us-east-1");
        this.s3AccessKey = config.getString(Keys.MEDIA_S3_ACCESS_KEY);
        this.s3SecretKey = config.getString(Keys.MEDIA_S3_SECRET_KEY);
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofMinutes(1)) // Wait 1 minute in OPEN state before transitioning to HALF_OPEN
                .permittedNumberOfCallsInHalfOpenState(5) // Allow 5 calls in HALF_OPEN state
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Count failures over the last 10 calls
                .build();
        this.circuitBreaker = CircuitBreaker.of("mediaOperations", circuitBreakerConfig);
        
        // Initialize retry policy
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3) // Maximum 3 attempts
                .waitDuration(Duration.ofMillis(500)) // Initial wait duration
                .retryExceptions(IOException.class) // Retry on IOException
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5)) // Exponential backoff from 500ms to 5s
                .build();
        this.retry = Retry.of("mediaOperations", retryConfig);
        
        // Initialize metrics
        this.writeOperationsCounter = METER.counterBuilder("media.write.operations")
                .setDescription("Number of media write operations")
                .build();
        
        this.writeFailuresCounter = METER.counterBuilder("media.write.failures")
                .setDescription("Number of failed media write operations")
                .build();
        
        this.bytesWrittenCounter = METER.counterBuilder("media.bytes.written")
                .setDescription("Number of bytes written to media storage")
                .build();
    }

    @PostConstruct
    public void init() {
        if (useS3Storage) {
            LOGGER.info("Initializing S3 client for media storage");
            try {
                S3Client.Builder builder = S3Client.builder();
                
                if (s3Endpoint != null && !s3Endpoint.isEmpty()) {
                    builder.endpointOverride(URI.create(s3Endpoint));
                    // Use path-style access for compatibility with most S3-compatible storage solutions
                    builder.forcePathStyle(true);
                }
                
                if (s3Region != null && !s3Region.isEmpty()) {
                    builder.region(Region.of(s3Region));
                }
                
                if (s3AccessKey != null && !s3AccessKey.isEmpty() && s3SecretKey != null && !s3SecretKey.isEmpty()) {
                    builder.credentialsProvider(StaticCredentialsProvider.create(
                            AwsBasicCredentials.create(s3AccessKey, s3SecretKey)));
                }
                
                s3Client = builder.build();
                
                // Ensure bucket exists
                if (!bucketExists(s3Bucket)) {
                    createBucket(s3Bucket);
                }
                
                LOGGER.info("S3 client initialized successfully");
            } catch (Exception e) {
                LOGGER.error("Failed to initialize S3 client", e);
                throw new RuntimeException("Failed to initialize S3 client", e);
            }
        } else {
            LOGGER.info("Using local filesystem for media storage at: {}", path);
            // Ensure the base media directory exists
            if (path != null) {
                try {
                    Files.createDirectories(Paths.get(path));
                } catch (IOException e) {
                    LOGGER.error("Failed to create media directory", e);
                    throw new RuntimeException("Failed to create media directory", e);
                }
            }
        }
    }
    
    @PreDestroy
    public void shutdown() {
        if (s3Client != null) {
            LOGGER.info("Closing S3 client");
            s3Client.close();
        }
    }
    
    private boolean bucketExists(String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder().bucket(bucketName).build());
            return true;
        } catch (NoSuchBucketException e) {
            return false;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }
    
    private void createBucket(String bucketName) {
        LOGGER.info("Creating S3 bucket: {}", bucketName);
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }
    
    private File createFile(String uniqueId, String name) throws IOException {
        Span span = TRACER.spanBuilder("createFile").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute(UNIQUE_ID_KEY, uniqueId);
            span.setAttribute(FILE_NAME_KEY, name);
            span.setAttribute(STORAGE_TYPE_KEY, "local");
            
            Path filePath = Paths.get(path, uniqueId, name);
            Path directoryPath = filePath.getParent();
            if (directoryPath != null) {
                Files.createDirectories(directoryPath);
            }
            return filePath.toFile();
        } catch (IOException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }

    public OutputStream createFileStream(String uniqueId, String name, String extension) throws IOException {
        Span span = TRACER.spanBuilder("createFileStream").startSpan();
        try (var scope = span.makeCurrent()) {
            span.setAttribute(UNIQUE_ID_KEY, uniqueId);
            span.setAttribute(FILE_NAME_KEY, name);
            span.setAttribute(FILE_EXTENSION_KEY, extension);
            
            if (useS3Storage) {
                span.setAttribute(STORAGE_TYPE_KEY, "s3");
                // For S3, we'll return a special output stream that buffers data and uploads to S3 when closed
                // This is a placeholder - in a real implementation, you'd use a proper S3 output stream
                throw new UnsupportedOperationException("Direct streaming to S3 is not supported. Use writeFile instead.");
            } else {
                span.setAttribute(STORAGE_TYPE_KEY, "local");
                return Retry.decorateCheckedSupplier(retry, () -> 
                    new FileOutputStream(createFile(uniqueId, name + "." + extension))
                ).get();
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            if (e instanceof IOException) {
                throw (IOException) e;
            } else {
                throw new IOException("Failed to create file stream", e);
            }
        } finally {
            span.end();
        }
    }

    public String writeFile(String uniqueId, ByteBuf buf, String extension) {
        Span span = TRACER.spanBuilder("writeFile").startSpan();
        Context context = Context.current().with(span);
        
        try (var scope = context.makeCurrent()) {
            int size = buf.readableBytes();
            String name = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()) + "." + extension;
            
            span.setAttribute(UNIQUE_ID_KEY, uniqueId);
            span.setAttribute(FILE_NAME_KEY, name);
            span.setAttribute(FILE_EXTENSION_KEY, extension);
            span.setAttribute(FILE_SIZE_KEY, (long) size);
            
            // Use circuit breaker pattern to prevent cascading failures
            return circuitBreaker.executeSupplier(() -> {
                try {
                    // Record metrics for the operation
                    writeOperationsCounter.add(1, Attributes.of(
                            STORAGE_TYPE_KEY, useS3Storage ? "s3" : "local",
                            FILE_EXTENSION_KEY, extension));
                    
                    if (useS3Storage) {
                        span.setAttribute(STORAGE_TYPE_KEY, "s3");
                        return writeToS3(uniqueId, buf, name, size, span);
                    } else if (path != null) {
                        span.setAttribute(STORAGE_TYPE_KEY, "local");
                        return writeToLocalFile(uniqueId, buf, name, size, span);
                    }
                    return null;
                } catch (Exception e) {
                    // Record metrics for failures
                    writeFailuresCounter.add(1, Attributes.of(
                            STORAGE_TYPE_KEY, useS3Storage ? "s3" : "local",
                            FILE_EXTENSION_KEY, extension));
                    
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.warn("Save media file error", e);
                    return null;
                }
            });
        } finally {
            span.end();
        }
    }
    
    private String writeToLocalFile(String uniqueId, ByteBuf buf, String name, int size, Span parentSpan) {
        Span span = TRACER.spanBuilder("writeToLocalFile")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            // Use retry pattern with exponential backoff
            return Retry.decorateCheckedSupplier(retry, () -> {
                try (FileOutputStream output = new FileOutputStream(createFile(uniqueId, name));
                     FileChannel fileChannel = output.getChannel()) {
                    
                    ByteBuffer byteBuffer = buf.nioBuffer();
                    int written = 0;
                    while (written < size) {
                        written += fileChannel.write(byteBuffer);
                    }
                    fileChannel.force(false);
                    
                    // Record metrics for bytes written
                    bytesWrittenCounter.add(written, Attributes.of(
                            STORAGE_TYPE_KEY, "local",
                            FILE_EXTENSION_KEY, name.substring(name.lastIndexOf('.') + 1)));
                    
                    return name;
                }
            }).get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.warn("Save media file to local storage error", e);
            return null;
        } finally {
            span.end();
        }
    }
    
    private String writeToS3(String uniqueId, ByteBuf buf, String name, int size, Span parentSpan) {
        Span span = TRACER.spanBuilder("writeToS3")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            // Use retry pattern with exponential backoff
            return Retry.decorateCheckedSupplier(retry, () -> {
                String key = uniqueId + "/" + name;
                ByteBuffer byteBuffer = buf.nioBuffer();
                
                // Upload to S3
                PutObjectResponse response = s3Client.putObject(
                        PutObjectRequest.builder()
                                .bucket(s3Bucket)
                                .key(key)
                                .contentLength((long) size)
                                .build(),
                        RequestBody.fromByteBuffer(byteBuffer));
                
                // Record metrics for bytes written
                bytesWrittenCounter.add(size, Attributes.of(
                        STORAGE_TYPE_KEY, "s3",
                        FILE_EXTENSION_KEY, name.substring(name.lastIndexOf('.') + 1)));
                
                LOGGER.debug("File uploaded to S3: {}/{}", s3Bucket, key);
                return name;
            }).get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.warn("Save media file to S3 error", e);
            return null;
        } finally {
            span.end();
        }
    }

    /**
     * Reads a media file from storage
     * 
     * @param uniqueId Device unique identifier
     * @param name File name
     * @return InputStream for the file or null if not found
     */
    public InputStream readFile(String uniqueId, String name) {
        Span span = TRACER.spanBuilder("readFile").startSpan();
        
        try (var scope = span.makeCurrent()) {
            span.setAttribute(UNIQUE_ID_KEY, uniqueId);
            span.setAttribute(FILE_NAME_KEY, name);
            
            // Use circuit breaker pattern to prevent cascading failures
            return circuitBreaker.executeSupplier(() -> {
                try {
                    if (useS3Storage) {
                        span.setAttribute(STORAGE_TYPE_KEY, "s3");
                        return readFromS3(uniqueId, name, span);
                    } else if (path != null) {
                        span.setAttribute(STORAGE_TYPE_KEY, "local");
                        return readFromLocalFile(uniqueId, name, span);
                    }
                    return null;
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.warn("Read media file error", e);
                    return null;
                }
            });
        } finally {
            span.end();
        }
    }
    
    private InputStream readFromLocalFile(String uniqueId, String name, Span parentSpan) {
        Span span = TRACER.spanBuilder("readFromLocalFile")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            // Use retry pattern with exponential backoff
            return Retry.decorateCheckedSupplier(retry, () -> {
                Path filePath = Paths.get(path, uniqueId, name);
                if (Files.exists(filePath)) {
                    return Files.newInputStream(filePath);
                }
                return null;
            }).get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.warn("Read media file from local storage error", e);
            return null;
        } finally {
            span.end();
        }
    }
    
    private InputStream readFromS3(String uniqueId, String name, Span parentSpan) {
        Span span = TRACER.spanBuilder("readFromS3")
                .setParent(Context.current().with(parentSpan))
                .startSpan();
        
        try (var scope = span.makeCurrent()) {
            // Use retry pattern with exponential backoff
            return Retry.decorateCheckedSupplier(retry, () -> {
                String key = uniqueId + "/" + name;
                
                // Check if object exists
                try {
                    s3Client.headObject(HeadObjectRequest.builder()
                            .bucket(s3Bucket)
                            .key(key)
                            .build());
                } catch (NoSuchKeyException e) {
                    LOGGER.debug("File not found in S3: {}/{}", s3Bucket, key);
                    return null;
                }
                
                // Get object
                GetObjectResponse response = s3Client.getObject(GetObjectRequest.builder()
                        .bucket(s3Bucket)
                        .key(key)
                        .build());
                
                LOGGER.debug("File downloaded from S3: {}/{}", s3Bucket, key);
                return response;
            }).get();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.warn("Read media file from S3 error", e);
            return null;
        } finally {
            span.end();
        }
    }
    
    /**
     * Checks if a media file exists in storage
     * 
     * @param uniqueId Device unique identifier
     * @param name File name
     * @return true if file exists, false otherwise
     */
    public boolean fileExists(String uniqueId, String name) {
        Span span = TRACER.spanBuilder("fileExists").startSpan();
        
        try (var scope = span.makeCurrent()) {
            span.setAttribute(UNIQUE_ID_KEY, uniqueId);
            span.setAttribute(FILE_NAME_KEY, name);
            
            // Use circuit breaker pattern to prevent cascading failures
            return circuitBreaker.executeSupplier(() -> {
                try {
                    if (useS3Storage) {
                        span.setAttribute(STORAGE_TYPE_KEY, "s3");
                        String key = uniqueId + "/" + name;
                        
                        try {
                            s3Client.headObject(HeadObjectRequest.builder()
                                    .bucket(s3Bucket)
                                    .key(key)
                                    .build());
                            return true;
                        } catch (NoSuchKeyException e) {
                            return false;
                        }
                    } else if (path != null) {
                        span.setAttribute(STORAGE_TYPE_KEY, "local");
                        Path filePath = Paths.get(path, uniqueId, name);
                        return Files.exists(filePath);
                    }
                    return false;
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.warn("Check media file existence error", e);
                    return false;
                }
            });
        } finally {
            span.end();
        }
    }
    
    /**
     * Deletes a media file from storage
     * 
     * @param uniqueId Device unique identifier
     * @param name File name
     * @return true if file was deleted, false otherwise
     */
    public boolean deleteFile(String uniqueId, String name) {
        Span span = TRACER.spanBuilder("deleteFile").startSpan();
        
        try (var scope = span.makeCurrent()) {
            span.setAttribute(UNIQUE_ID_KEY, uniqueId);
            span.setAttribute(FILE_NAME_KEY, name);
            
            // Use circuit breaker pattern to prevent cascading failures
            return circuitBreaker.executeSupplier(() -> {
                try {
                    if (useS3Storage) {
                        span.setAttribute(STORAGE_TYPE_KEY, "s3");
                        String key = uniqueId + "/" + name;
                        
                        s3Client.deleteObject(DeleteObjectRequest.builder()
                                .bucket(s3Bucket)
                                .key(key)
                                .build());
                        
                        LOGGER.debug("File deleted from S3: {}/{}", s3Bucket, key);
                        return true;
                    } else if (path != null) {
                        span.setAttribute(STORAGE_TYPE_KEY, "local");
                        Path filePath = Paths.get(path, uniqueId, name);
                        return Files.deleteIfExists(filePath);
                    }
                    return false;
                } catch (Exception e) {
                    span.recordException(e);
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    LOGGER.warn("Delete media file error", e);
                    return false;
                }
            });
        } finally {
            span.end();
        }
    }
}