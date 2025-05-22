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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;

/**
 * Implementation of ObjectStorage using S3-compatible storage.
 */
@Singleton
public class S3ObjectStorage implements ObjectStorage {

    private final String bucketName;
    private final S3Client s3Client;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    private final CircuitBreaker s3CircuitBreaker;

    /**
     * Constructor with all required dependencies.
     *
     * @param config Configuration
     * @param tracer OpenTelemetry tracer
     * @param meterRegistry Metrics registry
     */
    @Inject
    public S3ObjectStorage(Config config, Tracer tracer, MeterRegistry meterRegistry) {
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Get S3 configuration from environment or config
        String endpoint = config.getString("storage.s3.endpoint");
        String accessKey = config.getString("storage.s3.accessKey");
        String secretKey = config.getString("storage.s3.secretKey");
        String region = config.getString("storage.s3.region", "us-east-1");
        this.bucketName = config.getString("storage.s3.bucket", "traccar");
        boolean pathStyleAccess = config.getBoolean("storage.s3.pathStyleAccess", true);

        // Create S3 client
        S3Client.Builder builder = S3Client.builder()
                .region(Region.of(region));

        if (endpoint != null && !endpoint.isEmpty()) {
            builder.endpointOverride(URI.create(endpoint));
        }

        if (accessKey != null && !accessKey.isEmpty() && secretKey != null && !secretKey.isEmpty()) {
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(accessKey, secretKey)));
        }

        if (pathStyleAccess) {
            builder.forcePathStyle(true);
        }

        this.s3Client = builder.build();

        // Initialize circuit breaker for S3 operations
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(5)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.s3CircuitBreaker = circuitBreakerRegistry.circuitBreaker("s3");

        // Register circuit breaker events for monitoring
        s3CircuitBreaker.getEventPublisher()
                .onStateTransition(event -> {
                    Span span = tracer.spanBuilder("CircuitBreaker.stateTransition")
                            .setSpanKind(SpanKind.INTERNAL)
                            .startSpan();
                    try (Scope scope = span.makeCurrent()) {
                        span.setAttribute("circuitBreaker.name", event.getCircuitBreakerName());
                        span.setAttribute("circuitBreaker.fromState", event.getStateTransition().getFromState().name());
                        span.setAttribute("circuitBreaker.toState", event.getStateTransition().getToState().name());
                    } finally {
                        span.end();
                    }
                });
    }

    @Override
    public boolean exists(String path) throws IOException {
        Span span = tracer.spanBuilder("S3ObjectStorage.exists")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("path", path);
            span.setAttribute("bucket", bucketName);
            Timer.Sample sample = Timer.start(meterRegistry);

            boolean result = s3CircuitBreaker.executeSupplier(() -> {
                try {
                    s3Client.headObject(HeadObjectRequest.builder()
                            .bucket(bucketName)
                            .key(path)
                            .build());
                    return true;
                } catch (NoSuchKeyException e) {
                    return false;
                }
            });

            sample.stop(meterRegistry.timer("storage.s3.exists",
                    "circuitBreaker.state", s3CircuitBreaker.getState().name()));
            return result;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            meterRegistry.counter("storage.s3.failures", "operation", "exists").increment();
            throw new IOException("Failed to check if object exists: " + path, e);
        } finally {
            span.end();
        }
    }

    @Override
    public InputStream getObject(String path) throws IOException {
        Span span = tracer.spanBuilder("S3ObjectStorage.getObject")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("path", path);
            span.setAttribute("bucket", bucketName);
            Timer.Sample sample = Timer.start(meterRegistry);

            ResponseInputStream<?> s3Object = s3CircuitBreaker.executeSupplier(() ->
                    s3Client.getObject(GetObjectRequest.builder()
                            .bucket(bucketName)
                            .key(path)
                            .build()));

            // Copy to ByteArrayInputStream to avoid keeping the S3 connection open
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = s3Object.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            s3Object.close();

            sample.stop(meterRegistry.timer("storage.s3.getObject",
                    "circuitBreaker.state", s3CircuitBreaker.getState().name()));
            return new ByteArrayInputStream(baos.toByteArray());
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            meterRegistry.counter("storage.s3.failures", "operation", "getObject").increment();
            throw new IOException("Failed to get object: " + path, e);
        } finally {
            span.end();
        }
    }

    @Override
    public void putObject(String path, OutputStream outputStream) throws IOException {
        Span span = tracer.spanBuilder("S3ObjectStorage.putObject")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("path", path);
            span.setAttribute("bucket", bucketName);
            Timer.Sample sample = Timer.start(meterRegistry);

            // Convert OutputStream to byte array for S3 upload
            ByteArrayOutputStream baos;
            if (outputStream instanceof ByteArrayOutputStream) {
                baos = (ByteArrayOutputStream) outputStream;
            } else {
                throw new IOException("OutputStream must be a ByteArrayOutputStream");
            }

            byte[] bytes = baos.toByteArray();
            span.setAttribute("contentLength", bytes.length);

            s3CircuitBreaker.executeRunnable(() ->
                    s3Client.putObject(PutObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(path)
                                    .build(),
                            RequestBody.fromBytes(bytes)));

            sample.stop(meterRegistry.timer("storage.s3.putObject",
                    "circuitBreaker.state", s3CircuitBreaker.getState().name()));
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            meterRegistry.counter("storage.s3.failures", "operation", "putObject").increment();
            throw new IOException("Failed to put object: " + path, e);
        } finally {
            span.end();
        }
    }

    @Override
    public void deleteObject(String path) throws IOException {
        Span span = tracer.spanBuilder("S3ObjectStorage.deleteObject")
                .setSpanKind(SpanKind.CLIENT)
                .startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("path", path);
            span.setAttribute("bucket", bucketName);
            Timer.Sample sample = Timer.start(meterRegistry);

            s3CircuitBreaker.executeRunnable(() ->
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucketName)
                            .key(path)
                            .build()));

            sample.stop(meterRegistry.timer("storage.s3.deleteObject",
                    "circuitBreaker.state", s3CircuitBreaker.getState().name()));
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            meterRegistry.counter("storage.s3.failures", "operation", "deleteObject").increment();
            throw new IOException("Failed to delete object: " + path, e);
        } finally {
            span.end();
        }
    }
}