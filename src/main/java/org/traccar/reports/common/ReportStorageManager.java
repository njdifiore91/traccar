/*
 * Copyright 2023 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.observability.TracerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages storage and retrieval of reports in object storage (S3, Azure Blob, etc.)
 */
@Singleton
public class ReportStorageManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReportStorageManager.class);
    private static final String SPAN_NAME_STORE = "objectstorage.store";
    private static final String SPAN_NAME_RETRIEVE = "objectstorage.retrieve";

    private final Config config;
    private final Tracer tracer;
    private final ObjectStorageClient storageClient;

    /**
     * Constructs a new ReportStorageManager.
     *
     * @param config Configuration provider
     * @param tracerFactory Factory for creating OpenTelemetry tracers
     * @throws IOException If storage client initialization fails
     */
    @Inject
    public ReportStorageManager(Config config, TracerFactory tracerFactory) throws IOException {
        this.config = config;
        this.tracer = tracerFactory.getTracer(ReportStorageManager.class.getName());
        this.storageClient = createStorageClient();
    }

    /**
     * Creates and initializes the appropriate object storage client based on configuration.
     *
     * @return Initialized ObjectStorageClient
     * @throws IOException If client initialization fails
     */
    private ObjectStorageClient createStorageClient() throws IOException {
        String storageType = config.getString(Keys.REPORTS_STORAGE_TYPE);
        String endpoint = config.getString(Keys.REPORTS_STORAGE_ENDPOINT);
        String bucket = config.getString(Keys.REPORTS_STORAGE_BUCKET);
        String accessKey = config.getString(Keys.REPORTS_STORAGE_ACCESS_KEY);
        String secretKey = config.getString(Keys.REPORTS_STORAGE_SECRET_KEY);
        String region = config.getString(Keys.REPORTS_STORAGE_REGION);

        if (storageType == null || storageType.isEmpty()) {
            LOGGER.info("Object storage not configured, reports will not be stored");
            return new NoOpObjectStorageClient();
        }

        try {
            URI endpointUri = new URI(endpoint);
            Map<String, String> clientConfig = new HashMap<>();
            clientConfig.put("endpoint", endpoint);
            clientConfig.put("bucket", bucket);
            clientConfig.put("accessKey", accessKey);
            clientConfig.put("secretKey", secretKey);
            clientConfig.put("region", region);

            switch (storageType.toLowerCase()) {
                case "s3":
                    return new S3ObjectStorageClient(clientConfig);
                case "azure":
                    return new AzureBlobObjectStorageClient(clientConfig);
                case "gcs":
                    return new GcsObjectStorageClient(clientConfig);
                default:
                    LOGGER.warn("Unsupported storage type: {}, using no-op client", storageType);
                    return new NoOpObjectStorageClient();
            }
        } catch (URISyntaxException e) {
            LOGGER.error("Invalid storage endpoint URI: {}", endpoint, e);
            throw new IOException("Invalid storage endpoint URI", e);
        }
    }

    /**
     * Stores a report in object storage.
     *
     * @param objectKey The key/path where the report should be stored
     * @param reportData The report data as byte array
     * @throws IOException If storing the report fails
     */
    public void storeReport(String objectKey, byte[] reportData) throws IOException {
        Span span = tracer.spanBuilder(SPAN_NAME_STORE)
                .setParent(Context.current())
                .setAttribute("storage.operation", "store")
                .setAttribute("storage.key", objectKey)
                .setAttribute("storage.size", reportData.length)
                .startSpan();

        try (var scope = span.makeCurrent(); 
             InputStream inputStream = new ByteArrayInputStream(reportData)) {
            
            storageClient.storeObject(objectKey, inputStream, reportData.length);
            span.setStatus(StatusCode.OK);
            LOGGER.debug("Stored report at {}, size: {} bytes", objectKey, reportData.length);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Failed to store report at {}", objectKey, e);
            throw new IOException("Failed to store report", e);
        } finally {
            span.end();
        }
    }

    /**
     * Retrieves a report from object storage.
     *
     * @param objectKey The key/path where the report is stored
     * @return The report data as byte array
     * @throws IOException If retrieving the report fails
     */
    public byte[] retrieveReport(String objectKey) throws IOException {
        Span span = tracer.spanBuilder(SPAN_NAME_RETRIEVE)
                .setParent(Context.current())
                .setAttribute("storage.operation", "retrieve")
                .setAttribute("storage.key", objectKey)
                .startSpan();

        try (var scope = span.makeCurrent()) {
            byte[] data = storageClient.retrieveObject(objectKey);
            span.setAttribute("storage.size", data.length);
            span.setStatus(StatusCode.OK);
            LOGGER.debug("Retrieved report from {}, size: {} bytes", objectKey, data.length);
            return data;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Failed to retrieve report from {}", objectKey, e);
            throw new IOException("Failed to retrieve report", e);
        } finally {
            span.end();
        }
    }

    /**
     * Interface for object storage client implementations.
     */
    private interface ObjectStorageClient {
        void storeObject(String objectKey, InputStream data, long length) throws IOException;
        byte[] retrieveObject(String objectKey) throws IOException;
    }

    /**
     * S3-compatible object storage client implementation.
     */
    private static class S3ObjectStorageClient implements ObjectStorageClient {
        private final Map<String, String> config;

        public S3ObjectStorageClient(Map<String, String> config) {
            this.config = config;
            // In a real implementation, this would initialize the S3 client
            // using the AWS SDK for Java or a compatible library
        }

        @Override
        public void storeObject(String objectKey, InputStream data, long length) throws IOException {
            // Implementation would use S3Client to put the object
            // Example: s3Client.putObject(request -> request.bucket(bucket).key(objectKey), 
            //                            RequestBody.fromInputStream(data, length));
        }

        @Override
        public byte[] retrieveObject(String objectKey) throws IOException {
            // Implementation would use S3Client to get the object
            // Example: ResponseBytes<GetObjectResponse> response = 
            //          s3Client.getObjectAsBytes(request -> request.bucket(bucket).key(objectKey));
            // return response.asByteArray();
            return new byte[0]; // Placeholder
        }
    }

    /**
     * Azure Blob Storage client implementation.
     */
    private static class AzureBlobObjectStorageClient implements ObjectStorageClient {
        private final Map<String, String> config;

        public AzureBlobObjectStorageClient(Map<String, String> config) {
            this.config = config;
            // In a real implementation, this would initialize the Azure Blob client
        }

        @Override
        public void storeObject(String objectKey, InputStream data, long length) throws IOException {
            // Implementation would use BlobClient to upload the blob
        }

        @Override
        public byte[] retrieveObject(String objectKey) throws IOException {
            // Implementation would use BlobClient to download the blob
            return new byte[0]; // Placeholder
        }
    }

    /**
     * Google Cloud Storage client implementation.
     */
    private static class GcsObjectStorageClient implements ObjectStorageClient {
        private final Map<String, String> config;

        public GcsObjectStorageClient(Map<String, String> config) {
            this.config = config;
            // In a real implementation, this would initialize the GCS client
        }

        @Override
        public void storeObject(String objectKey, InputStream data, long length) throws IOException {
            // Implementation would use Storage to create a blob
        }

        @Override
        public byte[] retrieveObject(String objectKey) throws IOException {
            // Implementation would use Storage to read a blob
            return new byte[0]; // Placeholder
        }
    }

    /**
     * No-op implementation for when object storage is not configured.
     */
    private static class NoOpObjectStorageClient implements ObjectStorageClient {
        @Override
        public void storeObject(String objectKey, InputStream data, long length) {
            // Do nothing
            LOGGER.debug("No-op storage: ignoring store request for {}", objectKey);
        }

        @Override
        public byte[] retrieveObject(String objectKey) throws IOException {
            LOGGER.debug("No-op storage: ignoring retrieve request for {}", objectKey);
            throw new IOException("Object storage not configured");
        }
    }
}