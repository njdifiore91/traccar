/*
 * Copyright 2022 Anton Tananaev (anton@traccar.org)
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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.GeneratePresignedUrlRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.HttpMethod;
import com.amazonaws.SdkClientException;
import com.amazonaws.AmazonServiceException;

import org.traccar.config.Config;

import org.traccar.config.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URL;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.UUID;

/**
 * Provides integration with S3-compatible object storage for storing generated reports.
 * Supports multiple storage providers (AWS S3, MinIO, etc.) through a common interface.
 */
@Singleton
public class ObjectStorageClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObjectStorageClient.class);

    // Configuration keys for object storage
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
    public static final ConfigKey REPORT_STORAGE_URL_EXPIRATION = new ConfigKey(
            "report.storage.urlExpiration", Long.class, 86400L); // Default 24 hours
    public static final ConfigKey REPORT_STORAGE_PUBLIC_READ = new ConfigKey(
            "report.storage.publicRead", Boolean.class, false);

    private final AmazonS3 s3Client;
    private final String bucketName;
    private final long urlExpirationSeconds;
    private final boolean publicReadAccess;

    /**
     * Initializes the ObjectStorageClient with configuration from the provided Config.
     *
     * @param config The application configuration
     */
    @Inject
    public ObjectStorageClient(Config config) {
        String endpoint = config.getString(REPORT_STORAGE_ENDPOINT.getKey());
        String region = config.getString(REPORT_STORAGE_REGION.getKey());
        String accessKey = config.getString(REPORT_STORAGE_ACCESS_KEY.getKey());
        String secretKey = config.getString(REPORT_STORAGE_SECRET_KEY.getKey());
        this.bucketName = config.getString(REPORT_STORAGE_BUCKET.getKey());
        this.urlExpirationSeconds = config.getLong(REPORT_STORAGE_URL_EXPIRATION.getKey(), 86400); // Default 24 hours
        this.publicReadAccess = config.getBoolean(REPORT_STORAGE_PUBLIC_READ.getKey());

        LOGGER.info("Initializing ObjectStorageClient with endpoint: {}, region: {}, bucket: {}", 
                endpoint, region, bucketName);

        // Configure client with path style access for compatibility with most S3-compatible storage providers
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType"); // Use V4 signing for better compatibility
        
        // Configure retry policy
        clientConfiguration.setMaxErrorRetry(3);
        clientConfiguration.setConnectionTimeout(10000); // 10 seconds
        clientConfiguration.setSocketTimeout(30000);     // 30 seconds
        
        AWSCredentials credentials = new BasicAWSCredentials(accessKey, secretKey);
        
        s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, region))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(credentials))
                .build();
        
        LOGGER.debug("S3 client initialized successfully");
        
        // Create bucket if it doesn't exist
        createBucketIfNotExists();
    }

    /**
     * Creates the configured bucket if it doesn't already exist.
     */
    private void createBucketIfNotExists() {
        try {
            if (!s3Client.doesBucketExistV2(bucketName)) {
                LOGGER.info("Creating bucket '{}' as it does not exist", bucketName);
                s3Client.createBucket(bucketName);
                LOGGER.info("Bucket '{}' created successfully", bucketName);
            } else {
                LOGGER.debug("Bucket '{}' already exists", bucketName);
            }
        } catch (AmazonServiceException e) {
            LOGGER.error("Error creating bucket: {}", e.getMessage(), e);
            throw new RuntimeException("Error creating bucket: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            LOGGER.error("Error connecting to storage provider: {}", e.getMessage(), e);
            throw new RuntimeException("Error connecting to storage provider: " + e.getMessage(), e);
        }
    }

    /**
     * Uploads a file to the object storage.
     *
     * @param key The object key (path and filename) in the storage
     * @param file The file to upload
     * @param contentType The content type of the file
     * @return The URL to access the uploaded file
     */
    public String uploadFile(String key, File file, String contentType) {
        try {
            LOGGER.debug("Uploading file to object storage with key: {}, contentType: {}", key, contentType);
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType(contentType);
            
            PutObjectRequest request = new PutObjectRequest(bucketName, key, file)
                    .withMetadata(metadata);
            
            if (publicReadAccess) {
                request.withCannedAcl(CannedAccessControlList.PublicRead);
                LOGGER.debug("Setting public read access for object: {}", key);
            }
            
            s3Client.putObject(request);
            LOGGER.info("Successfully uploaded file to object storage with key: {}", key);
            
            String signedUrl = generateSignedUrl(key, HttpMethod.GET);
            LOGGER.debug("Generated signed URL for key: {}", key);
            return signedUrl;
        } catch (AmazonServiceException e) {
            LOGGER.error("Error uploading file to storage: {}", e.getMessage(), e);
            throw new RuntimeException("Error uploading file to storage: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            LOGGER.error("Error connecting to storage provider: {}", e.getMessage(), e);
            throw new RuntimeException("Error connecting to storage provider: " + e.getMessage(), e);
        }
    }

    /**
     * Uploads data from an input stream to the object storage.
     *
     * @param key The object key (path and filename) in the storage
     * @param inputStream The input stream containing the data to upload
     * @param contentLength The length of the data in bytes
     * @param contentType The content type of the data
     * @return The URL to access the uploaded data
     */
    public String uploadStream(String key, InputStream inputStream, long contentLength, String contentType) {
        try {
            LOGGER.debug("Uploading stream to object storage with key: {}, contentLength: {}, contentType: {}", 
                    key, contentLength, contentType);
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(contentLength);
            metadata.setContentType(contentType);
            
            PutObjectRequest request = new PutObjectRequest(bucketName, key, inputStream, metadata);
            
            if (publicReadAccess) {
                request.withCannedAcl(CannedAccessControlList.PublicRead);
                LOGGER.debug("Setting public read access for object: {}", key);
            }
            
            s3Client.putObject(request);
            LOGGER.info("Successfully uploaded stream to object storage with key: {}", key);
            
            String signedUrl = generateSignedUrl(key, HttpMethod.GET);
            LOGGER.debug("Generated signed URL for key: {}", key);
            return signedUrl;
        } catch (AmazonServiceException e) {
            LOGGER.error("Error uploading stream to storage: {}", e.getMessage(), e);
            throw new RuntimeException("Error uploading stream to storage: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            LOGGER.error("Error connecting to storage provider: {}", e.getMessage(), e);
            throw new RuntimeException("Error connecting to storage provider: " + e.getMessage(), e);
        }
    }

    /**
     * Uploads a byte array to the object storage.
     *
     * @param key The object key (path and filename) in the storage
     * @param data The byte array containing the data to upload
     * @param contentType The content type of the data
     * @return The URL to access the uploaded data
     */
    public String uploadBytes(String key, byte[] data, String contentType) {
        try {
            LOGGER.debug("Uploading bytes to object storage with key: {}, size: {} bytes, contentType: {}", 
                    key, data.length, contentType);
            
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentLength(data.length);
            metadata.setContentType(contentType);
            
            ByteArrayInputStream inputStream = new ByteArrayInputStream(data);
            
            PutObjectRequest request = new PutObjectRequest(bucketName, key, inputStream, metadata);
            
            if (publicReadAccess) {
                request.withCannedAcl(CannedAccessControlList.PublicRead);
                LOGGER.debug("Setting public read access for object: {}", key);
            }
            
            s3Client.putObject(request);
            LOGGER.info("Successfully uploaded bytes to object storage with key: {}", key);
            
            String signedUrl = generateSignedUrl(key, HttpMethod.GET);
            LOGGER.debug("Generated signed URL for key: {}", key);
            return signedUrl;
        } catch (AmazonServiceException e) {
            LOGGER.error("Error uploading bytes to storage: {}", e.getMessage(), e);
            throw new RuntimeException("Error uploading bytes to storage: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            LOGGER.error("Error connecting to storage provider: {}", e.getMessage(), e);
            throw new RuntimeException("Error connecting to storage provider: " + e.getMessage(), e);
        }
    }

    /**
     * Generates a signed URL for accessing an object with the specified HTTP method.
     *
     * @param key The object key (path and filename) in the storage
     * @param method The HTTP method to allow (GET, PUT, etc.)
     * @return The signed URL string
     */
    /**
     * Generates a signed URL for accessing an object with the specified HTTP method.
     *
     * @param key The object key (path and filename) in the storage
     * @param method The HTTP method to allow (GET, PUT, etc.)
     * @return The signed URL string
     */
    public String generateSignedUrl(String key, HttpMethod method) {
        return generateSignedUrl(key, method, urlExpirationSeconds);
    }

    /**
     * Generates a signed URL for accessing an object with the specified HTTP method and custom expiration.
     *
     * @param key The object key (path and filename) in the storage
     * @param method The HTTP method to allow (GET, PUT, etc.)
     * @param expirationSeconds The number of seconds until the URL expires
     * @return The signed URL string
     */
    public String generateSignedUrl(String key, HttpMethod method, long expirationSeconds) {
        try {
            LOGGER.debug("Generating signed URL for key: {}, method: {}, expiration: {} seconds", 
                    key, method, expirationSeconds);
            
            Date expiration = new Date();
            expiration.setTime(expiration.getTime() + TimeUnit.SECONDS.toMillis(expirationSeconds));
            
            GeneratePresignedUrlRequest request = new GeneratePresignedUrlRequest(bucketName, key)
                    .withMethod(method)
                    .withExpiration(expiration);
            
            URL url = s3Client.generatePresignedUrl(request);
            LOGGER.debug("Generated signed URL for key: {}", key);
            return url.toString();
        } catch (AmazonServiceException e) {
            LOGGER.error("Error generating signed URL: {}", e.getMessage(), e);
            throw new RuntimeException("Error generating signed URL: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            LOGGER.error("Error connecting to storage provider: {}", e.getMessage(), e);
            throw new RuntimeException("Error connecting to storage provider: " + e.getMessage(), e);
        }
    }

    /**
     * Checks if an object exists in the storage.
     *
     * @param key The object key (path and filename) to check
     * @return true if the object exists, false otherwise
     */
    public boolean objectExists(String key) {
        try {
            LOGGER.debug("Checking if object exists with key: {}", key);
            boolean exists = s3Client.doesObjectExist(bucketName, key);
            LOGGER.debug("Object with key: {} exists: {}", key, exists);
            return exists;
        } catch (AmazonServiceException e) {
            LOGGER.error("Error checking if object exists: {}", e.getMessage(), e);
            throw new RuntimeException("Error checking if object exists: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            LOGGER.error("Error connecting to storage provider: {}", e.getMessage(), e);
            throw new RuntimeException("Error connecting to storage provider: " + e.getMessage(), e);
        }
    }

    /**
     * Deletes an object from the storage.
     *
     * @param key The object key (path and filename) to delete
     */
    public void deleteObject(String key) {
        try {
            LOGGER.debug("Deleting object with key: {}", key);
            s3Client.deleteObject(bucketName, key);
            LOGGER.info("Successfully deleted object with key: {}", key);
        } catch (AmazonServiceException e) {
            LOGGER.error("Error deleting object: {}", e.getMessage(), e);
            throw new RuntimeException("Error deleting object: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            LOGGER.error("Error connecting to storage provider: {}", e.getMessage(), e);
            throw new RuntimeException("Error connecting to storage provider: " + e.getMessage(), e);
        }
    }

    /**
     * Gets an object from the storage.
     *
     * @param key The object key (path and filename) to get
     * @return The S3Object containing the object data and metadata
     */
    public S3Object getObject(String key) {
        try {
            LOGGER.debug("Getting object with key: {}", key);
            S3Object object = s3Client.getObject(bucketName, key);
            LOGGER.debug("Successfully retrieved object with key: {}", key);
            return object;
        } catch (AmazonServiceException e) {
            LOGGER.error("Error getting object: {}", e.getMessage(), e);
            throw new RuntimeException("Error getting object: " + e.getMessage(), e);
        } catch (SdkClientException e) {
            LOGGER.error("Error connecting to storage provider: {}", e.getMessage(), e);
            throw new RuntimeException("Error connecting to storage provider: " + e.getMessage(), e);
        }
    }

    /**
     * Gets the underlying S3 client for advanced operations.
     *
     * @return The AmazonS3 client instance
     */
    public AmazonS3 getS3Client() {
        return s3Client;
    }

    /**
     * Gets the configured bucket name.
     *
     * @return The bucket name
     */
    public String getBucketName() {
        return bucketName;
    }
    
    /**
     * Generates a unique key for a report file.
     *
     * @param prefix The prefix for the key (e.g., "reports/csv/")
     * @param extension The file extension (e.g., ".csv")
     * @return A unique key for the report file
     */
    public String generateUniqueKey(String prefix, String extension) {
        String uuid = UUID.randomUUID().toString();
        return prefix + uuid + extension;
    }
    
    /**
     * Gets the configured URL expiration time in seconds.
     *
     * @return The URL expiration time in seconds
     */
    public long getUrlExpirationSeconds() {
        return urlExpirationSeconds;
    }
    
    /**
     * Checks if public read access is enabled for uploaded objects.
     *
     * @return true if public read access is enabled, false otherwise
     */
    public boolean isPublicReadAccess() {
        return publicReadAccess;
    }
}