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
package org.traccar.api.signature;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Base64;
import org.traccar.storage.StorageException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Specialized token manager for service-to-service authentication.
 * Generates and validates short-lived JWT tokens with limited scopes for microservice communication.
 * Integrates with service registry for validation and supports automatic token renewal.
 */
@Singleton
public class ServiceTokenManager {

    private static final int DEFAULT_EXPIRATION_MINUTES = 30;
    private static final int RENEWAL_THRESHOLD_MINUTES = 5;
    private static final int CACHE_CLEANUP_INTERVAL_MINUTES = 10;

    private final ObjectMapper objectMapper;
    private final CryptoManager cryptoManager;
    private final ServiceRegistryClient serviceRegistryClient;
    private final Map<String, TokenData> tokenCache;
    private final ScheduledExecutorService scheduler;

    /**
     * Service token data structure.
     * Contains service identification, scopes, and expiration information.
     */
    public static class TokenData {
        @JsonProperty("s")
        private String serviceId;
        @JsonProperty("t")
        private String targetServiceId;
        @JsonProperty("sc")
        private Set<String> scopes;
        @JsonProperty("e")
        private Date expiration;

        public String getServiceId() {
            return serviceId;
        }

        public String getTargetServiceId() {
            return targetServiceId;
        }

        public Set<String> getScopes() {
            return scopes;
        }

        public Date getExpiration() {
            return expiration;
        }

        public boolean isExpiringSoon() {
            return expiration.getTime() - System.currentTimeMillis() < TimeUnit.MINUTES.toMillis(RENEWAL_THRESHOLD_MINUTES);
        }
    }

    /**
     * Constructs a new ServiceTokenManager.
     *
     * @param objectMapper JSON serialization/deserialization
     * @param cryptoManager Cryptographic operations for token signing
     * @param serviceRegistryClient Client for service registry integration
     */
    @Inject
    public ServiceTokenManager(
            ObjectMapper objectMapper,
            CryptoManager cryptoManager,
            ServiceRegistryClient serviceRegistryClient) {
        this.objectMapper = objectMapper;
        this.cryptoManager = cryptoManager;
        this.serviceRegistryClient = serviceRegistryClient;
        this.tokenCache = new ConcurrentHashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        
        // Schedule periodic cache cleanup
        scheduler.scheduleAtFixedRate(
                this::cleanupExpiredTokens,
                CACHE_CLEANUP_INTERVAL_MINUTES,
                CACHE_CLEANUP_INTERVAL_MINUTES,
                TimeUnit.MINUTES);
    }

    /**
     * Generates a service token for communicating with a target service.
     *
     * @param serviceId ID of the requesting service
     * @param targetServiceId ID of the target service
     * @param scopes Set of permission scopes requested
     * @return JWT token string
     * @throws IOException If token serialization fails
     * @throws GeneralSecurityException If token signing fails
     * @throws StorageException If crypto key storage access fails
     */
    public String generateToken(
            String serviceId,
            String targetServiceId,
            Set<String> scopes) throws IOException, GeneralSecurityException, StorageException {
        return generateToken(serviceId, targetServiceId, scopes, null);
    }

    /**
     * Generates a service token with custom expiration.
     *
     * @param serviceId ID of the requesting service
     * @param targetServiceId ID of the target service
     * @param scopes Set of permission scopes requested
     * @param expiration Custom expiration time or null for default
     * @return JWT token string
     * @throws IOException If token serialization fails
     * @throws GeneralSecurityException If token signing fails
     * @throws StorageException If crypto key storage access fails
     */
    public String generateToken(
            String serviceId,
            String targetServiceId,
            Set<String> scopes,
            Date expiration) throws IOException, GeneralSecurityException, StorageException {
        
        // Verify service exists in registry
        if (!serviceRegistryClient.isServiceRegistered(serviceId)) {
            throw new SecurityException("Source service not registered: " + serviceId);
        }
        
        // Verify target service exists in registry
        if (!serviceRegistryClient.isServiceRegistered(targetServiceId)) {
            throw new SecurityException("Target service not registered: " + targetServiceId);
        }
        
        TokenData data = new TokenData();
        data.serviceId = serviceId;
        data.targetServiceId = targetServiceId;
        data.scopes = scopes;
        
        if (expiration != null) {
            data.expiration = expiration;
        } else {
            data.expiration = new Date(System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(DEFAULT_EXPIRATION_MINUTES));
        }
        
        byte[] encoded = objectMapper.writeValueAsBytes(data);
        return Base64.encodeBase64URLSafeString(cryptoManager.sign(encoded));
    }

    /**
     * Verifies a service token.
     *
     * @param token JWT token string to verify
     * @return TokenData containing service information and scopes
     * @throws IOException If token deserialization fails
     * @throws GeneralSecurityException If token signature verification fails
     * @throws StorageException If crypto key storage access fails
     */
    public TokenData verifyToken(String token) throws IOException, GeneralSecurityException, StorageException {
        byte[] encoded = cryptoManager.verify(Base64.decodeBase64(token));
        TokenData data = objectMapper.readValue(encoded, TokenData.class);
        
        // Check expiration
        if (data.expiration.before(new Date())) {
            throw new SecurityException("Service token has expired");
        }
        
        // Verify services still exist in registry
        if (!serviceRegistryClient.isServiceRegistered(data.serviceId)) {
            throw new SecurityException("Source service no longer registered: " + data.serviceId);
        }
        
        if (!serviceRegistryClient.isServiceRegistered(data.targetServiceId)) {
            throw new SecurityException("Target service no longer registered: " + data.targetServiceId);
        }
        
        return data;
    }

    /**
     * Gets or generates a token for service-to-service communication.
     * Uses cached token if available and not expiring soon, otherwise generates a new one.
     *
     * @param serviceId ID of the requesting service
     * @param targetServiceId ID of the target service
     * @param scopes Set of permission scopes requested
     * @return JWT token string
     * @throws IOException If token serialization fails
     * @throws GeneralSecurityException If token signing fails
     * @throws StorageException If crypto key storage access fails
     */
    public String getServiceToken(
            String serviceId,
            String targetServiceId,
            Set<String> scopes) throws IOException, GeneralSecurityException, StorageException {
        
        String cacheKey = buildCacheKey(serviceId, targetServiceId, scopes);
        TokenData cachedToken = tokenCache.get(cacheKey);
        
        // If token exists, is valid, and not expiring soon, return it
        if (cachedToken != null && !cachedToken.isExpiringSoon()) {
            return Base64.encodeBase64URLSafeString(objectMapper.writeValueAsBytes(cachedToken));
        }
        
        // Generate new token
        String token = generateToken(serviceId, targetServiceId, scopes);
        
        // Cache the token data
        byte[] encoded = cryptoManager.verify(Base64.decodeBase64(token));
        TokenData data = objectMapper.readValue(encoded, TokenData.class);
        tokenCache.put(cacheKey, data);
        
        return token;
    }

    /**
     * Verifies if a token has the required scope.
     *
     * @param tokenData Token data to check
     * @param requiredScope Scope that is required
     * @return true if token has the required scope
     */
    public boolean hasScope(TokenData tokenData, String requiredScope) {
        return tokenData.scopes != null && tokenData.scopes.contains(requiredScope);
    }

    /**
     * Invalidates a cached token for a service pair.
     *
     * @param serviceId ID of the requesting service
     * @param targetServiceId ID of the target service
     */
    public void invalidateToken(String serviceId, String targetServiceId) {
        tokenCache.entrySet().removeIf(entry -> {
            TokenData data = entry.getValue();
            return data.serviceId.equals(serviceId) && data.targetServiceId.equals(targetServiceId);
        });
    }

    /**
     * Builds a cache key from service IDs and scopes.
     *
     * @param serviceId Source service ID
     * @param targetServiceId Target service ID
     * @param scopes Set of scopes
     * @return Cache key string
     */
    private String buildCacheKey(String serviceId, String targetServiceId, Set<String> scopes) {
        return serviceId + "->" + targetServiceId + ":[" + String.join(",", scopes) + "]";
    }

    /**
     * Removes expired tokens from the cache.
     * Called periodically by the scheduler.
     */
    private void cleanupExpiredTokens() {
        Date now = new Date();
        tokenCache.entrySet().removeIf(entry -> entry.getValue().expiration.before(now));
    }

    /**
     * Shutdown hook to clean up resources.
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}