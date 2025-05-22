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
package org.traccar.api.signature;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Manages cryptographic operations including data signing, verification, and JWT token handling.
 * Supports both ECDSA and RSA algorithms with key rotation policies and circuit breaker pattern.
 */
@Singleton
public class CryptoManager {

    private static final String CIRCUIT_BREAKER_NAME = "cryptoStorageCircuitBreaker";
    private static final String TRACER_INSTRUMENTATION_NAME = "org.traccar.api.signature";
    private static final String DEFAULT_KEY_ID = "default";
    private static final String DEFAULT_ALGORITHM = "EC";
    private static final String EC_CURVE = "secp256r1";
    private static final String RSA_ALGORITHM = "RSA";
    private static final int RSA_KEY_SIZE = 2048;
    private static final long KEY_ROTATION_PERIOD_DAYS = 90; // 3 months

    private final Storage storage;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    // Metrics
    private final Counter signCounter;
    private final Counter verifyCounter;
    private final Counter signJwtCounter;
    private final Counter verifyJwtCounter;
    private final Counter failureCounter;
    private final Timer signTimer;
    private final Timer verifyTimer;
    private final Timer signJwtTimer;
    private final Timer verifyJwtTimer;

    // Cache for keys
    private final Map<String, KeyPairInfo> keyPairCache = new HashMap<>();

    /**
     * Constructs a new CryptoManager with the specified dependencies.
     *
     * @param storage The storage interface for persisting keys
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The Micrometer registry for metrics
     */
    @Inject
    public CryptoManager(Storage storage, Tracer tracer, MeterRegistry meterRegistry) {
        this.storage = storage;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .permittedNumberOfCallsInHalfOpenState(3)
                .slidingWindowSize(10)
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .build();
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        this.circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);

        // Initialize metrics
        this.signCounter = Counter.builder("crypto.sign.count")
                .description("Number of sign operations")
                .register(meterRegistry);
        this.verifyCounter = Counter.builder("crypto.verify.count")
                .description("Number of verify operations")
                .register(meterRegistry);
        this.signJwtCounter = Counter.builder("crypto.jwt.sign.count")
                .description("Number of JWT sign operations")
                .register(meterRegistry);
        this.verifyJwtCounter = Counter.builder("crypto.jwt.verify.count")
                .description("Number of JWT verify operations")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("crypto.operation.failures")
                .description("Number of failed cryptographic operations")
                .register(meterRegistry);
        this.signTimer = Timer.builder("crypto.sign.time")
                .description("Time taken for sign operations")
                .register(meterRegistry);
        this.verifyTimer = Timer.builder("crypto.verify.time")
                .description("Time taken for verify operations")
                .register(meterRegistry);
        this.signJwtTimer = Timer.builder("crypto.jwt.sign.time")
                .description("Time taken for JWT sign operations")
                .register(meterRegistry);
        this.verifyJwtTimer = Timer.builder("crypto.jwt.verify.time")
                .description("Time taken for JWT verify operations")
                .register(meterRegistry);
    }

    /**
     * Signs data using ECDSA with SHA-256.
     *
     * @param data The data to sign
     * @return The signed data
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     */
    public byte[] sign(byte[] data) throws GeneralSecurityException, StorageException {
        Span span = tracer.spanBuilder("sign").startSpan();
        try (Scope scope = span.makeCurrent()) {
            signCounter.increment();
            return signTimer.record(() -> {
                try {
                    KeyPairInfo keyPairInfo = getOrCreateKeyPair(DEFAULT_KEY_ID, DEFAULT_ALGORITHM);
                    Signature signature = Signature.getInstance("SHA256withECDSA");
                    signature.initSign(keyPairInfo.getPrivateKey());
                    signature.update(data);
                    byte[] block = signature.sign();
                    byte[] combined = new byte[1 + block.length + data.length];
                    combined[0] = (byte) block.length;
                    System.arraycopy(block, 0, combined, 1, block.length);
                    System.arraycopy(data, 0, combined, 1 + block.length, data.length);
                    return combined;
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to sign data", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Verifies signed data using ECDSA with SHA-256.
     *
     * @param data The signed data to verify
     * @return The original data if verification succeeds
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     */
    public byte[] verify(byte[] data) throws GeneralSecurityException, StorageException {
        Span span = tracer.spanBuilder("verify").startSpan();
        try (Scope scope = span.makeCurrent()) {
            verifyCounter.increment();
            return verifyTimer.record(() -> {
                try {
                    KeyPairInfo keyPairInfo = getOrCreateKeyPair(DEFAULT_KEY_ID, DEFAULT_ALGORITHM);
                    Signature signature = Signature.getInstance("SHA256withECDSA");
                    signature.initVerify(keyPairInfo.getPublicKey());
                    int length = data[0];
                    byte[] originalData = new byte[data.length - 1 - length];
                    System.arraycopy(data, 1 + length, originalData, 0, originalData.length);
                    signature.update(originalData);
                    if (!signature.verify(data, 1, length)) {
                        throw new SecurityException("Invalid signature");
                    }
                    return originalData;
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to verify data", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Signs a JWT token using the specified algorithm.
     *
     * @param payload The payload to include in the JWT
     * @param algorithm The algorithm to use ("RS256" or "ES256")
     * @param expirationMinutes The expiration time in minutes
     * @return The signed JWT token
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     */
    public String signJwt(Map<String, Object> payload, String algorithm, int expirationMinutes) 
            throws GeneralSecurityException, StorageException {
        Span span = tracer.spanBuilder("signJwt").startSpan();
        span.setAttribute("algorithm", algorithm);
        try (Scope scope = span.makeCurrent()) {
            signJwtCounter.increment();
            return signJwtTimer.record(() -> {
                try {
                    String keyId;
                    Algorithm jwtAlgorithm;
                    
                    if ("RS256".equals(algorithm)) {
                        keyId = "rsa-" + UUID.randomUUID().toString();
                        KeyPairInfo keyPairInfo = getOrCreateKeyPair(keyId, RSA_ALGORITHM);
                        jwtAlgorithm = Algorithm.RSA256(
                                (RSAPublicKey) keyPairInfo.getPublicKey(), 
                                (RSAPrivateKey) keyPairInfo.getPrivateKey());
                    } else if ("ES256".equals(algorithm)) {
                        keyId = "ec-" + UUID.randomUUID().toString();
                        KeyPairInfo keyPairInfo = getOrCreateKeyPair(keyId, DEFAULT_ALGORITHM);
                        jwtAlgorithm = Algorithm.ECDSA256(
                                (ECPublicKey) keyPairInfo.getPublicKey(), 
                                (ECPrivateKey) keyPairInfo.getPrivateKey());
                    } else {
                        throw new IllegalArgumentException("Unsupported algorithm: " + algorithm);
                    }
                    
                    Date now = new Date();
                    Date expiration = new Date(now.getTime() + TimeUnit.MINUTES.toMillis(expirationMinutes));
                    
                    return JWT.create()
                            .withKeyId(keyId)
                            .withPayload(payload)
                            .withIssuedAt(now)
                            .withExpiresAt(expiration)
                            .withJWTId(UUID.randomUUID().toString())
                            .sign(jwtAlgorithm);
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to sign JWT", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Verifies a JWT token.
     *
     * @param token The JWT token to verify
     * @return The decoded JWT if verification succeeds
     * @throws JWTVerificationException If verification fails
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     */
    public DecodedJWT verifyJwt(String token) 
            throws JWTVerificationException, GeneralSecurityException, StorageException {
        Span span = tracer.spanBuilder("verifyJwt").startSpan();
        try (Scope scope = span.makeCurrent()) {
            verifyJwtCounter.increment();
            return verifyJwtTimer.record(() -> {
                try {
                    // First, decode the token without verification to get the key ID
                    DecodedJWT jwt = JWT.decode(token);
                    String keyId = jwt.getKeyId();
                    String algorithm = jwt.getAlgorithm();
                    
                    // Try to verify with the specified key ID
                    try {
                        return verifyJwtWithKeyId(token, keyId, algorithm);
                    } catch (Exception e) {
                        // If verification fails, try all available keys of the same algorithm type
                        List<KeystoreModel> keys = getAllKeys();
                        for (KeystoreModel key : keys) {
                            if (key.getKeyId().equals(keyId)) {
                                continue; // Skip the already tried key
                            }
                            if (algorithm.startsWith("RS") && RSA_ALGORITHM.equals(key.getAlgorithm()) ||
                                algorithm.startsWith("ES") && DEFAULT_ALGORITHM.equals(key.getAlgorithm())) {
                                try {
                                    return verifyJwtWithKeyId(token, key.getKeyId(), algorithm);
                                } catch (Exception ignored) {
                                    // Continue trying other keys
                                }
                            }
                        }
                        // If we get here, no key could verify the token
                        throw new JWTVerificationException("Could not verify JWT token with any available key");
                    }
                } catch (JWTVerificationException e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw e;
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to verify JWT", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Verifies a JWT token with a specific key ID.
     *
     * @param token The JWT token to verify
     * @param keyId The key ID to use for verification
     * @param algorithm The algorithm used in the JWT
     * @return The decoded JWT if verification succeeds
     * @throws JWTVerificationException If verification fails
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     */
    private DecodedJWT verifyJwtWithKeyId(String token, String keyId, String algorithm) 
            throws JWTVerificationException, GeneralSecurityException, StorageException {
        KeyPairInfo keyPairInfo = getOrCreateKeyPair(keyId, 
                algorithm.startsWith("RS") ? RSA_ALGORITHM : DEFAULT_ALGORITHM);
        
        Algorithm jwtAlgorithm;
        if ("RS256".equals(algorithm)) {
            jwtAlgorithm = Algorithm.RSA256(
                    (RSAPublicKey) keyPairInfo.getPublicKey(), null);
        } else if ("ES256".equals(algorithm)) {
            jwtAlgorithm = Algorithm.ECDSA256(
                    (ECPublicKey) keyPairInfo.getPublicKey(), null);
        } else {
            throw new IllegalArgumentException("Unsupported algorithm: " + algorithm);
        }
        
        JWTVerifier verifier = JWT.require(jwtAlgorithm)
                .withIssuer(keyId)
                .build();
        
        return verifier.verify(token);
    }

    /**
     * Rotates keys based on the configured rotation policy.
     *
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     */
    public void rotateKeys() throws GeneralSecurityException, StorageException {
        Span span = tracer.spanBuilder("rotateKeys").startSpan();
        try (Scope scope = span.makeCurrent()) {
            List<KeystoreModel> keys = getAllKeys();
            Instant now = Instant.now();
            
            // Check if any keys need rotation
            for (KeystoreModel key : keys) {
                if (key.getCreatedAt() != null) {
                    Instant createdAt = key.getCreatedAt().toInstant();
                    if (createdAt.plus(Duration.ofDays(KEY_ROTATION_PERIOD_DAYS)).isBefore(now)) {
                        // Create a new key with the same algorithm
                        String newKeyId = key.getAlgorithm() + "-" + UUID.randomUUID().toString();
                        getOrCreateKeyPair(newKeyId, key.getAlgorithm());
                        
                        // Mark the old key as rotated but keep it for verification
                        key.setRotated(true);
                        updateKey(key);
                    }
                }
            }
            
            // Clear the cache to force reload of keys
            keyPairCache.clear();
        } finally {
            span.end();
        }
    }

    /**
     * Gets or creates a key pair with the specified ID and algorithm.
     *
     * @param keyId The key ID
     * @param algorithm The algorithm to use
     * @return The key pair information
     * @throws StorageException If a storage error occurs
     * @throws GeneralSecurityException If a security error occurs
     */
    private KeyPairInfo getOrCreateKeyPair(String keyId, String algorithm) 
            throws StorageException, GeneralSecurityException {
        // Check cache first
        if (keyPairCache.containsKey(keyId)) {
            return keyPairCache.get(keyId);
        }
        
        // Try to get from storage with circuit breaker
        KeystoreModel model = executeWithCircuitBreaker(() -> {
            try {
                return storage.getObject(KeystoreModel.class, 
                        new Request(new Condition.Equals("keyId", keyId)));
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
        
        if (model != null) {
            // Create key pair from stored data
            PublicKey publicKey = KeyFactory.getInstance(model.getAlgorithm())
                    .generatePublic(new X509EncodedKeySpec(model.getPublicKey()));
            PrivateKey privateKey = KeyFactory.getInstance(model.getAlgorithm())
                    .generatePrivate(new PKCS8EncodedKeySpec(model.getPrivateKey()));
            
            KeyPairInfo keyPairInfo = new KeyPairInfo(keyId, publicKey, privateKey);
            keyPairCache.put(keyId, keyPairInfo);
            return keyPairInfo;
        } else {
            // Generate new key pair
            KeyPair pair;
            if (RSA_ALGORITHM.equals(algorithm)) {
                KeyPairGenerator generator = KeyPairGenerator.getInstance(RSA_ALGORITHM);
                generator.initialize(RSA_KEY_SIZE, new SecureRandom());
                pair = generator.generateKeyPair();
            } else {
                KeyPairGenerator generator = KeyPairGenerator.getInstance(DEFAULT_ALGORITHM);
                generator.initialize(new ECGenParameterSpec(EC_CURVE), new SecureRandom());
                pair = generator.generateKeyPair();
            }
            
            // Store the new key pair
            model = new KeystoreModel();
            model.setKeyId(keyId);
            model.setAlgorithm(algorithm);
            model.setPublicKey(pair.getPublic().getEncoded());
            model.setPrivateKey(pair.getPrivate().getEncoded());
            model.setCreatedAt(new Date());
            model.setRotated(false);
            
            executeWithCircuitBreaker(() -> {
                try {
                    storage.addObject(model, new Request(new Columns.Exclude("id")));
                    return null;
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                }
            });
            
            KeyPairInfo keyPairInfo = new KeyPairInfo(keyId, pair.getPublic(), pair.getPrivate());
            keyPairCache.put(keyId, keyPairInfo);
            return keyPairInfo;
        }
    }

    /**
     * Gets all keys from storage.
     *
     * @return A list of all keys
     * @throws StorageException If a storage error occurs
     */
    private List<KeystoreModel> getAllKeys() throws StorageException {
        return executeWithCircuitBreaker(() -> {
            try {
                return storage.getObjects(KeystoreModel.class, new Request(new Columns.All()));
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Updates a key in storage.
     *
     * @param model The key model to update
     * @throws StorageException If a storage error occurs
     */
    private void updateKey(KeystoreModel model) throws StorageException {
        executeWithCircuitBreaker(() -> {
            try {
                storage.updateObject(model, new Request(new Columns.All()));
                return null;
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Executes a function with circuit breaker protection.
     *
     * @param supplier The function to execute
     * @param <T> The return type of the function
     * @return The result of the function
     * @throws StorageException If a storage error occurs
     */
    private <T> T executeWithCircuitBreaker(Supplier<T> supplier) throws StorageException {
        try {
            return circuitBreaker.executeSupplier(supplier);
        } catch (Exception e) {
            if (e instanceof StorageException) {
                throw (StorageException) e;
            } else if (e.getCause() instanceof StorageException) {
                throw (StorageException) e.getCause();
            } else {
                throw new StorageException(e);
            }
        }
    }

    /**
     * Class to hold key pair information.
     */
    private static class KeyPairInfo {
        private final String keyId;
        private final PublicKey publicKey;
        private final PrivateKey privateKey;

        public KeyPairInfo(String keyId, PublicKey publicKey, PrivateKey privateKey) {
            this.keyId = keyId;
            this.publicKey = publicKey;
            this.privateKey = privateKey;
        }

        public String getKeyId() {
            return keyId;
        }

        public PublicKey getPublicKey() {
            return publicKey;
        }

        public PrivateKey getPrivateKey() {
            return privateKey;
        }
    }
}