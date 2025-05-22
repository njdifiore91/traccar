/*
 * Copyright 2022 - 2023 Anton Tananaev (anton@traccar.org)
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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import com.nimbusds.jwt.proc.JWTProcessor;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.traccar.storage.StorageException;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Manages JWT token generation, validation, and revocation.
 * Supports standard JWT claims and additional custom claims.
 */
@Singleton
public class TokenManager {

    private static final int DEFAULT_EXPIRATION_DAYS = 7;
    private static final String CLAIM_USER_ID = "uid";
    private static final String CLAIM_TOKEN_ID = "jti";
    private static final String CLAIM_SCOPES = "scopes";
    private static final String CLAIM_ISSUER = "iss";
    private static final String CLAIM_SUBJECT = "sub";
    private static final String CLAIM_AUDIENCE = "aud";
    private static final String CLAIM_ISSUED_AT = "iat";
    private static final String CLAIM_EXPIRATION = "exp";
    
    private final CryptoManager cryptoManager;
    private final Tracer tracer;
    
    // Store for revoked tokens
    private final Map<String, Date> revokedTokens = new ConcurrentHashMap<>();

    /**
     * Represents JWT token data including standard and custom claims.
     */
    public static class TokenData {
        private long userId;
        private String tokenId;
        private Date expiration;
        private Set<String> scopes;
        private String issuer;
        private String subject;
        private String audience;
        private Date issuedAt;

        public long getUserId() {
            return userId;
        }

        public void setUserId(long userId) {
            this.userId = userId;
        }

        public String getTokenId() {
            return tokenId;
        }

        public void setTokenId(String tokenId) {
            this.tokenId = tokenId;
        }

        public Date getExpiration() {
            return expiration;
        }

        public void setExpiration(Date expiration) {
            this.expiration = expiration;
        }

        public Set<String> getScopes() {
            return scopes;
        }

        public void setScopes(Set<String> scopes) {
            this.scopes = scopes;
        }

        public String getIssuer() {
            return issuer;
        }

        public void setIssuer(String issuer) {
            this.issuer = issuer;
        }

        public String getSubject() {
            return subject;
        }

        public void setSubject(String subject) {
            this.subject = subject;
        }

        public String getAudience() {
            return audience;
        }

        public void setAudience(String audience) {
            this.audience = audience;
        }

        public Date getIssuedAt() {
            return issuedAt;
        }

        public void setIssuedAt(Date issuedAt) {
            this.issuedAt = issuedAt;
        }
    }

    /**
     * Constructs a TokenManager with the specified dependencies.
     *
     * @param cryptoManager The crypto manager for signing and verifying tokens
     * @param tracer The OpenTelemetry tracer for distributed tracing
     */
    @Inject
    public TokenManager(CryptoManager cryptoManager, Tracer tracer) {
        this.cryptoManager = cryptoManager;
        this.tracer = tracer;
    }

    /**
     * Generates a JWT token for the specified user ID with default expiration and scopes.
     *
     * @param userId The user ID to include in the token
     * @return The generated JWT token string
     * @throws StorageException If there's an issue with storage operations
     * @throws JOSEException If there's an issue with JWT operations
     */
    public String generateToken(long userId) throws StorageException, JOSEException {
        return generateToken(userId, null, null, null, null, null);
    }

    /**
     * Generates a JWT token with the specified parameters.
     *
     * @param userId The user ID to include in the token
     * @param expiration The token expiration date (null for default)
     * @param scopes The token scopes (null for default)
     * @param issuer The token issuer (null for default)
     * @param subject The token subject (null for default)
     * @param audience The token audience (null for default)
     * @return The generated JWT token string
     * @throws StorageException If there's an issue with storage operations
     * @throws JOSEException If there's an issue with JWT operations
     */
    public String generateToken(
            long userId, Date expiration, Set<String> scopes, 
            String issuer, String subject, String audience) 
            throws StorageException, JOSEException {
        
        Span span = tracer.spanBuilder("TokenManager.generateToken")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("userId", userId);
            
            // Create JWT claims set builder
            JWTClaimsSet.Builder claimsBuilder = new JWTClaimsSet.Builder();
            
            // Add required claims
            String tokenId = UUID.randomUUID().toString();
            Date now = new Date();
            Date tokenExpiration = expiration != null ? expiration : 
                    new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(DEFAULT_EXPIRATION_DAYS));
            
            claimsBuilder.claim(CLAIM_USER_ID, userId);
            claimsBuilder.jwtID(tokenId);
            claimsBuilder.issueTime(now);
            claimsBuilder.expirationTime(tokenExpiration);
            
            // Add optional claims if provided
            if (scopes != null && !scopes.isEmpty()) {
                claimsBuilder.claim(CLAIM_SCOPES, scopes);
            }
            
            if (issuer != null) {
                claimsBuilder.issuer(issuer);
            }
            
            if (subject != null) {
                claimsBuilder.subject(subject);
            }
            
            if (audience != null) {
                claimsBuilder.audience(audience);
            }
            
            // Create the JWT
            JWTClaimsSet claims = claimsBuilder.build();
            
            // Create signed JWT
            SignedJWT signedJWT = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.HS256).build(),
                    claims);
            
            // Sign the JWT
            JWSSigner signer = new MACSigner(cryptoManager.getSecretKey());
            signedJWT.sign(signer);
            
            span.setStatus(StatusCode.OK);
            return signedJWT.serialize();
        } catch (JOSEException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to generate token");
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Verifies a JWT token and returns the token data.
     *
     * @param token The JWT token string to verify
     * @return The verified token data
     * @throws StorageException If there's an issue with storage operations
     * @throws JOSEException If there's an issue with JWT operations
     * @throws ParseException If there's an issue parsing the JWT
     * @throws SecurityException If the token is invalid or expired
     */
    public TokenData verifyToken(String token) 
            throws StorageException, JOSEException, ParseException, SecurityException {
        
        Span span = tracer.spanBuilder("TokenManager.verifyToken")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Parse the JWT
            SignedJWT signedJWT = SignedJWT.parse(token);
            
            // Verify the signature
            JWSVerifier verifier = new MACVerifier(cryptoManager.getSecretKey());
            if (!signedJWT.verify(verifier)) {
                span.setStatus(StatusCode.ERROR, "Invalid token signature");
                throw new SecurityException("Invalid token signature");
            }
            
            // Get the claims
            JWTClaimsSet claims = signedJWT.getJWTClaimsSet();
            
            // Check if token is expired
            Date expirationTime = claims.getExpirationTime();
            if (expirationTime != null && expirationTime.before(new Date())) {
                span.setStatus(StatusCode.ERROR, "Token has expired");
                throw new SecurityException("Token has expired");
            }
            
            // Check if token has been revoked
            String tokenId = claims.getJWTID();
            if (tokenId != null && isTokenRevoked(tokenId)) {
                span.setStatus(StatusCode.ERROR, "Token has been revoked");
                throw new SecurityException("Token has been revoked");
            }
            
            // Extract token data
            TokenData data = new TokenData();
            data.setUserId(claims.getLongClaim(CLAIM_USER_ID));
            data.setTokenId(tokenId);
            data.setExpiration(expirationTime);
            data.setIssuedAt(claims.getIssueTime());
            data.setIssuer(claims.getIssuer());
            data.setSubject(claims.getSubject());
            data.setAudience(claims.getAudience().size() > 0 ? claims.getAudience().get(0) : null);
            
            // Extract scopes if present
            Object scopesObj = claims.getClaim(CLAIM_SCOPES);
            if (scopesObj instanceof Iterable<?>) {
                Set<String> scopes = new HashSet<>();
                for (Object scope : (Iterable<?>) scopesObj) {
                    if (scope instanceof String) {
                        scopes.add((String) scope);
                    }
                }
                data.setScopes(scopes);
            } else {
                data.setScopes(Collections.emptySet());
            }
            
            span.setStatus(StatusCode.OK);
            return data;
        } catch (ParseException | JOSEException | SecurityException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to verify token");
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Introspects a token and returns its validity and metadata.
     * This method is primarily used by the API Gateway for token validation.
     *
     * @param token The JWT token string to introspect
     * @return A map containing token introspection data including 'active' status
     */
    public Map<String, Object> introspectToken(String token) {
        Span span = tracer.spanBuilder("TokenManager.introspectToken")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Map<String, Object> result = new ConcurrentHashMap<>();
            result.put("active", false);
            
            try {
                // Verify the token
                TokenData data = verifyToken(token);
                
                // Token is valid, populate introspection data
                result.put("active", true);
                result.put("uid", data.getUserId());
                result.put("jti", data.getTokenId());
                result.put("exp", data.getExpiration().getTime() / 1000);
                result.put("iat", data.getIssuedAt().getTime() / 1000);
                
                if (data.getScopes() != null && !data.getScopes().isEmpty()) {
                    result.put("scope", String.join(" ", data.getScopes()));
                }
                
                if (data.getIssuer() != null) {
                    result.put("iss", data.getIssuer());
                }
                
                if (data.getSubject() != null) {
                    result.put("sub", data.getSubject());
                }
                
                if (data.getAudience() != null) {
                    result.put("aud", data.getAudience());
                }
                
                span.setStatus(StatusCode.OK);
            } catch (Exception e) {
                // Token is invalid, return active=false
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, "Token introspection failed");
            }
            
            return result;
        } finally {
            span.end();
        }
    }
    
    /**
     * Revokes a token by its ID.
     *
     * @param tokenId The ID of the token to revoke
     */
    public void revokeToken(String tokenId) {
        Span span = tracer.spanBuilder("TokenManager.revokeToken")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("tokenId", tokenId);
            revokedTokens.put(tokenId, new Date());
            span.setStatus(StatusCode.OK);
        } finally {
            span.end();
        }
    }
    
    /**
     * Revokes a token by parsing the token string and extracting its ID.
     *
     * @param token The JWT token string to revoke
     * @throws ParseException If there's an issue parsing the JWT
     */
    public void revokeToken(SignedJWT token) throws ParseException {
        Span span = tracer.spanBuilder("TokenManager.revokeTokenByJWT")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            String tokenId = token.getJWTClaimsSet().getJWTID();
            if (tokenId != null) {
                revokeToken(tokenId);
                span.setStatus(StatusCode.OK);
            } else {
                span.setStatus(StatusCode.ERROR, "Token does not have an ID");
                throw new IllegalArgumentException("Token does not have an ID");
            }
        } catch (ParseException e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, "Failed to parse token");
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Checks if a token has been revoked.
     *
     * @param tokenId The ID of the token to check
     * @return true if the token has been revoked, false otherwise
     */
    public boolean isTokenRevoked(String tokenId) {
        return revokedTokens.containsKey(tokenId);
    }
    
    /**
     * Cleans up expired revoked tokens to prevent memory leaks.
     * This method should be called periodically by a scheduled task.
     */
    public void cleanupRevokedTokens() {
        Span span = tracer.spanBuilder("TokenManager.cleanupRevokedTokens")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            Date now = new Date();
            int removedTokens = 0;
            
            // Remove tokens that have been revoked for longer than their expiration period
            for (Map.Entry<String, Date> entry : revokedTokens.entrySet()) {
                if (entry.getValue().getTime() + TimeUnit.DAYS.toMillis(DEFAULT_EXPIRATION_DAYS) < now.getTime()) {
                    revokedTokens.remove(entry.getKey());
                    removedTokens++;
                }
            }
            
            span.setAttribute("removedTokens", removedTokens);
            span.setStatus(StatusCode.OK);
        } finally {
            span.end();
        }
    }