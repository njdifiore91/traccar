package org.traccar.api.security;

import org.traccar.model.User;

import java.util.Date;
import java.util.Map;

/**
 * Represents the result of a successful login operation.
 * Contains the authenticated user, token information, and session metadata.
 */
public class LoginResult {

    private final User user;
    private final Date expiration;
    private final String token;
    private final String refreshToken;
    private final Date refreshTokenExpiration;
    private final String sessionId;
    private final String issuer;
    private final Map<String, Object> sessionMetadata;

    /**
     * Comprehensive constructor with all fields for distributed authentication.
     * 
     * @param user The authenticated user
     * @param expiration Token expiration date
     * @param token JWT token for authentication
     * @param refreshToken Token used to obtain a new JWT without re-authentication
     * @param refreshTokenExpiration Expiration date for the refresh token
     * @param sessionId Unique identifier for the session in the distributed session store
     * @param issuer The service that issued the token
     * @param sessionMetadata Additional metadata for the session in distributed architecture
     */
    public LoginResult(User user, Date expiration, String token, String refreshToken, 
                      Date refreshTokenExpiration, String sessionId, String issuer, 
                      Map<String, Object> sessionMetadata) {
        this.user = user;
        this.expiration = expiration;
        this.token = token;
        this.refreshToken = refreshToken;
        this.refreshTokenExpiration = refreshTokenExpiration;
        this.sessionId = sessionId;
        this.issuer = issuer;
        this.sessionMetadata = sessionMetadata;
    }

    /**
     * Minimal constructor for backward compatibility.
     * 
     * @param user The authenticated user
     */
    public LoginResult(User user) {
        this(user, null, null, null, null, null, null, null);
    }

    /**
     * Constructor with user and expiration for backward compatibility.
     * 
     * @param user The authenticated user
     * @param expiration Token expiration date
     */
    public LoginResult(User user, Date expiration) {
        this(user, expiration, null, null, null, null, null, null);
    }

    /**
     * Constructor with essential JWT fields.
     * 
     * @param user The authenticated user
     * @param expiration Token expiration date
     * @param token JWT token for authentication
     * @param refreshToken Token used to obtain a new JWT without re-authentication
     * @param refreshTokenExpiration Expiration date for the refresh token
     */
    public LoginResult(User user, Date expiration, String token, String refreshToken, 
                      Date refreshTokenExpiration) {
        this(user, expiration, token, refreshToken, refreshTokenExpiration, null, null, null);
    }

    /**
     * @return The authenticated user
     */
    public User getUser() {
        return user;
    }

    /**
     * @return Token expiration date
     */
    public Date getExpiration() {
        return expiration;
    }

    /**
     * @return JWT token for authentication
     */
    public String getToken() {
        return token;
    }

    /**
     * @return Token used to obtain a new JWT without re-authentication
     */
    public String getRefreshToken() {
        return refreshToken;
    }

    /**
     * @return Expiration date for the refresh token
     */
    public Date getRefreshTokenExpiration() {
        return refreshTokenExpiration;
    }

    /**
     * @return Unique identifier for the session in the distributed session store
     */
    public String getSessionId() {
        return sessionId;
    }

    /**
     * @return The service that issued the token
     */
    public String getIssuer() {
        return issuer;
    }

    /**
     * @return Additional metadata for the session in distributed architecture
     */
    public Map<String, Object> getSessionMetadata() {
        return sessionMetadata;
    }
}