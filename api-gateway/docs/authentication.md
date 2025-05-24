# Authentication

## Overview

This document describes the authentication mechanisms implemented by the Traccar API Gateway. Authentication is centralized at the API Gateway level for all external clients, providing a unified security layer for the entire microservices architecture. The API Gateway handles authentication for all client requests and propagates authenticated user context to downstream microservices.

The authentication system has been designed with the following key principles:

- **Security**: Robust protection against common authentication attacks
- **Scalability**: Stateless design with distributed session storage
- **Flexibility**: Support for multiple authentication methods and identity sources
- **Usability**: Simple integration for client applications
- **Compliance**: Adherence to security best practices and standards

## Authentication Methods

The API Gateway supports multiple authentication methods to accommodate different client types and security requirements:

### 1. Basic Authentication

Basic authentication uses HTTP Basic Auth with username and password credentials.

```http
GET /api/session HTTP/1.1
Host: example.traccar.org
Authorization: Basic dXNlcm5hbWU6cGFzc3dvcmQ=
```

The credentials are Base64-encoded in the format `username:password`. This method is simple but should only be used over HTTPS connections to ensure security.

### 2. Token-based Authentication (JWT)

JSON Web Tokens (JWT) provide a stateless authentication mechanism. After initial authentication, clients receive a token that can be used for subsequent requests.

```http
GET /api/devices HTTP/1.1
Host: example.traccar.org
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

Tokens are signed using a secure cryptographic algorithm and contain encoded user information and an expiration timestamp.

### 3. Session-based Authentication

After successful authentication, the API Gateway establishes a session and sets a session cookie. Subsequent requests that include this cookie are automatically authenticated.

```http
POST /api/session HTTP/1.1
Host: example.traccar.org
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "password123"
}
```

Response includes a session cookie:

```http
HTTP/1.1 200 OK
Set-Cookie: JSESSIONID=abcdef123456; Path=/; HttpOnly; Secure; SameSite=Strict
```

### 4. OpenID Connect

The API Gateway supports authentication via external identity providers using OpenID Connect protocol. This allows integration with enterprise identity systems.

## Identity Sources

The system supports multiple identity sources for user authentication:

### 1. Local Database

User credentials are stored in the Traccar database with passwords securely hashed.

### 2. LDAP/Active Directory

Authentication can be delegated to an LDAP server or Active Directory. Configure LDAP settings in the configuration file:

```properties
ldap.url=ldap://ldap.example.com:389
ldap.user=cn=admin,dc=example,dc=com
ldap.password=admin_password
ldap.base=ou=users,dc=example,dc=com
ldap.idAttribute=uid
ldap.nameAttribute=cn
ldap.mailAttribute=mail
ldap.searchFilter=(&(objectClass=inetOrgPerson)(uid={0}))
ldap.force=false
```

Set `ldap.force=true` to require all authentication to go through LDAP.

### 3. OpenID Connect

Authentication can be delegated to an OpenID Connect provider (Google, Microsoft, Okta, etc.).

```properties
openid.client_id=your_client_id
openid.client_secret=your_client_secret
openid.authorization_endpoint=https://accounts.google.com/o/oauth2/auth
openid.token_endpoint=https://oauth2.googleapis.com/token
openid.userinfo_endpoint=https://openidconnect.googleapis.com/v1/userinfo
openid.issuer=https://accounts.google.com
openid.force=false
```

Set `openid.force=true` to require all authentication to go through the OpenID provider.

## Multi-Factor Authentication (MFA)

The API Gateway supports Time-based One-Time Password (TOTP) as a second authentication factor. When enabled, users must provide a verification code from an authenticator app (like Google Authenticator) in addition to their password.

### Enabling MFA for a User

1. Generate a TOTP key for the user and store it in their profile
2. Display the QR code or secret key to the user for scanning with their authenticator app
3. Verify the first code to ensure proper setup

```http
POST /api/users/mfa HTTP/1.1
Host: example.traccar.org
Content-Type: application/json
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

{
  "userId": 123
}
```

Response includes the TOTP key and QR code URL:

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "totpKey": "JBSWY3DPEHPK3PXP",
  "qrCodeUrl": "otpauth://totp/Traccar:user@example.com?secret=JBSWY3DPEHPK3PXP&issuer=Traccar"
}
```

### Authentication with MFA

When MFA is enabled for a user, the authentication flow has two steps:

1. Initial authentication with username/password
2. Verification with TOTP code

```http
POST /api/session HTTP/1.1
Host: example.traccar.org
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "password123"
}
```

Response indicates MFA is required:

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json

{
  "error": "mfa_required"
}
```

Complete authentication with TOTP code:

```http
POST /api/session HTTP/1.1
Host: example.traccar.org
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "password123",
  "code": "123456"
}
```

### MFA Recovery Options

The system provides recovery mechanisms in case users lose access to their authenticator app:

1. **Recovery Codes**: Generate one-time use backup codes when MFA is enabled
2. **Administrator Reset**: Administrators can disable MFA for a user
3. **Secondary Email Verification**: Verify identity through a secondary email

## Service Account Authentication

Service accounts provide a way for automated systems to authenticate with the API without using user credentials. Service accounts use a special token that doesn't expire.

### Configuring a Service Account Token

Set the service account token in the configuration file:

```properties
web.serviceAccountToken=your_secure_token_here
```

### Using Service Account Authentication

```http
GET /api/devices HTTP/1.1
Host: example.traccar.org
Authorization: Bearer your_secure_token_here
```

Service accounts have system-level access and bypass MFA requirements.

## Token Lifecycle

### Token Generation

Tokens are generated upon successful authentication and contain the following information:
- User ID
- Expiration timestamp
- Digital signature
- Token issuance timestamp
- Authentication source information

Tokens are cryptographically signed to prevent tampering and ensure authenticity.

### Token Structure

The JWT token consists of three parts:
1. **Header**: Identifies the algorithm used for signing
2. **Payload**: Contains claims about the user and token validity
3. **Signature**: Ensures token integrity and authenticity

Example payload structure:
```json
{
  "u": 123,           // User ID
  "e": 1633046400000, // Expiration timestamp
  "iat": 1632441600000, // Issued at timestamp
  "src": "local"      // Authentication source
}
```

### Token Expiration

By default, tokens expire after 7 days. The expiration period can be configured in the server settings:

```properties
web.tokenExpirationDays=7
```

### Token Refresh

To obtain a new token before the current one expires, use the token refresh endpoint:

```http
POST /api/session/refresh HTTP/1.1
Host: example.traccar.org
Authorization: Bearer current_token_here
```

Response includes a new token with extended expiration:

```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expiration": "2023-10-01T00:00:00.000Z"
}
```

### Token Invalidation

Tokens can be invalidated in several ways:
1. Explicit logout by the user
2. Password change
3. Administrator action
4. Expiration
5. Security policy violation

To explicitly invalidate a token, call the logout endpoint:

```http
DELETE /api/session HTTP/1.1
Host: example.traccar.org
Authorization: Bearer token_to_invalidate
```

### Token Revocation List

The system maintains a token revocation list in the Redis cluster for tokens that have been explicitly invalidated before their expiration. This ensures that revoked tokens cannot be used even if they haven't expired yet.

## Session Management

Sessions are stored in a distributed Redis cluster to enable stateless API Gateway instances and horizontal scaling. This architecture allows for high availability and resilience in the session management system.

### Session Creation

Sessions are created upon successful authentication and stored with an expiration time. The session contains:
- User ID and principal information
- Session creation timestamp
- Expiration timestamp
- Authentication source information
- Client metadata (IP, user agent)

### Session Validation

Each request with a session cookie is validated against the session store. The validation process checks:
- Session existence in the store
- Session expiration status
- User account status (enabled/disabled)

### Session Heartbeat

Active sessions receive heartbeat signals to update their TTL (Time-To-Live) and prevent premature expiration. The heartbeat mechanism includes:
- TTL refresh on each authenticated request
- Background TTL refresh service for active sessions
- Configurable heartbeat intervals

### Session Termination

Sessions can be terminated by:
1. Explicit logout - User initiates logout through the API
2. Inactivity timeout - Session expires after a period of inactivity
3. Administrator action - Admin forcibly terminates user sessions
4. Kubernetes pod termination - Graceful cleanup during scaling or updates

### Kubernetes Integration

The session management system integrates with Kubernetes lifecycle hooks:
- Liveness probes verify Redis session store health
- Readiness probes check session cache availability
- PreStop handlers trigger session cleanup during controlled termination

## Security Considerations

### Transport Security

Always use HTTPS for all API communications to protect authentication credentials and tokens.

### Token Storage

Clients should store tokens securely:
- Web applications: Use HttpOnly, Secure cookies with appropriate SameSite policy
- Mobile applications: Use secure storage mechanisms (Keychain, KeyStore)
- Desktop applications: Use OS-level secure storage

### Password Policies

Implement strong password policies including:
- Minimum length requirements
- Complexity requirements (uppercase, lowercase, numbers, special characters)
- Password expiration
- Account lockout after failed attempts

### Service-to-Service Authentication

Internal service-to-service communication uses either:
- JWT token propagation - The authenticated user context is passed to downstream services
- Mutual TLS (mTLS) - Services authenticate each other using certificates

This ensures that only properly authenticated services can communicate with each other within the microservices architecture.

## Troubleshooting

### Common Authentication Errors

| HTTP Status | Error Code | Description | Solution |
|-------------|------------|-------------|----------|
| 401 | invalid_credentials | Username or password is incorrect | Verify credentials |
| 401 | mfa_required | Multi-factor authentication required | Submit TOTP code |
| 401 | invalid_token | Token is invalid or expired | Re-authenticate or refresh token |
| 401 | token_revoked | Token has been explicitly revoked | Re-authenticate with credentials |
| 403 | account_disabled | User account is disabled | Contact administrator |
| 403 | insufficient_permissions | User lacks required permissions | Request additional permissions |
| 429 | too_many_attempts | Too many failed authentication attempts | Wait for lockout period to expire |
| 500 | auth_service_error | Authentication service unavailable | Retry later or contact support |

### Logging

Authentication failures are logged with appropriate detail for troubleshooting while protecting sensitive information. Logs include:

- Timestamp of authentication attempt
- Username (partially masked)
- IP address of client
- Authentication method used
- Error type
- Request ID for correlation

Example log entry:
```
2023-09-24 15:30:45.123 INFO [auth-service] - Authentication failed for user u***@example.com from 192.168.1.1: invalid_credentials [request-id: abc123]
```

### Debugging Authentication Issues

1. **Check client configuration**:
   - Ensure correct API endpoint URLs
   - Verify credentials are being sent correctly
   - Check token format and expiration

2. **Server-side verification**:
   - Check server logs for detailed error information
   - Verify identity provider connectivity
   - Check Redis cluster health for session storage

3. **Network troubleshooting**:
   - Ensure firewall rules allow API Gateway access
   - Verify TLS/SSL configuration
   - Check for network latency issues

## API Reference

### Authentication Endpoints

| Endpoint | Method | Description | Authentication Required |
|----------|--------|-------------|------------------------|
| /api/session | POST | Create a new session (login) | No |
| /api/session | GET | Get current session information | Yes |
| /api/session | DELETE | End current session (logout) | Yes |
| /api/session/refresh | POST | Refresh authentication token | Yes |
| /api/users/mfa | POST | Enable MFA for a user | Yes |
| /api/users/mfa | DELETE | Disable MFA for a user | Yes |
| /api/users/mfa/verify | POST | Verify MFA setup | Yes |
| /api/users/mfa/recovery | GET | Generate recovery codes | Yes |

### Request and Response Examples

#### Login (Create Session)

Request:
```http
POST /api/session HTTP/1.1
Host: example.traccar.org
Content-Type: application/json

{
  "email": "user@example.com",
  "password": "password123"
}
```

Success Response:
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "id": 123,
  "name": "John Doe",
  "email": "user@example.com",
  "administrator": false,
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expiration": "2023-10-01T00:00:00.000Z"
}
```

#### Get Current Session

Request:
```http
GET /api/session HTTP/1.1
Host: example.traccar.org
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

Response:
```http
HTTP/1.1 200 OK
Content-Type: application/json

{
  "id": 123,
  "name": "John Doe",
  "email": "user@example.com",
  "administrator": false
}
```

#### Logout (End Session)

Request:
```http
DELETE /api/session HTTP/1.1
Host: example.traccar.org
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

Response:
```http
HTTP/1.1 204 No Content
```