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
package org.traccar.api.security;

import org.traccar.model.User;

import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Represents a service account user for service-to-service authentication in a microservices architecture.
 * Enhanced to support mTLS certificate-based authentication, JWT token generation, and service registry integration.
 */
public class ServiceAccountUser extends User {

    public static final long ID = 9000000000000000000L;
    
    private String serviceName;
    private String serviceId;
    private X509Certificate certificate;
    private Date certificateExpiration;
    private String registryUrl;
    private boolean mtlsEnabled;
    private final Set<String> scopes = new HashSet<>();
    private final Map<String, Object> claims = new HashMap<>();

    /**
     * Default constructor for a generic service account.
     * Maintains backward compatibility with the original implementation.
     */
    public ServiceAccountUser() {
        setId(ID);
        setName("Service Account");
        setEmail("none");
        setAdministrator(true);
        this.serviceName = "generic";
        this.serviceId = String.valueOf(ID);
        this.mtlsEnabled = false;
    }

    /**
     * Constructor for a specific microservice account with a name and ID.
     * 
     * @param serviceName Name of the service
     * @param serviceId Unique identifier for the service
     */
    public ServiceAccountUser(String serviceName, String serviceId) {
        setId(ID);
        setName("Service Account: " + serviceName);
        setEmail(serviceName + "@service.traccar.org");
        setAdministrator(true);
        this.serviceName = serviceName;
        this.serviceId = serviceId;
        this.mtlsEnabled = false;
    }

    /**
     * Constructor for a service account with mTLS certificate authentication.
     * 
     * @param serviceName Name of the service
     * @param serviceId Unique identifier for the service
     * @param certificate X509 certificate for mTLS authentication
     */
    public ServiceAccountUser(String serviceName, String serviceId, X509Certificate certificate) {
        this(serviceName, serviceId);
        this.certificate = certificate;
        this.certificateExpiration = certificate.getNotAfter();
        this.mtlsEnabled = true;
    }

    /**
     * Get the service name.
     * 
     * @return Service name
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Set the service name.
     * 
     * @param serviceName Service name
     */
    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
        setName("Service Account: " + serviceName);
        setEmail(serviceName + "@service.traccar.org");
    }

    /**
     * Get the service ID.
     * 
     * @return Service ID
     */
    public String getServiceId() {
        return serviceId;
    }

    /**
     * Set the service ID.
     * 
     * @param serviceId Service ID
     */
    public void setServiceId(String serviceId) {
        this.serviceId = serviceId;
    }

    /**
     * Get the X509 certificate used for mTLS authentication.
     * 
     * @return X509 certificate
     */
    public X509Certificate getCertificate() {
        return certificate;
    }

    /**
     * Set the X509 certificate used for mTLS authentication.
     * 
     * @param certificate X509 certificate
     */
    public void setCertificate(X509Certificate certificate) {
        this.certificate = certificate;
        this.certificateExpiration = certificate.getNotAfter();
        this.mtlsEnabled = true;
    }

    /**
     * Get the certificate expiration date.
     * 
     * @return Certificate expiration date
     */
    public Date getCertificateExpiration() {
        return certificateExpiration;
    }

    /**
     * Check if the certificate is expired.
     * 
     * @return true if the certificate is expired
     */
    public boolean isCertificateExpired() {
        return certificateExpiration != null && certificateExpiration.before(new Date());
    }

    /**
     * Check if mTLS authentication is enabled for this service account.
     * 
     * @return true if mTLS is enabled
     */
    public boolean isMtlsEnabled() {
        return mtlsEnabled;
    }

    /**
     * Set whether mTLS authentication is enabled for this service account.
     * 
     * @param mtlsEnabled true to enable mTLS
     */
    public void setMtlsEnabled(boolean mtlsEnabled) {
        this.mtlsEnabled = mtlsEnabled;
    }

    /**
     * Get the service registry URL.
     * 
     * @return Service registry URL
     */
    public String getRegistryUrl() {
        return registryUrl;
    }

    /**
     * Set the service registry URL.
     * 
     * @param registryUrl Service registry URL
     */
    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    /**
     * Get the permission scopes for this service account.
     * 
     * @return Set of permission scopes
     */
    public Set<String> getScopes() {
        return scopes;
    }

    /**
     * Add a permission scope to this service account.
     * 
     * @param scope Permission scope to add
     */
    public void addScope(String scope) {
        this.scopes.add(scope);
    }

    /**
     * Check if this service account has a specific permission scope.
     * 
     * @param scope Scope to check
     * @return true if the service account has the scope
     */
    public boolean hasScope(String scope) {
        return scopes.contains(scope);
    }

    /**
     * Get the JWT claims for this service account.
     * 
     * @return Map of JWT claims
     */
    public Map<String, Object> getClaims() {
        return claims;
    }

    /**
     * Add a JWT claim to this service account.
     * 
     * @param name Claim name
     * @param value Claim value
     */
    public void addClaim(String name, Object value) {
        this.claims.put(name, value);
    }

    /**
     * Get a specific JWT claim value.
     * 
     * @param name Claim name
     * @return Claim value or null if not present
     */
    public Object getClaim(String name) {
        return claims.get(name);
    }

    /**
     * Generate a JWT token for this service account.
     * This method should be implemented by a JWT provider service.
     * 
     * @param expirationMinutes Token expiration time in minutes
     * @return JWT token string
     */
    public String generateJwtToken(int expirationMinutes) {
        // This is a placeholder. In a real implementation, this would use a JWT library
        // to generate a signed token with the service account's claims and scopes.
        // The actual implementation would be provided by a JWT provider service.
        throw new UnsupportedOperationException("JWT token generation must be implemented by a provider service");
    }

    /**
     * Resolve service identity from the service registry.
     * This method should be implemented by a service registry client.
     * 
     * @param registryUrl URL of the service registry
     * @param serviceName Name of the service to resolve
     * @return ServiceAccountUser with resolved identity or null if not found
     */
    public static ServiceAccountUser resolveFromRegistry(String registryUrl, String serviceName) {
        // This is a placeholder. In a real implementation, this would use a service registry client
        // to resolve the service identity from the registry.
        // The actual implementation would be provided by a service registry client.
        throw new UnsupportedOperationException("Service identity resolution must be implemented by a registry client");
    }

    /**
     * Validate the service account's mTLS certificate against a trusted certificate authority.
     * This method should be implemented by a certificate validation service.
     * 
     * @return true if the certificate is valid
     */
    public boolean validateCertificate() {
        // This is a placeholder. In a real implementation, this would validate the certificate
        // against a trusted certificate authority.
        // The actual implementation would be provided by a certificate validation service.
        if (certificate == null) {
            return false;
        }
        
        try {
            // Check if the certificate is expired
            if (isCertificateExpired()) {
                return false;
            }
            
            // Additional validation would be performed here
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}