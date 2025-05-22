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

import org.traccar.model.BaseModel;
import org.traccar.storage.StorageName;

import java.util.Date;

/**
 * Model for storing cryptographic keys, certificates, and related metadata.
 * Supports key rotation, expiration tracking, and multiple key types.
 */
@StorageName("tc_keystore")
public class KeystoreModel extends BaseModel {

    /**
     * Enumeration of supported key types.
     */
    public enum KeyType {
        RSA,
        ECDSA,
        ED25519,
        AES,
        HMAC
    }

    /**
     * Enumeration of key status values.
     */
    public enum KeyStatus {
        ACTIVE,      // Key is currently in use
        INACTIVE,    // Key exists but is not currently in use
        EXPIRED,     // Key has reached its expiration date
        REVOKED,     // Key has been explicitly revoked
        ROTATING,    // Key is in the process of being rotated
        ARCHIVED     // Key is archived for historical purposes
    }

    private byte[] publicKey;
    private byte[] privateKey;
    
    // Certificate fields for mTLS
    private byte[] certificate;
    private byte[] certificateChain;
    
    // Key type and algorithm information
    private KeyType keyType;
    private int keySize;
    private String algorithm;
    
    // Key rotation and expiration tracking
    private Date createdAt;
    private Date expiresAt;
    private Date rotationDue;
    private KeyStatus status;
    
    // Versioning for key rotation
    private int version;
    private boolean active;
    private Long previousVersionId;
    private String keyIdentifier; // Unique identifier for this key

    /**
     * Gets the public key data.
     * 
     * @return The public key as a byte array
     */
    public byte[] getPublicKey() {
        return publicKey;
    }

    /**
     * Sets the public key data.
     * 
     * @param publicKey The public key as a byte array
     */
    public void setPublicKey(byte[] publicKey) {
        this.publicKey = publicKey;
    }

    /**
     * Gets the private key data.
     * 
     * @return The private key as a byte array
     */
    public byte[] getPrivateKey() {
        return privateKey;
    }

    /**
     * Sets the private key data.
     * 
     * @param privateKey The private key as a byte array
     */
    public void setPrivateKey(byte[] privateKey) {
        this.privateKey = privateKey;
    }
    
    /**
     * Gets the certificate data for mTLS.
     * 
     * @return The certificate as a byte array
     */
    public byte[] getCertificate() {
        return certificate;
    }
    
    /**
     * Sets the certificate data for mTLS.
     * 
     * @param certificate The certificate as a byte array
     */
    public void setCertificate(byte[] certificate) {
        this.certificate = certificate;
    }
    
    /**
     * Gets the certificate chain data for mTLS.
     * 
     * @return The certificate chain as a byte array
     */
    public byte[] getCertificateChain() {
        return certificateChain;
    }
    
    /**
     * Sets the certificate chain data for mTLS.
     * 
     * @param certificateChain The certificate chain as a byte array
     */
    public void setCertificateChain(byte[] certificateChain) {
        this.certificateChain = certificateChain;
    }
    
    /**
     * Gets the type of this key.
     * 
     * @return The key type
     */
    public KeyType getKeyType() {
        return keyType;
    }
    
    /**
     * Sets the type of this key.
     * 
     * @param keyType The key type to set
     */
    public void setKeyType(KeyType keyType) {
        this.keyType = keyType;
    }
    
    /**
     * Gets the size of this key in bits.
     * 
     * @return The key size
     */
    public int getKeySize() {
        return keySize;
    }
    
    /**
     * Sets the size of this key in bits.
     * 
     * @param keySize The key size to set
     */
    public void setKeySize(int keySize) {
        this.keySize = keySize;
    }
    
    /**
     * Gets the specific algorithm used for this key.
     * 
     * @return The algorithm name
     */
    public String getAlgorithm() {
        return algorithm;
    }
    
    /**
     * Sets the specific algorithm used for this key.
     * 
     * @param algorithm The algorithm name to set
     */
    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }
    
    /**
     * Gets the creation date of this key.
     * 
     * @return The creation date
     */
    public Date getCreatedAt() {
        return createdAt;
    }
    
    /**
     * Sets the creation date of this key.
     * 
     * @param createdAt The creation date to set
     */
    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }
    
    /**
     * Gets the expiration date of this key.
     * 
     * @return The expiration date
     */
    public Date getExpiresAt() {
        return expiresAt;
    }
    
    /**
     * Sets the expiration date of this key.
     * 
     * @param expiresAt The expiration date to set
     */
    public void setExpiresAt(Date expiresAt) {
        this.expiresAt = expiresAt;
    }
    
    /**
     * Gets the date when this key should be rotated.
     * 
     * @return The rotation due date
     */
    public Date getRotationDue() {
        return rotationDue;
    }
    
    /**
     * Sets the date when this key should be rotated.
     * 
     * @param rotationDue The rotation due date to set
     */
    public void setRotationDue(Date rotationDue) {
        this.rotationDue = rotationDue;
    }
    
    /**
     * Gets the current status of this key.
     * 
     * @return The key status
     */
    public KeyStatus getStatus() {
        return status;
    }
    
    /**
     * Sets the current status of this key.
     * 
     * @param status The key status to set
     */
    public void setStatus(KeyStatus status) {
        this.status = status;
    }
    
    /**
     * Gets the version number of this key.
     * 
     * @return The version number
     */
    public int getVersion() {
        return version;
    }
    
    /**
     * Sets the version number of this key.
     * 
     * @param version The version number to set
     */
    public void setVersion(int version) {
        this.version = version;
    }
    
    /**
     * Checks if this key is currently active.
     * 
     * @return true if the key is active, false otherwise
     */
    public boolean isActive() {
        return active;
    }
    
    /**
     * Sets whether this key is currently active.
     * 
     * @param active The active status to set
     */
    public void setActive(boolean active) {
        this.active = active;
    }
    
    /**
     * Gets the ID of the previous version of this key.
     * 
     * @return The previous version ID, or null if this is the first version
     */
    public Long getPreviousVersionId() {
        return previousVersionId;
    }
    
    /**
     * Sets the ID of the previous version of this key.
     * 
     * @param previousVersionId The previous version ID to set
     */
    public void setPreviousVersionId(Long previousVersionId) {
        this.previousVersionId = previousVersionId;
    }
    
    /**
     * Gets the unique identifier for this key.
     * 
     * @return The key identifier
     */
    public String getKeyIdentifier() {
        return keyIdentifier;
    }
    
    /**
     * Sets the unique identifier for this key.
     * 
     * @param keyIdentifier The key identifier to set
     */
    public void setKeyIdentifier(String keyIdentifier) {
        this.keyIdentifier = keyIdentifier;
    }
    
    /**
     * Checks if this key has expired.
     * 
     * @return true if the key has expired, false otherwise
     */
    public boolean isExpired() {
        if (expiresAt == null) {
            return false;
        }
        return expiresAt.before(new Date());
    }
    
    /**
     * Checks if this key is due for rotation.
     * 
     * @return true if the key is due for rotation, false otherwise
     */
    public boolean isRotationDue() {
        if (rotationDue == null) {
            return false;
        }
        return rotationDue.before(new Date());
    }
}
