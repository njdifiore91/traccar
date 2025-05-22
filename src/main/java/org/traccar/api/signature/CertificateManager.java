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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.storage.Storage;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.CRL;
import java.security.cert.CRLException;
import java.security.cert.CRLReason;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateExpiredException;
import java.security.cert.CertificateFactory;
import java.security.cert.CertificateNotYetValidException;
import java.security.cert.CertificateRevokedException;
import java.security.cert.X509CRL;
import java.security.cert.X509CRLEntry;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.security.auth.x500.X500Principal;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.asn1.x509.KeyUsage;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CRLHolder;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v2CRLBuilder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CRLConverter;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509ExtensionUtils;
import org.bouncycastle.cert.jcajce.JcaX509v2CRLBuilder;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * Manages mTLS certificate lifecycle including generation, storage, retrieval, and validation.
 * Supports certificate rotation, revocation checking, and integration with service discovery.
 */
@Singleton
public class CertificateManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(CertificateManager.class);
    private static final String CIRCUIT_BREAKER_NAME = "certificateStorageCircuitBreaker";
    private static final String TRACER_INSTRUMENTATION_NAME = "org.traccar.api.signature";
    private static final String CERTIFICATE_ALGORITHM = "RSA";
    private static final int CERTIFICATE_KEY_SIZE = 2048;
    private static final long CERTIFICATE_VALIDITY_DAYS = 365; // 1 year
    private static final long CERTIFICATE_RENEWAL_THRESHOLD_DAYS = 30; // 30 days before expiration
    private static final String KEYSTORE_TYPE = "PKCS12";
    private static final String KEYSTORE_PASSWORD = "traccar";
    private static final String SIGNATURE_ALGORITHM = "SHA256withRSA";
    private static final String ROOT_CA_ALIAS = "traccar-root-ca";
    private static final String INTERMEDIATE_CA_ALIAS = "traccar-intermediate-ca";
    private static final String CERTIFICATE_ISSUER = "CN=Traccar Certificate Authority,O=Traccar,C=US";

    private final Storage storage;
    private final CircuitBreaker circuitBreaker;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;

    // Metrics
    private final Counter generateCounter;
    private final Counter validateCounter;
    private final Counter revokeCounter;
    private final Counter renewCounter;
    private final Counter failureCounter;
    private final Timer generateTimer;
    private final Timer validateTimer;
    private final Timer retrieveTimer;
    private final Timer revokeTimer;
    private final Timer renewTimer;

    // Cache for certificates
    private final Map<String, CertificateInfo> certificateCache = new HashMap<>();
    private X509Certificate rootCaCertificate;
    private PrivateKey rootCaPrivateKey;
    private X509Certificate intermediateCaCertificate;
    private PrivateKey intermediateCaPrivateKey;
    private X509CRL certificateRevocationList;

    /**
     * Constructs a new CertificateManager with the specified dependencies.
     *
     * @param storage The storage interface for persisting certificates
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param meterRegistry The Micrometer registry for metrics
     */
    @Inject
    public CertificateManager(Storage storage, Tracer tracer, MeterRegistry meterRegistry) {
        this.storage = storage;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;

        // Initialize BouncyCastle provider
        java.security.Security.addProvider(new BouncyCastleProvider());

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
        this.generateCounter = Counter.builder("certificate.generate.count")
                .description("Number of certificate generation operations")
                .register(meterRegistry);
        this.validateCounter = Counter.builder("certificate.validate.count")
                .description("Number of certificate validation operations")
                .register(meterRegistry);
        this.revokeCounter = Counter.builder("certificate.revoke.count")
                .description("Number of certificate revocation operations")
                .register(meterRegistry);
        this.renewCounter = Counter.builder("certificate.renew.count")
                .description("Number of certificate renewal operations")
                .register(meterRegistry);
        this.failureCounter = Counter.builder("certificate.operation.failures")
                .description("Number of failed certificate operations")
                .register(meterRegistry);
        this.generateTimer = Timer.builder("certificate.generate.time")
                .description("Time taken for certificate generation operations")
                .register(meterRegistry);
        this.validateTimer = Timer.builder("certificate.validate.time")
                .description("Time taken for certificate validation operations")
                .register(meterRegistry);
        this.retrieveTimer = Timer.builder("certificate.retrieve.time")
                .description("Time taken for certificate retrieval operations")
                .register(meterRegistry);
        this.revokeTimer = Timer.builder("certificate.revoke.time")
                .description("Time taken for certificate revocation operations")
                .register(meterRegistry);
        this.renewTimer = Timer.builder("certificate.renew.time")
                .description("Time taken for certificate renewal operations")
                .register(meterRegistry);

        // Initialize CA certificates
        try {
            initializeCaCertificates();
        } catch (Exception e) {
            LOGGER.error("Failed to initialize CA certificates", e);
            failureCounter.increment();
        }
    }

    /**
     * Initializes the root and intermediate CA certificates.
     *
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     * @throws OperatorCreationException If an error occurs creating the certificate signer
     */
    private void initializeCaCertificates() throws GeneralSecurityException, StorageException, IOException, OperatorCreationException {
        Span span = tracer.spanBuilder("initializeCaCertificates").startSpan();
        try (Scope scope = span.makeCurrent()) {
            // Check if root CA certificate exists in storage
            CertificateModel rootCaModel = executeWithCircuitBreaker(() -> {
                try {
                    return storage.getObject(CertificateModel.class,
                            new Request(new Condition.Equals("alias", ROOT_CA_ALIAS)));
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                }
            });

            if (rootCaModel == null) {
                // Generate root CA certificate
                KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(CERTIFICATE_ALGORITHM);
                keyPairGenerator.initialize(CERTIFICATE_KEY_SIZE, new SecureRandom());
                KeyPair rootKeyPair = keyPairGenerator.generateKeyPair();

                X500Name rootIssuer = new X500Name("CN=Traccar Root CA,O=Traccar,C=US");
                BigInteger rootSerialNumber = new BigInteger(64, new SecureRandom());
                Date startDate = new Date();
                Date endDate = new Date(startDate.getTime() + TimeUnit.DAYS.toMillis(CERTIFICATE_VALIDITY_DAYS * 2)); // Root CA valid for 2 years

                X509v3CertificateBuilder rootCertBuilder = new JcaX509v3CertificateBuilder(
                        rootIssuer,
                        rootSerialNumber,
                        startDate,
                        endDate,
                        rootIssuer,
                        rootKeyPair.getPublic());

                // Add extensions
                JcaX509ExtensionUtils extensionUtils = new JcaX509ExtensionUtils();
                rootCertBuilder.addExtension(
                        Extension.basicConstraints,
                        true,
                        new BasicConstraints(true));
                rootCertBuilder.addExtension(
                        Extension.keyUsage,
                        true,
                        new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
                rootCertBuilder.addExtension(
                        Extension.subjectKeyIdentifier,
                        false,
                        extensionUtils.createSubjectKeyIdentifier(rootKeyPair.getPublic()));

                ContentSigner rootContentSigner = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
                        .build(rootKeyPair.getPrivate());
                X509CertificateHolder rootCertHolder = rootCertBuilder.build(rootContentSigner);
                X509Certificate rootCert = new JcaX509CertificateConverter()
                        .getCertificate(rootCertHolder);

                // Store root CA certificate
                rootCaModel = new CertificateModel();
                rootCaModel.setAlias(ROOT_CA_ALIAS);
                rootCaModel.setSubject("CN=Traccar Root CA,O=Traccar,C=US");
                rootCaModel.setIssuer("CN=Traccar Root CA,O=Traccar,C=US");
                rootCaModel.setSerialNumber(rootSerialNumber.toString());
                rootCaModel.setNotBefore(startDate);
                rootCaModel.setNotAfter(endDate);
                rootCaModel.setCertificate(rootCert.getEncoded());
                rootCaModel.setPrivateKey(rootKeyPair.getPrivate().getEncoded());
                rootCaModel.setCreatedAt(new Date());
                rootCaModel.setRevoked(false);

                executeWithCircuitBreaker(() -> {
                    try {
                        storage.addObject(rootCaModel, new Request(new Columns.Exclude("id")));
                        return null;
                    } catch (StorageException e) {
                        throw new RuntimeException(e);
                    }
                });

                rootCaCertificate = rootCert;
                rootCaPrivateKey = rootKeyPair.getPrivate();

                // Generate intermediate CA certificate
                KeyPair intermediateKeyPair = keyPairGenerator.generateKeyPair();
                X500Name intermediateSubject = new X500Name("CN=Traccar Intermediate CA,O=Traccar,C=US");
                BigInteger intermediateSerialNumber = new BigInteger(64, new SecureRandom());

                X509v3CertificateBuilder intermediateCertBuilder = new JcaX509v3CertificateBuilder(
                        rootIssuer,
                        intermediateSerialNumber,
                        startDate,
                        endDate,
                        intermediateSubject,
                        intermediateKeyPair.getPublic());

                // Add extensions
                intermediateCertBuilder.addExtension(
                        Extension.basicConstraints,
                        true,
                        new BasicConstraints(true));
                intermediateCertBuilder.addExtension(
                        Extension.keyUsage,
                        true,
                        new KeyUsage(KeyUsage.keyCertSign | KeyUsage.cRLSign));
                intermediateCertBuilder.addExtension(
                        Extension.subjectKeyIdentifier,
                        false,
                        extensionUtils.createSubjectKeyIdentifier(intermediateKeyPair.getPublic()));
                intermediateCertBuilder.addExtension(
                        Extension.authorityKeyIdentifier,
                        false,
                        extensionUtils.createAuthorityKeyIdentifier(rootKeyPair.getPublic()));

                ContentSigner intermediateContentSigner = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
                        .build(rootKeyPair.getPrivate());
                X509CertificateHolder intermediateCertHolder = intermediateCertBuilder.build(intermediateContentSigner);
                X509Certificate intermediateCert = new JcaX509CertificateConverter()
                        .getCertificate(intermediateCertHolder);

                // Store intermediate CA certificate
                CertificateModel intermediateCaModel = new CertificateModel();
                intermediateCaModel.setAlias(INTERMEDIATE_CA_ALIAS);
                intermediateCaModel.setSubject("CN=Traccar Intermediate CA,O=Traccar,C=US");
                intermediateCaModel.setIssuer("CN=Traccar Root CA,O=Traccar,C=US");
                intermediateCaModel.setSerialNumber(intermediateSerialNumber.toString());
                intermediateCaModel.setNotBefore(startDate);
                intermediateCaModel.setNotAfter(endDate);
                intermediateCaModel.setCertificate(intermediateCert.getEncoded());
                intermediateCaModel.setPrivateKey(intermediateKeyPair.getPrivate().getEncoded());
                intermediateCaModel.setCreatedAt(new Date());
                intermediateCaModel.setRevoked(false);

                executeWithCircuitBreaker(() -> {
                    try {
                        storage.addObject(intermediateCaModel, new Request(new Columns.Exclude("id")));
                        return null;
                    } catch (StorageException e) {
                        throw new RuntimeException(e);
                    }
                });

                intermediateCaCertificate = intermediateCert;
                intermediateCaPrivateKey = intermediateKeyPair.getPrivate();

                // Initialize empty CRL
                initializeCrl();
            } else {
                // Load existing root CA certificate
                CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                rootCaCertificate = (X509Certificate) certFactory.generateCertificate(
                        new ByteArrayInputStream(rootCaModel.getCertificate()));
                rootCaPrivateKey = KeyFactory.getInstance(CERTIFICATE_ALGORITHM)
                        .generatePrivate(new PKCS8EncodedKeySpec(rootCaModel.getPrivateKey()));

                // Load existing intermediate CA certificate
                CertificateModel intermediateCaModel = executeWithCircuitBreaker(() -> {
                    try {
                        return storage.getObject(CertificateModel.class,
                                new Request(new Condition.Equals("alias", INTERMEDIATE_CA_ALIAS)));
                    } catch (StorageException e) {
                        throw new RuntimeException(e);
                    }
                });

                if (intermediateCaModel != null) {
                    intermediateCaCertificate = (X509Certificate) certFactory.generateCertificate(
                            new ByteArrayInputStream(intermediateCaModel.getCertificate()));
                    intermediateCaPrivateKey = KeyFactory.getInstance(CERTIFICATE_ALGORITHM)
                            .generatePrivate(new PKCS8EncodedKeySpec(intermediateCaModel.getPrivateKey()));
                } else {
                    throw new GeneralSecurityException("Intermediate CA certificate not found");
                }

                // Load CRL
                loadCrl();
            }
        } catch (Exception e) {
            failureCounter.increment();
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
        }
    }

    /**
     * Initializes an empty Certificate Revocation List (CRL).
     *
     * @throws GeneralSecurityException If a security error occurs
     * @throws IOException If an I/O error occurs
     * @throws OperatorCreationException If an error occurs creating the CRL signer
     */
    private void initializeCrl() throws GeneralSecurityException, IOException, OperatorCreationException {
        X500Name issuer = new X500Name(intermediateCaCertificate.getSubjectX500Principal().getName());
        Date now = new Date();
        Date nextUpdate = new Date(now.getTime() + TimeUnit.DAYS.toMillis(1)); // CRL valid for 1 day

        X509v2CRLBuilder crlBuilder = new JcaX509v2CRLBuilder(issuer, now);
        crlBuilder.setNextUpdate(nextUpdate);

        JcaContentSignerBuilder contentSignerBuilder = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM);
        ContentSigner contentSigner = contentSignerBuilder.build(intermediateCaPrivateKey);
        X509CRLHolder crlHolder = crlBuilder.build(contentSigner);

        JcaX509CRLConverter converter = new JcaX509CRLConverter();
        certificateRevocationList = converter.getCRL(crlHolder);

        // Store CRL in database
        CrlModel crlModel = new CrlModel();
        crlModel.setCrl(certificateRevocationList.getEncoded());
        crlModel.setLastUpdate(now);
        crlModel.setNextUpdate(nextUpdate);

        executeWithCircuitBreaker(() -> {
            try {
                storage.addObject(crlModel, new Request(new Columns.Exclude("id")));
                return null;
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Loads the Certificate Revocation List (CRL) from storage.
     *
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     */
    private void loadCrl() throws GeneralSecurityException, StorageException, IOException {
        CrlModel crlModel = executeWithCircuitBreaker(() -> {
            try {
                return storage.getObject(CrlModel.class, new Request(new Columns.All()));
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });

        if (crlModel != null) {
            CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
            certificateRevocationList = (X509CRL) certFactory.generateCRL(
                    new ByteArrayInputStream(crlModel.getCrl()));

            // Check if CRL needs to be updated
            Date now = new Date();
            if (certificateRevocationList.getNextUpdate().before(now)) {
                updateCrl(new ArrayList<>());
            }
        } else {
            // Initialize empty CRL if not found
            initializeCrl();
        }
    }

    /**
     * Updates the Certificate Revocation List (CRL) with newly revoked certificates.
     *
     * @param revokedSerialNumbers List of serial numbers of newly revoked certificates
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     * @throws OperatorCreationException If an error occurs creating the CRL signer
     */
    private void updateCrl(List<BigInteger> revokedSerialNumbers) 
            throws GeneralSecurityException, StorageException, IOException, OperatorCreationException {
        X500Name issuer = new X500Name(intermediateCaCertificate.getSubjectX500Principal().getName());
        Date now = new Date();
        Date nextUpdate = new Date(now.getTime() + TimeUnit.DAYS.toMillis(1)); // CRL valid for 1 day

        X509v2CRLBuilder crlBuilder = new JcaX509v2CRLBuilder(issuer, now);
        crlBuilder.setNextUpdate(nextUpdate);

        // Add existing revoked certificates
        if (certificateRevocationList != null) {
            Set<? extends X509CRLEntry> revokedCertificates = certificateRevocationList.getRevokedCertificates();
            if (revokedCertificates != null) {
                for (X509CRLEntry entry : revokedCertificates) {
                    crlBuilder.addCRLEntry(entry.getSerialNumber(), entry.getRevocationDate(), CRLReason.privilegeWithdrawn.ordinal());
                }
            }
        }

        // Add newly revoked certificates
        for (BigInteger serialNumber : revokedSerialNumbers) {
            crlBuilder.addCRLEntry(serialNumber, now, CRLReason.privilegeWithdrawn.ordinal());
        }

        JcaContentSignerBuilder contentSignerBuilder = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM);
        ContentSigner contentSigner = contentSignerBuilder.build(intermediateCaPrivateKey);
        X509CRLHolder crlHolder = crlBuilder.build(contentSigner);

        JcaX509CRLConverter converter = new JcaX509CRLConverter();
        certificateRevocationList = converter.getCRL(crlHolder);

        // Update CRL in database
        CrlModel crlModel = executeWithCircuitBreaker(() -> {
            try {
                return storage.getObject(CrlModel.class, new Request(new Columns.All()));
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });

        if (crlModel == null) {
            crlModel = new CrlModel();
        }

        crlModel.setCrl(certificateRevocationList.getEncoded());
        crlModel.setLastUpdate(now);
        crlModel.setNextUpdate(nextUpdate);

        executeWithCircuitBreaker(() -> {
            try {
                if (crlModel.getId() != 0) {
                    storage.updateObject(crlModel, new Request(new Columns.All()));
                } else {
                    storage.addObject(crlModel, new Request(new Columns.Exclude("id")));
                }
                return null;
            } catch (StorageException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Generates a new certificate for a service.
     *
     * @param serviceId The service identifier
     * @param commonName The common name for the certificate (typically service DNS name)
     * @param alternativeNames List of Subject Alternative Names (SANs) for the certificate
     * @return The generated certificate information
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     * @throws OperatorCreationException If an error occurs creating the certificate signer
     */
    public CertificateInfo generateCertificate(String serviceId, String commonName, List<String> alternativeNames) 
            throws GeneralSecurityException, StorageException, IOException, OperatorCreationException {
        Span span = tracer.spanBuilder("generateCertificate").startSpan();
        span.setAttribute("serviceId", serviceId);
        span.setAttribute("commonName", commonName);
        try (Scope scope = span.makeCurrent()) {
            generateCounter.increment();
            return generateTimer.record(() -> {
                try {
                    // Check if certificate already exists
                    CertificateModel existingCert = executeWithCircuitBreaker(() -> {
                        try {
                            return storage.getObject(CertificateModel.class,
                                    new Request(new Condition.Equals("alias", serviceId)));
                        } catch (StorageException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    if (existingCert != null && !existingCert.isRevoked()) {
                        // Check if certificate is still valid
                        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                        X509Certificate cert = (X509Certificate) certFactory.generateCertificate(
                                new ByteArrayInputStream(existingCert.getCertificate()));
                        PrivateKey privateKey = KeyFactory.getInstance(CERTIFICATE_ALGORITHM)
                                .generatePrivate(new PKCS8EncodedKeySpec(existingCert.getPrivateKey()));

                        try {
                            cert.checkValidity();
                            // Check if certificate needs renewal
                            Date renewalDate = new Date(System.currentTimeMillis() 
                                    + TimeUnit.DAYS.toMillis(CERTIFICATE_RENEWAL_THRESHOLD_DAYS));
                            if (cert.getNotAfter().before(renewalDate)) {
                                // Certificate is approaching expiration, renew it
                                return renewCertificate(serviceId, commonName, alternativeNames);
                            } else {
                                // Certificate is still valid, return it
                                CertificateInfo certInfo = new CertificateInfo(serviceId, cert, privateKey);
                                certificateCache.put(serviceId, certInfo);
                                return certInfo;
                            }
                        } catch (CertificateExpiredException | CertificateNotYetValidException e) {
                            // Certificate is expired or not yet valid, generate a new one
                            LOGGER.info("Certificate for service {} is expired or not yet valid, generating new one", serviceId);
                        }
                    }

                    // Generate new key pair
                    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(CERTIFICATE_ALGORITHM);
                    keyPairGenerator.initialize(CERTIFICATE_KEY_SIZE, new SecureRandom());
                    KeyPair keyPair = keyPairGenerator.generateKeyPair();

                    // Prepare certificate details
                    X500Name subject = new X500Name("CN=" + commonName + ",O=Traccar,C=US");
                    X500Name issuer = new X500Name(intermediateCaCertificate.getSubjectX500Principal().getName());
                    BigInteger serialNumber = new BigInteger(64, new SecureRandom());
                    Date startDate = new Date();
                    Date endDate = new Date(startDate.getTime() + TimeUnit.DAYS.toMillis(CERTIFICATE_VALIDITY_DAYS));

                    // Create certificate builder
                    X509v3CertificateBuilder certBuilder = new JcaX509v3CertificateBuilder(
                            issuer,
                            serialNumber,
                            startDate,
                            endDate,
                            subject,
                            keyPair.getPublic());

                    // Add extensions
                    JcaX509ExtensionUtils extensionUtils = new JcaX509ExtensionUtils();
                    certBuilder.addExtension(
                            Extension.basicConstraints,
                            true,
                            new BasicConstraints(false)); // Not a CA
                    certBuilder.addExtension(
                            Extension.keyUsage,
                            true,
                            new KeyUsage(KeyUsage.digitalSignature | KeyUsage.keyEncipherment));
                    certBuilder.addExtension(
                            Extension.subjectKeyIdentifier,
                            false,
                            extensionUtils.createSubjectKeyIdentifier(keyPair.getPublic()));
                    certBuilder.addExtension(
                            Extension.authorityKeyIdentifier,
                            false,
                            extensionUtils.createAuthorityKeyIdentifier(intermediateCaCertificate.getPublicKey()));

                    // Add Subject Alternative Names if provided
                    if (alternativeNames != null && !alternativeNames.isEmpty()) {
                        GeneralName[] names = new GeneralName[alternativeNames.size()];
                        for (int i = 0; i < alternativeNames.size(); i++) {
                            names[i] = new GeneralName(GeneralName.dNSName, alternativeNames.get(i));
                        }
                        certBuilder.addExtension(
                                Extension.subjectAlternativeName,
                                false,
                                new GeneralNames(names));
                    }

                    // Sign the certificate
                    ContentSigner contentSigner = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM)
                            .build(intermediateCaPrivateKey);
                    X509CertificateHolder certHolder = certBuilder.build(contentSigner);
                    X509Certificate cert = new JcaX509CertificateConverter()
                            .getCertificate(certHolder);

                    // Store certificate in database
                    CertificateModel certModel = new CertificateModel();
                    certModel.setAlias(serviceId);
                    certModel.setSubject("CN=" + commonName + ",O=Traccar,C=US");
                    certModel.setIssuer(intermediateCaCertificate.getSubjectX500Principal().getName());
                    certModel.setSerialNumber(serialNumber.toString());
                    certModel.setNotBefore(startDate);
                    certModel.setNotAfter(endDate);
                    certModel.setCertificate(cert.getEncoded());
                    certModel.setPrivateKey(keyPair.getPrivate().getEncoded());
                    certModel.setCreatedAt(new Date());
                    certModel.setRevoked(false);

                    if (existingCert != null) {
                        // Update existing record
                        certModel.setId(existingCert.getId());
                        executeWithCircuitBreaker(() -> {
                            try {
                                storage.updateObject(certModel, new Request(new Columns.All()));
                                return null;
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } else {
                        // Add new record
                        executeWithCircuitBreaker(() -> {
                            try {
                                storage.addObject(certModel, new Request(new Columns.Exclude("id")));
                                return null;
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }

                    // Create certificate chain
                    Certificate[] chain = new Certificate[] {
                            cert,
                            intermediateCaCertificate,
                            rootCaCertificate
                    };

                    // Create certificate info and cache it
                    CertificateInfo certInfo = new CertificateInfo(serviceId, cert, keyPair.getPrivate(), chain);
                    certificateCache.put(serviceId, certInfo);

                    return certInfo;
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to generate certificate", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Renews an existing certificate for a service.
     *
     * @param serviceId The service identifier
     * @param commonName The common name for the certificate (typically service DNS name)
     * @param alternativeNames List of Subject Alternative Names (SANs) for the certificate
     * @return The renewed certificate information
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     * @throws OperatorCreationException If an error occurs creating the certificate signer
     */
    public CertificateInfo renewCertificate(String serviceId, String commonName, List<String> alternativeNames) 
            throws GeneralSecurityException, StorageException, IOException, OperatorCreationException {
        Span span = tracer.spanBuilder("renewCertificate").startSpan();
        span.setAttribute("serviceId", serviceId);
        try (Scope scope = span.makeCurrent()) {
            renewCounter.increment();
            return renewTimer.record(() -> {
                try {
                    LOGGER.info("Renewing certificate for service {}", serviceId);
                    // Revoke the old certificate first
                    CertificateModel existingCert = executeWithCircuitBreaker(() -> {
                        try {
                            return storage.getObject(CertificateModel.class,
                                    new Request(new Condition.Equals("alias", serviceId)));
                        } catch (StorageException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    if (existingCert != null && !existingCert.isRevoked()) {
                        // Mark as revoked but don't add to CRL yet
                        existingCert.setRevoked(true);
                        executeWithCircuitBreaker(() -> {
                            try {
                                storage.updateObject(existingCert, new Request(new Columns.All()));
                                return null;
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    }

                    // Generate new certificate
                    return generateCertificate(serviceId, commonName, alternativeNames);
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to renew certificate", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Retrieves a certificate for a service.
     *
     * @param serviceId The service identifier
     * @return The certificate information or null if not found
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     */
    public CertificateInfo getCertificate(String serviceId) 
            throws GeneralSecurityException, StorageException, IOException {
        Span span = tracer.spanBuilder("getCertificate").startSpan();
        span.setAttribute("serviceId", serviceId);
        try (Scope scope = span.makeCurrent()) {
            return retrieveTimer.record(() -> {
                try {
                    // Check cache first
                    if (certificateCache.containsKey(serviceId)) {
                        return certificateCache.get(serviceId);
                    }

                    // Try to get from storage
                    CertificateModel certModel = executeWithCircuitBreaker(() -> {
                        try {
                            return storage.getObject(CertificateModel.class,
                                    new Request(new Condition.Equals("alias", serviceId)));
                        } catch (StorageException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    if (certModel != null && !certModel.isRevoked()) {
                        CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
                        X509Certificate cert = (X509Certificate) certFactory.generateCertificate(
                                new ByteArrayInputStream(certModel.getCertificate()));
                        PrivateKey privateKey = KeyFactory.getInstance(CERTIFICATE_ALGORITHM)
                                .generatePrivate(new PKCS8EncodedKeySpec(certModel.getPrivateKey()));

                        // Create certificate chain
                        Certificate[] chain = new Certificate[] {
                                cert,
                                intermediateCaCertificate,
                                rootCaCertificate
                        };

                        CertificateInfo certInfo = new CertificateInfo(serviceId, cert, privateKey, chain);
                        certificateCache.put(serviceId, certInfo);
                        return certInfo;
                    }

                    return null;
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to retrieve certificate", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Validates a certificate.
     *
     * @param certificate The certificate to validate
     * @return True if the certificate is valid, false otherwise
     */
    public boolean validateCertificate(X509Certificate certificate) {
        Span span = tracer.spanBuilder("validateCertificate").startSpan();
        try (Scope scope = span.makeCurrent()) {
            validateCounter.increment();
            return validateTimer.record(() -> {
                try {
                    // Check validity period
                    certificate.checkValidity();

                    // Check if certificate is revoked
                    if (certificateRevocationList != null) {
                        if (certificateRevocationList.isRevoked(certificate)) {
                            LOGGER.warn("Certificate with serial number {} is revoked",
                                    certificate.getSerialNumber());
                            return false;
                        }
                    }

                    // Verify certificate chain
                    try {
                        certificate.verify(intermediateCaCertificate.getPublicKey());
                        intermediateCaCertificate.verify(rootCaCertificate.getPublicKey());
                        return true;
                    } catch (GeneralSecurityException e) {
                        LOGGER.warn("Certificate chain validation failed", e);
                        return false;
                    }
                } catch (CertificateExpiredException | CertificateNotYetValidException e) {
                    LOGGER.warn("Certificate is not valid: {}", e.getMessage());
                    return false;
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    LOGGER.error("Certificate validation failed", e);
                    return false;
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Revokes a certificate.
     *
     * @param serviceId The service identifier of the certificate to revoke
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     * @throws OperatorCreationException If an error occurs creating the CRL signer
     */
    public void revokeCertificate(String serviceId) 
            throws GeneralSecurityException, StorageException, IOException, OperatorCreationException {
        Span span = tracer.spanBuilder("revokeCertificate").startSpan();
        span.setAttribute("serviceId", serviceId);
        try (Scope scope = span.makeCurrent()) {
            revokeCounter.increment();
            revokeTimer.record(() -> {
                try {
                    // Get certificate from storage
                    CertificateModel certModel = executeWithCircuitBreaker(() -> {
                        try {
                            return storage.getObject(CertificateModel.class,
                                    new Request(new Condition.Equals("alias", serviceId)));
                        } catch (StorageException e) {
                            throw new RuntimeException(e);
                        }
                    });

                    if (certModel != null && !certModel.isRevoked()) {
                        // Mark as revoked
                        certModel.setRevoked(true);
                        executeWithCircuitBreaker(() -> {
                            try {
                                storage.updateObject(certModel, new Request(new Columns.All()));
                                return null;
                            } catch (StorageException e) {
                                throw new RuntimeException(e);
                            }
                        });

                        // Add to CRL
                        BigInteger serialNumber = new BigInteger(certModel.getSerialNumber());
                        List<BigInteger> revokedSerialNumbers = new ArrayList<>();
                        revokedSerialNumbers.add(serialNumber);
                        updateCrl(revokedSerialNumbers);

                        // Remove from cache
                        certificateCache.remove(serviceId);

                        LOGGER.info("Certificate for service {} has been revoked", serviceId);
                    } else {
                        LOGGER.warn("Certificate for service {} not found or already revoked", serviceId);
                    }
                    return null;
                } catch (Exception e) {
                    failureCounter.increment();
                    span.setStatus(StatusCode.ERROR, e.getMessage());
                    span.recordException(e);
                    throw new RuntimeException("Failed to revoke certificate", e);
                }
            });
        } finally {
            span.end();
        }
    }

    /**
     * Exports a certificate and private key as a PKCS12 keystore.
     *
     * @param serviceId The service identifier
     * @return The PKCS12 keystore as a byte array
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     */
    public byte[] exportCertificateAsPkcs12(String serviceId) 
            throws GeneralSecurityException, StorageException, IOException {
        Span span = tracer.spanBuilder("exportCertificateAsPkcs12").startSpan();
        span.setAttribute("serviceId", serviceId);
        try (Scope scope = span.makeCurrent()) {
            try {
                CertificateInfo certInfo = getCertificate(serviceId);
                if (certInfo == null) {
                    throw new GeneralSecurityException("Certificate not found for service: " + serviceId);
                }

                KeyStore keyStore = KeyStore.getInstance(KEYSTORE_TYPE);
                keyStore.load(null, null);

                // Add certificate chain
                keyStore.setKeyEntry(
                        serviceId,
                        certInfo.getPrivateKey(),
                        KEYSTORE_PASSWORD.toCharArray(),
                        certInfo.getCertificateChain());

                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                keyStore.store(outputStream, KEYSTORE_PASSWORD.toCharArray());

                return outputStream.toByteArray();
            } catch (Exception e) {
                failureCounter.increment();
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                throw new RuntimeException("Failed to export certificate as PKCS12", e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Exports a certificate as PEM format.
     *
     * @param serviceId The service identifier
     * @return The certificate in PEM format
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     */
    public String exportCertificateAsPem(String serviceId) 
            throws GeneralSecurityException, StorageException, IOException {
        Span span = tracer.spanBuilder("exportCertificateAsPem").startSpan();
        span.setAttribute("serviceId", serviceId);
        try (Scope scope = span.makeCurrent()) {
            try {
                CertificateInfo certInfo = getCertificate(serviceId);
                if (certInfo == null) {
                    throw new GeneralSecurityException("Certificate not found for service: " + serviceId);
                }

                StringBuilder sb = new StringBuilder();
                sb.append("-----BEGIN CERTIFICATE-----\n");
                sb.append(Base64.getMimeEncoder().encodeToString(certInfo.getCertificate().getEncoded()));
                sb.append("\n-----END CERTIFICATE-----\n");

                return sb.toString();
            } catch (Exception e) {
                failureCounter.increment();
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                throw new RuntimeException("Failed to export certificate as PEM", e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Exports a private key as PEM format.
     *
     * @param serviceId The service identifier
     * @return The private key in PEM format
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     */
    public String exportPrivateKeyAsPem(String serviceId) 
            throws GeneralSecurityException, StorageException, IOException {
        Span span = tracer.spanBuilder("exportPrivateKeyAsPem").startSpan();
        span.setAttribute("serviceId", serviceId);
        try (Scope scope = span.makeCurrent()) {
            try {
                CertificateInfo certInfo = getCertificate(serviceId);
                if (certInfo == null) {
                    throw new GeneralSecurityException("Certificate not found for service: " + serviceId);
                }

                StringBuilder sb = new StringBuilder();
                sb.append("-----BEGIN PRIVATE KEY-----\n");
                sb.append(Base64.getMimeEncoder().encodeToString(certInfo.getPrivateKey().getEncoded()));
                sb.append("\n-----END PRIVATE KEY-----\n");

                return sb.toString();
            } catch (Exception e) {
                failureCounter.increment();
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                throw new RuntimeException("Failed to export private key as PEM", e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Gets the Certificate Revocation List (CRL) in PEM format.
     *
     * @return The CRL in PEM format
     * @throws CRLException If a CRL error occurs
     */
    public String getCrlAsPem() throws CRLException {
        Span span = tracer.spanBuilder("getCrlAsPem").startSpan();
        try (Scope scope = span.makeCurrent()) {
            try {
                if (certificateRevocationList == null) {
                    throw new CRLException("CRL not initialized");
                }

                StringBuilder sb = new StringBuilder();
                sb.append("-----BEGIN X509 CRL-----\n");
                sb.append(Base64.getMimeEncoder().encodeToString(certificateRevocationList.getEncoded()));
                sb.append("\n-----END X509 CRL-----\n");

                return sb.toString();
            } catch (Exception e) {
                failureCounter.increment();
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                throw new RuntimeException("Failed to get CRL as PEM", e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Gets the root CA certificate in PEM format.
     *
     * @return The root CA certificate in PEM format
     * @throws CertificateEncodingException If a certificate encoding error occurs
     */
    public String getRootCaCertificateAsPem() throws CertificateEncodingException {
        Span span = tracer.spanBuilder("getRootCaCertificateAsPem").startSpan();
        try (Scope scope = span.makeCurrent()) {
            try {
                if (rootCaCertificate == null) {
                    throw new CertificateEncodingException("Root CA certificate not initialized");
                }

                StringBuilder sb = new StringBuilder();
                sb.append("-----BEGIN CERTIFICATE-----\n");
                sb.append(Base64.getMimeEncoder().encodeToString(rootCaCertificate.getEncoded()));
                sb.append("\n-----END CERTIFICATE-----\n");

                return sb.toString();
            } catch (Exception e) {
                failureCounter.increment();
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                throw new RuntimeException("Failed to get root CA certificate as PEM", e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Gets the intermediate CA certificate in PEM format.
     *
     * @return The intermediate CA certificate in PEM format
     * @throws CertificateEncodingException If a certificate encoding error occurs
     */
    public String getIntermediateCaCertificateAsPem() throws CertificateEncodingException {
        Span span = tracer.spanBuilder("getIntermediateCaCertificateAsPem").startSpan();
        try (Scope scope = span.makeCurrent()) {
            try {
                if (intermediateCaCertificate == null) {
                    throw new CertificateEncodingException("Intermediate CA certificate not initialized");
                }

                StringBuilder sb = new StringBuilder();
                sb.append("-----BEGIN CERTIFICATE-----\n");
                sb.append(Base64.getMimeEncoder().encodeToString(intermediateCaCertificate.getEncoded()));
                sb.append("\n-----END CERTIFICATE-----\n");

                return sb.toString();
            } catch (Exception e) {
                failureCounter.increment();
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                throw new RuntimeException("Failed to get intermediate CA certificate as PEM", e);
            }
        } finally {
            span.end();
        }
    }

    /**
     * Checks for certificates that need renewal and renews them.
     *
     * @throws GeneralSecurityException If a security error occurs
     * @throws StorageException If a storage error occurs
     * @throws IOException If an I/O error occurs
     * @throws OperatorCreationException If an error occurs creating the certificate signer
     */
    public void checkAndRenewCertificates() 
            throws GeneralSecurityException, StorageException, IOException, OperatorCreationException {
        Span span = tracer.spanBuilder("checkAndRenewCertificates").startSpan();
        try (Scope scope = span.makeCurrent()) {
            try {
                List<CertificateModel> certificates = executeWithCircuitBreaker(() -> {
                    try {
                        return storage.getObjects(CertificateModel.class, 
                                new Request(new Condition.Equals("revoked", false)));
                    } catch (StorageException e) {
                        throw new RuntimeException(e);
                    }
                });

                Date renewalDate = new Date(System.currentTimeMillis() 
                        + TimeUnit.DAYS.toMillis(CERTIFICATE_RENEWAL_THRESHOLD_DAYS));

                for (CertificateModel certModel : certificates) {
                    if (certModel.getNotAfter().before(renewalDate)) {
                        // Certificate is approaching expiration, renew it
                        String serviceId = certModel.getAlias();
                        if (!ROOT_CA_ALIAS.equals(serviceId) && !INTERMEDIATE_CA_ALIAS.equals(serviceId)) {
                            try {
                                // Extract common name from subject
                                String subject = certModel.getSubject();
                                String commonName = subject.substring(subject.indexOf("CN=") + 3, 
                                        subject.indexOf(",", subject.indexOf("CN=")));
                                
                                // Renew certificate
                                renewCertificate(serviceId, commonName, null);
                                LOGGER.info("Renewed certificate for service {}", serviceId);
                            } catch (Exception e) {
                                LOGGER.error("Failed to renew certificate for service {}", serviceId, e);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                failureCounter.increment();
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.recordException(e);
                throw new RuntimeException("Failed to check and renew certificates", e);
            }
        } finally {
            span.end();
        }
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
     * Class to hold certificate information.
     */
    public static class CertificateInfo {
        private final String serviceId;
        private final X509Certificate certificate;
        private final PrivateKey privateKey;
        private final Certificate[] certificateChain;

        public CertificateInfo(String serviceId, X509Certificate certificate, PrivateKey privateKey) {
            this(serviceId, certificate, privateKey, new Certificate[] { certificate });
        }

        public CertificateInfo(String serviceId, X509Certificate certificate, PrivateKey privateKey, 
                              Certificate[] certificateChain) {
            this.serviceId = serviceId;
            this.certificate = certificate;
            this.privateKey = privateKey;
            this.certificateChain = certificateChain;
        }

        public String getServiceId() {
            return serviceId;
        }

        public X509Certificate getCertificate() {
            return certificate;
        }

        public PrivateKey getPrivateKey() {
            return privateKey;
        }

        public Certificate[] getCertificateChain() {
            return certificateChain;
        }
    }
}