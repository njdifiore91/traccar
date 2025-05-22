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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects and exposes metrics for cryptographic operations in the signature subsystem.
 * Integrates with OpenTelemetry for distributed tracing and Micrometer for metrics collection.
 */
@Singleton
public class MetricsCollector {

    private final MeterRegistry registry;
    private final Tracer tracer;

    // Counters for key usage
    private final Counter keyGenerationCounter;
    private final Counter keyRetrievalCounter;
    
    // Counters for token operations
    private final Counter tokenGenerationCounter;
    private final Counter tokenVerificationCounter;
    private final Counter tokenVerificationFailedCounter;
    
    // Timers for performance metrics
    private final Timer signOperationTimer;
    private final Timer verifyOperationTimer;
    
    // Gauges for monitoring key age
    private final AtomicLong keyAgeInDays;

    @Inject
    public MetricsCollector(MeterRegistry registry, Tracer tracer) {
        this.registry = registry;
        this.tracer = tracer;
        
        // Initialize counters for key operations
        this.keyGenerationCounter = Counter.builder("traccar.signature.key.generation")
                .description("Number of cryptographic key pair generations")
                .register(registry);
        
        this.keyRetrievalCounter = Counter.builder("traccar.signature.key.retrieval")
                .description("Number of cryptographic key retrievals from storage")
                .register(registry);
        
        // Initialize counters for token operations
        this.tokenGenerationCounter = Counter.builder("traccar.signature.token.generation")
                .description("Number of signed tokens generated")
                .register(registry);
        
        this.tokenVerificationCounter = Counter.builder("traccar.signature.token.verification")
                .description("Number of token verification attempts")
                .register(registry);
        
        this.tokenVerificationFailedCounter = Counter.builder("traccar.signature.token.verification.failed")
                .description("Number of failed token verifications")
                .register(registry);
        
        // Initialize timers for performance metrics
        this.signOperationTimer = Timer.builder("traccar.signature.operation.sign")
                .description("Time taken to sign data")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        
        this.verifyOperationTimer = Timer.builder("traccar.signature.operation.verify")
                .description("Time taken to verify signatures")
                .publishPercentiles(0.5, 0.95, 0.99)
                .register(registry);
        
        // Initialize gauge for key age monitoring
        this.keyAgeInDays = registry.gauge("traccar.signature.key.age.days", new AtomicLong(0));
    }

    /**
     * Records a key generation event.
     */
    public void recordKeyGeneration() {
        Span span = tracer.spanBuilder("signature.key.generation").startSpan();
        try (Scope scope = span.makeCurrent()) {
            keyGenerationCounter.increment();
            keyAgeInDays.set(0); // Reset key age when new keys are generated
            span.setAttribute("signature.operation", "key_generation");
        } finally {
            span.end();
        }
    }

    /**
     * Records a key retrieval event.
     */
    public void recordKeyRetrieval() {
        keyRetrievalCounter.increment();
    }

    /**
     * Records a token generation event and measures its duration.
     * 
     * @param startTime The start time of the operation in nanoseconds
     */
    public void recordTokenGeneration(long startTime) {
        Span span = tracer.spanBuilder("signature.token.generation").startSpan();
        try (Scope scope = span.makeCurrent()) {
            tokenGenerationCounter.increment();
            long duration = System.nanoTime() - startTime;
            signOperationTimer.record(duration, TimeUnit.NANOSECONDS);
            span.setAttribute("signature.operation", "token_generation");
            span.setAttribute("signature.duration_ms", duration / 1_000_000.0);
        } finally {
            span.end();
        }
    }

    /**
     * Records a token verification event and measures its duration.
     * 
     * @param startTime The start time of the operation in nanoseconds
     * @param success Whether the verification was successful
     */
    public void recordTokenVerification(long startTime, boolean success) {
        Span span = tracer.spanBuilder("signature.token.verification").startSpan();
        try (Scope scope = span.makeCurrent()) {
            tokenVerificationCounter.increment();
            if (!success) {
                tokenVerificationFailedCounter.increment();
                span.setAttribute("signature.verification_result", "failed");
            } else {
                span.setAttribute("signature.verification_result", "success");
            }
            
            long duration = System.nanoTime() - startTime;
            verifyOperationTimer.record(duration, TimeUnit.NANOSECONDS);
            span.setAttribute("signature.operation", "token_verification");
            span.setAttribute("signature.duration_ms", duration / 1_000_000.0);
        } finally {
            span.end();
        }
    }

    /**
     * Updates the age of the current key pair in days.
     * 
     * @param ageInDays The age of the key pair in days
     */
    public void updateKeyAge(long ageInDays) {
        keyAgeInDays.set(ageInDays);
    }

    /**
     * Records a sign operation and measures its duration.
     * 
     * @param startTime The start time of the operation in nanoseconds
     */
    public void recordSignOperation(long startTime) {
        long duration = System.nanoTime() - startTime;
        signOperationTimer.record(duration, TimeUnit.NANOSECONDS);
    }

    /**
     * Records a verify operation and measures its duration.
     * 
     * @param startTime The start time of the operation in nanoseconds
     * @param success Whether the verification was successful
     */
    public void recordVerifyOperation(long startTime, boolean success) {
        long duration = System.nanoTime() - startTime;
        verifyOperationTimer.record(duration, TimeUnit.NANOSECONDS);
        
        if (!success) {
            tokenVerificationFailedCounter.increment();
        }
    }

    /**
     * Gets the current verification failure rate.
     * This can be used for alerting on abnormal patterns.
     * 
     * @return The ratio of failed verifications to total verifications
     */
    public double getVerificationFailureRate() {
        double totalVerifications = tokenVerificationCounter.count();
        if (totalVerifications > 0) {
            return tokenVerificationFailedCounter.count() / totalVerifications;
        }
        return 0.0;
    }
}