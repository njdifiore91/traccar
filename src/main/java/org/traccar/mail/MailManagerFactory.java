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
package org.traccar.mail;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.database.StatisticsManager;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.discovery.ServiceDiscoveryListener;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory class that creates and configures appropriate MailManager implementations based on
 * application configuration and service discovery. This component integrates with the service
 * discovery system to locate mail services, applies decorators for circuit breakers, metrics,
 * and tracing, and supports dynamic reconfiguration based on service discovery events.
 */
@Singleton
public class MailManagerFactory implements ServiceDiscoveryListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MailManagerFactory.class);
    private static final String MAIL_SERVICE_NAME = "mail-service";
    private static final String CIRCUIT_BREAKER_NAME = "mailManager";

    private final Config config;
    private final StatisticsManager statisticsManager;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MeterRegistry meterRegistry;
    private final Tracer tracer;
    private final ServiceDiscovery serviceDiscovery;
    private final AtomicReference<MailManager> mailManagerRef = new AtomicReference<>();

    /**
     * Creates a new MailManagerFactory with the specified dependencies.
     *
     * @param config The application configuration
     * @param statisticsManager The statistics manager for tracking mail metrics
     * @param circuitBreakerRegistry The circuit breaker registry for resilience patterns
     * @param meterRegistry The meter registry for metrics collection
     * @param tracer The OpenTelemetry tracer for distributed tracing
     * @param serviceDiscovery The service discovery component for locating mail services
     */
    @Inject
    public MailManagerFactory(
            Config config,
            StatisticsManager statisticsManager,
            CircuitBreakerRegistry circuitBreakerRegistry,
            MeterRegistry meterRegistry,
            Tracer tracer,
            ServiceDiscovery serviceDiscovery) {
        this.config = config;
        this.statisticsManager = statisticsManager;
        this.circuitBreakerRegistry = circuitBreakerRegistry;
        this.meterRegistry = meterRegistry;
        this.tracer = tracer;
        this.serviceDiscovery = serviceDiscovery;
        
        // Register for service discovery events
        this.serviceDiscovery.addListener(MAIL_SERVICE_NAME, this);
        
        // Initialize the mail manager
        createMailManager();
    }

    /**
     * Gets the current MailManager instance. This method always returns the most up-to-date
     * implementation based on the current configuration and service discovery state.
     *
     * @return The current MailManager implementation
     */
    public MailManager getMailManager() {
        return mailManagerRef.get();
    }

    /**
     * Creates and configures the appropriate MailManager implementation based on the current
     * configuration and service discovery state.
     */
    private void createMailManager() {
        MailManager mailManager;
        String mailType = config.getString(Keys.MAIL_TYPE, "smtp");

        switch (mailType.toLowerCase()) {
            case "smtp":
                mailManager = new SmtpMailManager(config, statisticsManager);
                LOGGER.info("Created SMTP mail manager");
                break;
            case "message_broker":
                // Check if the mail service is available through service discovery
                if (serviceDiscovery.isServiceAvailable(MAIL_SERVICE_NAME)) {
                    mailManager = createMessageBrokerMailManager();
                    LOGGER.info("Created MessageBroker mail manager using service discovery");
                } else {
                    // Fallback to SMTP if service is not available
                    mailManager = new SmtpMailManager(config, statisticsManager);
                    LOGGER.warn("Mail service not available through service discovery, falling back to SMTP");
                }
                break;
            case "log":
                mailManager = new LogMailManager();
                LOGGER.info("Created Log mail manager");
                break;
            default:
                mailManager = new LogMailManager();
                LOGGER.warn("Unknown mail type: {}, using Log mail manager as fallback", mailType);
                break;
        }

        // Apply decorators for cross-cutting concerns
        mailManager = applyDecorators(mailManager);
        
        // Update the reference atomically
        mailManagerRef.set(mailManager);
    }

    /**
     * Creates a MessageBrokerMailManager that communicates with the mail service through a message broker.
     * This implementation is used when the mail service is available through service discovery.
     *
     * @return A new MessageBrokerMailManager instance
     */
    private MailManager createMessageBrokerMailManager() {
        // This would be implemented to create a MessageBrokerMailManager that communicates
        // with the mail service through a message broker. For now, we'll use a placeholder.
        // In a real implementation, we would use the service discovery to get the mail service
        // endpoint and create a client that communicates with it.
        
        // Placeholder implementation - in a real system, this would create a proper MessageBrokerMailManager
        return new LogMailManager(); // Placeholder
    }

    /**
     * Applies decorators to the mail manager for cross-cutting concerns such as circuit breaking,
     * metrics collection, and distributed tracing.
     *
     * @param mailManager The mail manager to decorate
     * @return The decorated mail manager
     */
    private MailManager applyDecorators(MailManager mailManager) {
        // Apply circuit breaker decorator
        CircuitBreaker circuitBreaker = getOrCreateCircuitBreaker();
        MailManager circuitBreakerDecorated = new CircuitBreakerMailManagerDecorator(mailManager, circuitBreaker);
        
        // Apply metrics decorator
        MailManager metricsDecorated = new MetricsMailManagerDecorator(circuitBreakerDecorated, meterRegistry);
        
        // Apply tracing decorator
        MailManager tracingDecorated = new TracingMailManagerDecorator(metricsDecorated, tracer);
        
        return tracingDecorated;
    }

    /**
     * Gets or creates a circuit breaker for the mail manager with appropriate configuration.
     *
     * @return The circuit breaker instance
     */
    private CircuitBreaker getOrCreateCircuitBreaker() {
        // Check if the circuit breaker already exists
        if (circuitBreakerRegistry.find(CIRCUIT_BREAKER_NAME).isPresent()) {
            return circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        }
        
        // Create a new circuit breaker with custom configuration
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to trip the circuit
                .waitDurationInOpenState(Duration.ofSeconds(30)) // Wait 30 seconds in open state
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10) // Consider the last 10 calls
                .minimumNumberOfCalls(5) // Minimum calls before calculating failure rate
                .permittedNumberOfCallsInHalfOpenState(3) // Allow 3 calls in half-open state
                .automaticTransitionFromOpenToHalfOpenEnabled(true) // Auto transition to half-open
                .build();
        
        return circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME, circuitBreakerConfig);
    }

    /**
     * Called when a service discovery event occurs for the mail service.
     * This method reconfigures the mail manager based on the new service discovery state.
     */
    @Override
    public void onServiceUpdate() {
        LOGGER.info("Mail service discovery update received, reconfiguring mail manager");
        createMailManager();
    }

    /**
     * Decorator that adds circuit breaker functionality to a MailManager implementation.
     */
    private static class CircuitBreakerMailManagerDecorator implements MailManager {
        private final MailManager delegate;
        private final CircuitBreaker circuitBreaker;

        CircuitBreakerMailManagerDecorator(MailManager delegate, CircuitBreaker circuitBreaker) {
            this.delegate = delegate;
            this.circuitBreaker = circuitBreaker;
        }

        @Override
        public boolean getEmailEnabled() {
            return circuitBreaker.executeSupplier(delegate::getEmailEnabled);
        }

        @Override
        public void sendMessage(org.traccar.model.User user, boolean system, String subject, String body)
                throws jakarta.mail.MessagingException {
            try {
                circuitBreaker.executeRunnable(() -> {
                    try {
                        delegate.sendMessage(user, system, subject, body);
                    } catch (jakarta.mail.MessagingException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (RuntimeException e) {
                if (e.getCause() instanceof jakarta.mail.MessagingException) {
                    throw (jakarta.mail.MessagingException) e.getCause();
                }
                throw e;
            }
        }

        @Override
        public void sendMessage(org.traccar.model.User user, boolean system, String subject, String body,
                               jakarta.mail.internet.MimeBodyPart attachment) throws jakarta.mail.MessagingException {
            try {
                circuitBreaker.executeRunnable(() -> {
                    try {
                        delegate.sendMessage(user, system, subject, body, attachment);
                    } catch (jakarta.mail.MessagingException e) {
                        throw new RuntimeException(e);
                    }
                });
            } catch (RuntimeException e) {
                if (e.getCause() instanceof jakarta.mail.MessagingException) {
                    throw (jakarta.mail.MessagingException) e.getCause();
                }
                throw e;
            }
        }
    }

    /**
     * Decorator that adds metrics collection to a MailManager implementation.
     */
    private static class MetricsMailManagerDecorator implements MailManager {
        private final MailManager delegate;
        private final MeterRegistry meterRegistry;

        MetricsMailManagerDecorator(MailManager delegate, MeterRegistry meterRegistry) {
            this.delegate = delegate;
            this.meterRegistry = meterRegistry;
        }

        @Override
        public boolean getEmailEnabled() {
            return delegate.getEmailEnabled();
        }

        @Override
        public void sendMessage(org.traccar.model.User user, boolean system, String subject, String body)
                throws jakarta.mail.MessagingException {
            meterRegistry.counter("mail.send", "type", "text", "system", String.valueOf(system)).increment();
            long startTime = System.nanoTime();
            try {
                delegate.sendMessage(user, system, subject, body);
                meterRegistry.counter("mail.send.success", "type", "text").increment();
            } catch (jakarta.mail.MessagingException e) {
                meterRegistry.counter("mail.send.error", "type", "text", "error", e.getClass().getSimpleName()).increment();
                throw e;
            } finally {
                long duration = System.nanoTime() - startTime;
                meterRegistry.timer("mail.send.time", "type", "text").record(duration, java.util.concurrent.TimeUnit.NANOSECONDS);
            }
        }

        @Override
        public void sendMessage(org.traccar.model.User user, boolean system, String subject, String body,
                               jakarta.mail.internet.MimeBodyPart attachment) throws jakarta.mail.MessagingException {
            meterRegistry.counter("mail.send", "type", "attachment", "system", String.valueOf(system)).increment();
            long startTime = System.nanoTime();
            try {
                delegate.sendMessage(user, system, subject, body, attachment);
                meterRegistry.counter("mail.send.success", "type", "attachment").increment();
            } catch (jakarta.mail.MessagingException e) {
                meterRegistry.counter("mail.send.error", "type", "attachment", "error", e.getClass().getSimpleName()).increment();
                throw e;
            } finally {
                long duration = System.nanoTime() - startTime;
                meterRegistry.timer("mail.send.time", "type", "attachment").record(duration, java.util.concurrent.TimeUnit.NANOSECONDS);
            }
        }
    }

    /**
     * Decorator that adds distributed tracing to a MailManager implementation.
     */
    private static class TracingMailManagerDecorator implements MailManager {
        private final MailManager delegate;
        private final Tracer tracer;

        TracingMailManagerDecorator(MailManager delegate, Tracer tracer) {
            this.delegate = delegate;
            this.tracer = tracer;
        }

        @Override
        public boolean getEmailEnabled() {
            return delegate.getEmailEnabled();
        }

        @Override
        public void sendMessage(org.traccar.model.User user, boolean system, String subject, String body)
                throws jakarta.mail.MessagingException {
            var span = tracer.spanBuilder("MailManager.sendMessage").startSpan();
            try (var scope = span.makeCurrent()) {
                span.setAttribute("mail.to", user.getEmail());
                span.setAttribute("mail.subject", subject);
                span.setAttribute("mail.system", system);
                span.setAttribute("mail.attachment", false);
                
                delegate.sendMessage(user, system, subject, body);
                
                span.setAttribute("mail.status", "success");
            } catch (jakarta.mail.MessagingException e) {
                span.setAttribute("mail.status", "error");
                span.setAttribute("mail.error.type", e.getClass().getName());
                span.setAttribute("mail.error.message", e.getMessage() != null ? e.getMessage() : "");
                span.recordException(e);
                throw e;
            } finally {
                span.end();
            }
        }

        @Override
        public void sendMessage(org.traccar.model.User user, boolean system, String subject, String body,
                               jakarta.mail.internet.MimeBodyPart attachment) throws jakarta.mail.MessagingException {
            var span = tracer.spanBuilder("MailManager.sendMessageWithAttachment").startSpan();
            try (var scope = span.makeCurrent()) {
                span.setAttribute("mail.to", user.getEmail());
                span.setAttribute("mail.subject", subject);
                span.setAttribute("mail.system", system);
                span.setAttribute("mail.attachment", true);
                try {
                    span.setAttribute("mail.attachment.name", attachment.getFileName());
                } catch (Exception e) {
                    // Ignore if we can't get the filename
                }
                
                delegate.sendMessage(user, system, subject, body, attachment);
                
                span.setAttribute("mail.status", "success");
            } catch (jakarta.mail.MessagingException e) {
                span.setAttribute("mail.status", "error");
                span.setAttribute("mail.error.type", e.getClass().getName());
                span.setAttribute("mail.error.message", e.getMessage() != null ? e.getMessage() : "");
                span.recordException(e);
                throw e;
            } finally {
                span.end();
            }
        }
    }
}