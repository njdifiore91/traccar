/*
 * Copyright 2017 - 2022 Anton Tananaev (anton@traccar.org)
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
package org.traccar.database;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.github.resilience4j.retry.RetryRegistry;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.InitialDirContext;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.User;

import java.time.Duration;
import java.util.Hashtable;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

public class LdapProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(LdapProvider.class);
    private static final String CIRCUIT_BREAKER_NAME = "ldapCircuitBreaker";
    private static final String RETRY_NAME = "ldapRetry";
    private static final String INSTRUMENTATION_NAME = "org.traccar.database.LdapProvider";
    
    // OpenTelemetry instrumentation
    private final Tracer tracer;
    private final Meter meter;
    private final LongCounter ldapOperationCounter;
    private final LongCounter ldapFailureCounter;
    
    // Circuit breaker and retry patterns
    private final CircuitBreaker circuitBreaker;
    private final Retry retry;
    
    // Connection pooling properties
    private final boolean poolingEnabled;
    private final int maxPoolSize;
    private final int preferredPoolSize;
    private final long poolTimeout;
    
    private final String url;
    private final String searchBase;
    private final String idAttribute;
    private final String nameAttribute;
    private final String mailAttribute;
    private final String searchFilter;
    private final String adminFilter;
    private final String serviceUser;
    private final String servicePassword;

    public LdapProvider(Config config) {
        url = config.getString(Keys.LDAP_URL);
        searchBase = config.getString(Keys.LDAP_BASE);
        idAttribute = config.getString(Keys.LDAP_ID_ATTRIBUTE);
        nameAttribute = config.getString(Keys.LDAP_NAME_ATTRIBUTE);
        mailAttribute = config.getString(Keys.LDAP_MAIN_ATTRIBUTE);
        if (config.hasKey(Keys.LDAP_SEARCH_FILTER)) {
            searchFilter = config.getString(Keys.LDAP_SEARCH_FILTER);
        } else {
            searchFilter = "(" + idAttribute + "=:login)";
        }
        if (config.hasKey(Keys.LDAP_ADMIN_FILTER)) {
            adminFilter = config.getString(Keys.LDAP_ADMIN_FILTER);
        } else {
            String adminGroup = config.getString(Keys.LDAP_ADMIN_GROUP);
            if (adminGroup != null) {
                adminFilter = "(&(" + idAttribute + "=:login)(memberOf=" + adminGroup + "))";
            } else {
                adminFilter = null;
            }
        }
        serviceUser = config.getString(Keys.LDAP_USER);
        servicePassword = config.getString(Keys.LDAP_PASSWORD);
        
        // Initialize connection pooling
        poolingEnabled = config.getBoolean(Keys.LDAP_POOL_ENABLED, true);
        maxPoolSize = config.getInteger(Keys.LDAP_POOL_MAX_SIZE, 10);
        preferredPoolSize = config.getInteger(Keys.LDAP_POOL_PREFERRED_SIZE, 5);
        poolTimeout = config.getLong(Keys.LDAP_POOL_TIMEOUT, 300000); // 5 minutes default
        
        // Initialize OpenTelemetry
        tracer = GlobalOpenTelemetry.getTracer(INSTRUMENTATION_NAME);
        meter = GlobalOpenTelemetry.getMeter(INSTRUMENTATION_NAME);
        ldapOperationCounter = meter.counterBuilder("ldap.operations")
                .setDescription("Number of LDAP operations")
                .build();
        ldapFailureCounter = meter.counterBuilder("ldap.failures")
                .setDescription("Number of failed LDAP operations")
                .build();
        
        // Initialize circuit breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50) // 50% failure rate to open circuit
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .permittedNumberOfCallsInHalfOpenState(3)
                .recordExceptions(NamingException.class, TimeoutException.class)
                .build();
        
        CircuitBreakerRegistry circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker(CIRCUIT_BREAKER_NAME);
        
        // Initialize retry with exponential backoff
        RetryConfig retryConfig = RetryConfig.custom()
                .maxAttempts(3)
                .waitDuration(Duration.ofMillis(500))
                .retryExceptions(NamingException.class)
                .exponentialBackoff(Duration.ofMillis(500), Duration.ofSeconds(5), 2.0)
                .build();
        
        RetryRegistry retryRegistry = RetryRegistry.of(retryConfig);
        retry = retryRegistry.retry(RETRY_NAME);
        
        // Register event listeners for monitoring
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker state changed from {} to {}",
                        event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
        
        retry.getEventPublisher()
                .onRetry(event -> LOGGER.debug("Retry attempt {} after {} ms",
                        event.getNumberOfRetryAttempts(),
                        event.getWaitInterval().toMillis()));
    }
    
    @PostConstruct
    public void init() {
        if (poolingEnabled) {
            LOGGER.info("Initializing LDAP connection pool with max size: {}, preferred size: {}, timeout: {} ms",
                    maxPoolSize, preferredPoolSize, poolTimeout);
            
            // Set system properties for LDAP connection pooling
            System.setProperty("com.sun.jndi.ldap.connect.pool", "true");
            System.setProperty("com.sun.jndi.ldap.connect.pool.maxsize", String.valueOf(maxPoolSize));
            System.setProperty("com.sun.jndi.ldap.connect.pool.prefsize", String.valueOf(preferredPoolSize));
            System.setProperty("com.sun.jndi.ldap.connect.pool.timeout", String.valueOf(poolTimeout));
            System.setProperty("com.sun.jndi.ldap.connect.pool.protocol", "plain ssl");
            System.setProperty("com.sun.jndi.ldap.connect.pool.authentication", "none simple");
            
            // Pre-initialize connections in the pool
            try {
                InitialDirContext context = initContext();
                context.close();
                LOGGER.info("LDAP connection pool initialized successfully");
            } catch (NamingException e) {
                LOGGER.warn("Failed to initialize LDAP connection pool", e);
            }
        }
    }
    
    @PreDestroy
    public void destroy() {
        LOGGER.info("Shutting down LDAP provider");
        // Any cleanup needed for graceful shutdown
    }

    private InitialDirContext auth(String accountName, String password) throws NamingException {
        Span span = tracer.spanBuilder("ldap.auth").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("ldap.url", url);
            span.setAttribute("ldap.account", accountName);
            
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
            env.put(Context.PROVIDER_URL, url);

            env.put(Context.SECURITY_AUTHENTICATION, "simple");
            env.put(Context.SECURITY_PRINCIPAL, accountName);
            env.put(Context.SECURITY_CREDENTIALS, password);
            
            // Enable connection pooling
            if (poolingEnabled) {
                env.put("com.sun.jndi.ldap.connect.pool", "true");
            }

            ldapOperationCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "auth"));
            
            try {
                return new InitialDirContext(env);
            } catch (NamingException e) {
                ldapFailureCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "auth"));
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                throw e;
            }
        } finally {
            span.end();
        }
    }

    private boolean isAdmin(String accountName) {
        if (this.adminFilter != null) {
            Span span = tracer.spanBuilder("ldap.isAdmin").startSpan();
            try (Scope scope = span.makeCurrent()) {
                span.setAttribute("ldap.account", accountName);
                
                return circuitBreaker.executeSupplier(() -> {
                    return retry.executeSupplier(() -> {
                        try {
                            InitialDirContext context = initContext();
                            String searchString = adminFilter.replace(":login", encodeForLdap(accountName));
                            SearchControls searchControls = new SearchControls();
                            searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
                            
                            ldapOperationCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "isAdmin"));
                            
                            NamingEnumeration<SearchResult> results = context.search(searchBase, searchString, searchControls);
                            if (results.hasMoreElements()) {
                                results.nextElement();
                                if (results.hasMoreElements()) {
                                    LOGGER.warn("Matched multiple users for the accountName: " + accountName);
                                    return false;
                                }
                                return true;
                            }
                            return false;
                        } catch (NamingException e) {
                            ldapFailureCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "isAdmin"));
                            span.recordException(e);
                            span.setStatus(StatusCode.ERROR, e.getMessage());
                            throw e;
                        }
                    });
                });
            } catch (Exception e) {
                LOGGER.warn("Error checking admin status for user: {}", accountName, e);
                return false;
            } finally {
                span.end();
            }
        }
        return false;
    }

    public InitialDirContext initContext() throws NamingException {
        Span span = tracer.spanBuilder("ldap.initContext").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("ldap.url", url);
            span.setAttribute("ldap.serviceUser", serviceUser);
            
            ldapOperationCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "initContext"));
            
            try {
                return auth(serviceUser, servicePassword);
            } catch (NamingException e) {
                ldapFailureCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "initContext"));
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
                throw e;
            }
        } finally {
            span.end();
        }
    }

    private SearchResult lookupUser(String accountName) throws NamingException {
        Span span = tracer.spanBuilder("ldap.lookupUser").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("ldap.account", accountName);
            
            return circuitBreaker.executeSupplier(() -> {
                return retry.executeSupplier(() -> {
                    try {
                        InitialDirContext context = initContext();
                        String searchString = searchFilter.replace(":login", encodeForLdap(accountName));

                        SearchControls searchControls = new SearchControls();
                        String[] attributeFilter = {idAttribute, nameAttribute, mailAttribute};
                        searchControls.setReturningAttributes(attributeFilter);
                        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);

                        ldapOperationCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "lookupUser"));
                        
                        NamingEnumeration<SearchResult> results = context.search(searchBase, searchString, searchControls);

                        SearchResult searchResult = null;
                        if (results.hasMoreElements()) {
                            searchResult = results.nextElement();
                            if (results.hasMoreElements()) {
                                LOGGER.warn("Matched multiple users for the accountName: " + accountName);
                                return null;
                            }
                        }

                        return searchResult;
                    } catch (NamingException e) {
                        ldapFailureCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "lookupUser"));
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        throw e;
                    }
                });
            });
        } catch (Exception e) {
            LOGGER.warn("Error looking up user: {}", accountName, e);
            throw new NamingException("Error looking up user: " + e.getMessage());
        } finally {
            span.end();
        }
    }

    public User getUser(String accountName) {
        Span span = tracer.spanBuilder("ldap.getUser").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("ldap.account", accountName);
            
            SearchResult ldapUser;
            User user = new User();
            try {
                ldapUser = lookupUser(accountName);
                if (ldapUser != null) {
                    Attribute attribute = ldapUser.getAttributes().get(idAttribute);
                    if (attribute != null) {
                        user.setLogin((String) attribute.get());
                    } else {
                        user.setLogin(accountName);
                    }
                    attribute = ldapUser.getAttributes().get(nameAttribute);
                    if (attribute != null) {
                        user.setName((String) attribute.get());
                    } else {
                        user.setName(accountName);
                    }
                    attribute = ldapUser.getAttributes().get(mailAttribute);
                    if (attribute != null) {
                        user.setEmail((String) attribute.get());
                    } else {
                        user.setEmail(accountName);
                    }
                }
                user.setAdministrator(isAdmin(accountName));
            } catch (NamingException e) {
                user.setLogin(accountName);
                user.setName(accountName);
                user.setEmail(accountName);
                LOGGER.warn("User lookup error", e);
                span.recordException(e);
                span.setStatus(StatusCode.ERROR, e.getMessage());
            }
            return user;
        } finally {
            span.end();
        }
    }

    public boolean login(String username, String password) {
        Span span = tracer.spanBuilder("ldap.login").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute("ldap.username", username);
            
            ldapOperationCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "login"));
            
            return circuitBreaker.executeSupplier(() -> {
                return retry.executeSupplier(() -> {
                    try {
                        SearchResult ldapUser = lookupUser(username);
                        if (ldapUser != null) {
                            InitialDirContext context = auth(ldapUser.getNameInNamespace(), password);
                            context.close();
                            return true;
                        }
                        return false;
                    } catch (NamingException e) {
                        ldapFailureCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "login"));
                        span.recordException(e);
                        span.setStatus(StatusCode.ERROR, e.getMessage());
                        return false;
                    }
                });
            });
        } catch (Exception e) {
            LOGGER.warn("Login error for user: {}", username, e);
            return false;
        } finally {
            span.end();
        }
    }

    public String encodeForLdap(String input) {
        if (input == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\5c");
                case '*' -> sb.append("\\2a");
                case '(' -> sb.append("\\28");
                case ')' -> sb.append("\\29");
                case '\0' -> sb.append("\\00");
                default -> sb.append(c);
            }
        }
        return sb.toString();
    }

}