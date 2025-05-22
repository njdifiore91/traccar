/*
 * Copyright 2012 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.web;

import com.google.inject.Injector;
import com.google.inject.servlet.GuiceFilter;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.eclipse.jetty.http.HttpCookie;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.RequestLogWriter;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.server.session.DefaultSessionCache;
import org.eclipse.jetty.server.session.SessionCache;
import org.eclipse.jetty.server.session.SessionHandler;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.server.config.JettyWebSocketServletContainerInitializer;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.session.data.redis.RedisSessionRepository;
import org.springframework.session.web.http.SessionRepositoryFilter;
import org.traccar.LifecycleObject;
import org.traccar.api.CorsResponseFilter;
import org.traccar.api.DateParameterConverterProvider;
import org.traccar.api.ResourceErrorHandler;
import org.traccar.api.resource.ServerResource;
import org.traccar.api.security.SecurityRequestFilter;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.ObjectMapperContextResolver;
import org.traccar.web.api.ApiServlet;
import org.traccar.web.api.HealthCheckServlet;
import org.traccar.web.api.MetricsServlet;
import org.traccar.web.api.ServiceDiscoveryClient;
import org.traccar.web.messaging.MessageBrokerManager;

import jakarta.servlet.DispatcherType;
import jakarta.servlet.ServletException;
import jakarta.servlet.SessionCookieConfig;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.EnumSet;

/**
 * Web server component that serves as the API Gateway in the microservices architecture.
 * Handles HTTP requests, WebSocket connections, service discovery, circuit breaking,
 * health checks, metrics collection, and session management.
 */
public class WebServer implements LifecycleObject {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebServer.class);

    private final Injector injector;
    private final Config config;
    private final Server server;
    private final CircuitBreakerRegistry circuitBreakerRegistry;
    private final MeterRegistry meterRegistry;
    private final ServiceDiscoveryClient serviceDiscoveryClient;
    private final MessageBrokerManager messageBrokerManager;
    private RedisConnectionFactory redisConnectionFactory;
    private RedisSessionRepository sessionRepository;

    public WebServer(Injector injector, Config config) {
        this.injector = injector;
        this.config = config;
        String address = config.getString(Keys.WEB_ADDRESS);
        int port = config.getInteger(Keys.WEB_PORT);
        if (address == null) {
            server = new Server(port);
        } else {
            server = new Server(new InetSocketAddress(address, port));
        }

        // Initialize metrics registry
        meterRegistry = createMeterRegistry();

        // Initialize circuit breaker registry
        circuitBreakerRegistry = createCircuitBreakerRegistry();

        // Initialize service discovery client
        serviceDiscoveryClient = new ServiceDiscoveryClient(config, meterRegistry);

        // Initialize message broker manager
        messageBrokerManager = new MessageBrokerManager(config, meterRegistry);

        // Initialize Redis for session management if enabled
        if (config.getBoolean("web.session.redis.enabled")) {
            initRedisSessionManagement();
        }

        ServletContextHandler servletHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        JettyWebSocketServletContainerInitializer.configure(servletHandler, null);
        servletHandler.addFilter(GuiceFilter.class, "/*", EnumSet.allOf(DispatcherType.class));

        initApi(servletHandler);
        initSessionConfig(servletHandler);
        initHealthChecks(servletHandler);
        initMetrics(servletHandler);

        if (config.getBoolean(Keys.WEB_CONSOLE)) {
            servletHandler.addServlet(new ServletHolder(new ConsoleServlet(config)), "/console/*");
        }

        initWebApp(servletHandler);

        servletHandler.setErrorHandler(new ErrorHandler() {
            @Override
            protected void handleErrorPage(
                    HttpServletRequest request, Writer writer, int code, String message) throws IOException {
                writer.write("<!DOCTYPE><html><head><title>Error</title></head><html><body>"
                        + code + " - " + HttpStatus.getMessage(code) + "</body></html>");
            }
        });

        HandlerList handlers = new HandlerList();
        initClientProxy(handlers);
        handlers.addHandler(servletHandler);
        handlers.addHandler(new GzipHandler());
        server.setHandler(handlers);

        if (config.hasKey(Keys.WEB_REQUEST_LOG_PATH)) {
            RequestLogWriter logWriter = new RequestLogWriter(config.getString(Keys.WEB_REQUEST_LOG_PATH));
            logWriter.setAppend(true);
            logWriter.setRetainDays(config.getInteger(Keys.WEB_REQUEST_LOG_RETAIN_DAYS));
            server.setRequestLog(new WebRequestLog(logWriter));
        }
    }

    private MeterRegistry createMeterRegistry() {
        if (config.getBoolean("web.metrics.prometheus.enabled", false)) {
            return new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        } else {
            return new SimpleMeterRegistry();
        }
    }

    private CircuitBreakerRegistry createCircuitBreakerRegistry() {
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(config.getFloat("web.circuitBreaker.failureRateThreshold", 50.0f))
                .waitDurationInOpenState(Duration.ofMillis(
                        config.getLong("web.circuitBreaker.waitDurationInOpenState", 10000L)))
                .permittedNumberOfCallsInHalfOpenState(
                        config.getInteger("web.circuitBreaker.permittedNumberOfCallsInHalfOpenState", 10))
                .slidingWindowSize(config.getInteger("web.circuitBreaker.slidingWindowSize", 100))
                .minimumNumberOfCalls(config.getInteger("web.circuitBreaker.minimumNumberOfCalls", 10))
                .build();

        CircuitBreakerRegistry registry = CircuitBreakerRegistry.of(circuitBreakerConfig);

        // Register default circuit breaker
        CircuitBreaker defaultCircuitBreaker = registry.circuitBreaker("default");
        LOGGER.info("Initialized default circuit breaker with state: {}", defaultCircuitBreaker.getState());

        return registry;
    }

    private void initRedisSessionManagement() {
        try {
            // Configure Redis connection
            RedisStandaloneConfiguration redisConfig = new RedisStandaloneConfiguration();
            redisConfig.setHostName(config.getString("web.session.redis.host", "localhost"));
            redisConfig.setPort(config.getInteger("web.session.redis.port", 6379));
            
            String password = config.getString("web.session.redis.password", null);
            if (password != null && !password.isEmpty()) {
                redisConfig.setPassword(password);
            }
            
            String database = config.getString("web.session.redis.database", "0");
            redisConfig.setDatabase(Integer.parseInt(database));

            // Create Redis connection factory
            redisConnectionFactory = new LettuceConnectionFactory(redisConfig);
            ((LettuceConnectionFactory) redisConnectionFactory).afterPropertiesSet();

            // Create Redis session repository
            sessionRepository = new RedisSessionRepository(redisConnectionFactory);
            sessionRepository.setDefaultMaxInactiveInterval(config.getInteger(Keys.WEB_SESSION_TIMEOUT, 1800));

            LOGGER.info("Redis session management initialized successfully");
        } catch (Exception e) {
            LOGGER.error("Failed to initialize Redis session management", e);
        }
    }

    private void initHealthChecks(ServletContextHandler servletHandler) {
        // Add health check endpoints for Kubernetes liveness and readiness probes
        servletHandler.addServlet(
                new ServletHolder(new HealthCheckServlet(config, circuitBreakerRegistry, serviceDiscoveryClient)),
                "/health/*");
        LOGGER.info("Health check endpoints initialized at /health/*");
    }

    private void initMetrics(ServletContextHandler servletHandler) {
        // Add metrics endpoint for Prometheus scraping
        if (meterRegistry instanceof PrometheusMeterRegistry) {
            servletHandler.addServlet(
                    new ServletHolder(new MetricsServlet((PrometheusMeterRegistry) meterRegistry)),
                    "/metrics");
            LOGGER.info("Metrics endpoint initialized at /metrics");
        }
    }

    private void initClientProxy(HandlerList handlers) {
        int port = config.getInteger(Keys.PROTOCOL_PORT.withPrefix("osmand"));
        if (port != 0) {
            ServletContextHandler servletHandler = new ServletContextHandler() {
                @Override
                public void doScope(
                        String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
                        throws IOException, ServletException {
                    if (target.equals("/") && request.getMethod().equals(HttpMethod.POST.asString())) {
                        super.doScope(target, baseRequest, request, response);
                    }
                }
            };
            ServletHolder servletHolder = new ServletHolder(AsyncProxyServlet.Transparent.class);
            servletHolder.setInitParameter("proxyTo", "http://localhost:" + port);
            servletHandler.addServlet(servletHolder, "/");
            handlers.addHandler(servletHandler);
        }
    }

    private void initWebApp(ServletContextHandler servletHandler) {
        ServletHolder servletHolder = new ServletHolder(new DefaultOverrideServlet(config));
        servletHolder.setInitParameter("resourceBase", new File(config.getString(Keys.WEB_PATH)).getAbsolutePath());
        servletHolder.setInitParameter("dirAllowed", "false");
        if (config.getBoolean(Keys.WEB_DEBUG)) {
            servletHandler.setWelcomeFiles(new String[] {"debug.html", "index.html"});
        } else {
            String cache = config.getString(Keys.WEB_CACHE_CONTROL);
            if (cache != null && !cache.isEmpty()) {
                servletHolder.setInitParameter("cacheControl", cache);
            }
            servletHandler.setWelcomeFiles(new String[] {"release.html", "index.html"});
        }
        servletHandler.addServlet(servletHolder, "/*");
    }

    private void initApi(ServletContextHandler servletHandler) {
        String mediaPath = config.getString(Keys.MEDIA_PATH);
        if (mediaPath != null) {
            ServletHolder servletHolder = new ServletHolder(DefaultServlet.class);
            servletHolder.setInitParameter("resourceBase", new File(mediaPath).getAbsolutePath());
            servletHolder.setInitParameter("dirAllowed", "false");
            servletHolder.setInitParameter("pathInfoOnly", "true");
            servletHandler.addServlet(servletHolder, "/api/media/*");
        }

        // Add API Gateway servlet for routing requests to microservices
        servletHandler.addServlet(
                new ServletHolder(new ApiServlet(config, serviceDiscoveryClient, circuitBreakerRegistry, meterRegistry)),
                "/api/v1/*");

        // Legacy API endpoints
        ResourceConfig resourceConfig = new ResourceConfig();
        resourceConfig.property("jersey.config.server.wadl.disableWadl", true);
        resourceConfig.registerClasses(
                JacksonFeature.class,
                ObjectMapperContextResolver.class,
                DateParameterConverterProvider.class,
                SecurityRequestFilter.class,
                CorsResponseFilter.class,
                ResourceErrorHandler.class);
        resourceConfig.packages(ServerResource.class.getPackage().getName());
        if (resourceConfig.getClasses().stream().filter(ServerResource.class::equals).findAny().isEmpty()) {
            LOGGER.warn("Failed to load API resources");
        }
        servletHandler.addServlet(new ServletHolder(new ServletContainer(resourceConfig)), "/api/*");
    }

    private void initSessionConfig(ServletContextHandler servletHandler) {
        if (sessionRepository != null) {
            // Use Redis-based session management
            try {
                SessionRepositoryFilter<org.springframework.session.Session> filter = 
                        new SessionRepositoryFilter<>(sessionRepository);
                servletHandler.addFilter(filter.getClass().getName(), filter, "/*", EnumSet.allOf(DispatcherType.class));
                LOGGER.info("Redis-based session management configured");
            } catch (Exception e) {
                LOGGER.error("Failed to configure Redis session management", e);
            }
        } else if (config.getBoolean(Keys.WEB_PERSIST_SESSION)) {
            // Fallback to JDBC session management if Redis is not configured
            LOGGER.info("Using JDBC-based session management");
            // Original JDBC session management code is kept for backward compatibility
            try {
                Class.forName("org.eclipse.jetty.server.session.DatabaseAdaptor");
                Class.forName("org.eclipse.jetty.server.session.JDBCSessionDataStoreFactory");
                
                Object databaseAdaptor = Class.forName("org.eclipse.jetty.server.session.DatabaseAdaptor")
                        .getDeclaredConstructor().newInstance();
                
                // Set datasource using reflection
                databaseAdaptor.getClass().getMethod("setDatasource", javax.sql.DataSource.class)
                        .invoke(databaseAdaptor, injector.getInstance(javax.sql.DataSource.class));
                
                Object jdbcSessionDataStoreFactory = Class.forName("org.eclipse.jetty.server.session.JDBCSessionDataStoreFactory")
                        .getDeclaredConstructor().newInstance();
                
                // Set database adaptor using reflection
                jdbcSessionDataStoreFactory.getClass().getMethod("setDatabaseAdaptor", databaseAdaptor.getClass())
                        .invoke(jdbcSessionDataStoreFactory, databaseAdaptor);
                
                SessionHandler sessionHandler = servletHandler.getSessionHandler();
                SessionCache sessionCache = new DefaultSessionCache(sessionHandler);
                
                // Get session data store using reflection
                Object sessionDataStore = jdbcSessionDataStoreFactory.getClass()
                        .getMethod("getSessionDataStore", SessionHandler.class)
                        .invoke(jdbcSessionDataStoreFactory, sessionHandler);
                
                // Set session data store using reflection
                sessionCache.getClass().getMethod("setSessionDataStore", Class.forName("org.eclipse.jetty.server.session.SessionDataStore"))
                        .invoke(sessionCache, sessionDataStore);
                
                sessionHandler.setSessionCache(sessionCache);
            } catch (Exception e) {
                LOGGER.warn("Failed to initialize JDBC session store", e);
            }
        }

        SessionCookieConfig sessionCookieConfig = servletHandler.getServletContext().getSessionCookieConfig();

        int sessionTimeout = config.getInteger(Keys.WEB_SESSION_TIMEOUT);
        if (sessionTimeout > 0) {
            servletHandler.getSessionHandler().setMaxInactiveInterval(sessionTimeout);
            sessionCookieConfig.setMaxAge(sessionTimeout);
        }

        String sameSiteCookie = config.getString(Keys.WEB_SAME_SITE_COOKIE);
        if (sameSiteCookie != null) {
            switch (sameSiteCookie.toLowerCase()) {
                case "lax":
                    sessionCookieConfig.setComment(HttpCookie.SAME_SITE_LAX_COMMENT);
                    break;
                case "strict":
                    sessionCookieConfig.setComment(HttpCookie.SAME_SITE_STRICT_COMMENT);
                    break;
                case "none":
                    sessionCookieConfig.setSecure(true);
                    sessionCookieConfig.setComment(HttpCookie.SAME_SITE_NONE_COMMENT);
                    break;
                default:
                    break;
            }
        }

        sessionCookieConfig.setHttpOnly(true);
    }

    @Override
    public void start() throws Exception {
        // Register with service discovery if enabled
        if (config.getBoolean("web.serviceDiscovery.enabled", false)) {
            serviceDiscoveryClient.register();
        }
        
        // Connect to message broker if enabled
        if (config.getBoolean("web.messageBroker.enabled", false)) {
            messageBrokerManager.connect();
        }
        
        server.start();
        LOGGER.info("Web server started");
    }

    @Override
    public void stop() throws Exception {
        // Disconnect from message broker if enabled
        if (config.getBoolean("web.messageBroker.enabled", false)) {
            messageBrokerManager.disconnect();
        }
        
        // Deregister from service discovery if enabled
        if (config.getBoolean("web.serviceDiscovery.enabled", false)) {
            serviceDiscoveryClient.deregister();
        }
        
        // Close Redis connection if used
        if (redisConnectionFactory != null) {
            ((LettuceConnectionFactory) redisConnectionFactory).destroy();
        }
        
        server.stop();
        LOGGER.info("Web server stopped");
    }

}