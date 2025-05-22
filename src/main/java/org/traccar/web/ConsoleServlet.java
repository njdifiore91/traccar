/*
 * Copyright 2015 - 2023 Anton Tananaev (anton@traccar.org)
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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import org.h2.server.web.ConnectionInfo;
import org.h2.server.web.JakartaWebServlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;

/**
 * H2 Database Console servlet for database administration and debugging.
 * This servlet provides a web interface to the H2 database for development and troubleshooting.
 * 
 * In the microservices architecture, this console is conditionally enabled based on environment
 * and provides secure access controls for containerized deployments.
 * 
 * Key features:
 * - Environment-based conditional enabling (development vs. production)
 * - IP-based access restrictions for security in containerized environments
 * - Support for service discovery in database connection strings
 * - Distributed tracing integration for monitoring database operations
 */

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class ConsoleServlet extends JakartaWebServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsoleServlet.class);
    
    // Keys for configuration properties
    private static final String KEY_WEB_ENVIRONMENT = "web.environment";
    private static final String KEY_DATABASE_CONSOLE_FORCE_ENABLE = "database.console.forceEnable";
    private static final String KEY_DATABASE_CONSOLE_ALLOWED_IPS = "database.console.allowedIps";
    private static final String KEY_DATABASE_CONSOLE_ALLOW_OTHERS = "database.console.allowOthers";
    private static final String KEY_SERVICE_DISCOVERY_TYPE = "service.discovery.type";
    private static final String TRACER_NAME = "org.traccar.web.console";
    private static final Tracer TRACER = GlobalOpenTelemetry.getTracer(TRACER_NAME);

    private final Config config;
    private boolean enabled;
    private Set<String> allowedIpAddresses;

    public ConsoleServlet(Config config) {
        this.config = config;
    }

    @Override
    public void init() {
        // Check if console should be enabled based on environment
        String environment = config.getString(KEY_WEB_ENVIRONMENT, "production");
        boolean forceEnable = config.getBoolean(KEY_DATABASE_CONSOLE_FORCE_ENABLE, false);
        enabled = "development".equalsIgnoreCase(environment) || forceEnable;
        
        if (!enabled) {
            LOGGER.info("H2 Console is disabled in {} environment. Set {} to true to override.", 
                    environment, KEY_DATABASE_CONSOLE_FORCE_ENABLE);
            return;
        }
        
        // Configure IP-based access restrictions
        String allowedIps = config.getString(KEY_DATABASE_CONSOLE_ALLOWED_IPS, "127.0.0.1,::1");
        allowedIpAddresses = new HashSet<>(Arrays.asList(allowedIps.split(",")));
        
        LOGGER.info("Initializing H2 Console with allowed IPs: {}", allowedIpAddresses);
        
        super.init();

        try {
            Field field = JakartaWebServlet.class.getDeclaredField("server");
            field.setAccessible(true);
            org.h2.server.web.WebServer server = (org.h2.server.web.WebServer) field.get(this);

            // Get database connection details with support for service discovery
            String dbUrl = getDatabaseUrl();
            String dbDriver = config.getString(Keys.DATABASE_DRIVER);
            String dbUser = config.getString(Keys.DATABASE_USER);
            
            LOGGER.debug("Configuring H2 Console with URL: {}", dbUrl);
            
            ConnectionInfo connectionInfo = new ConnectionInfo("Traccar|" + dbDriver + "|" + dbUrl + "|" + dbUser);

            Method method;

            method = org.h2.server.web.WebServer.class.getDeclaredMethod("updateSetting", ConnectionInfo.class);
            method.setAccessible(true);
            method.invoke(server, connectionInfo);

            // Configure security settings
            boolean allowOthers = config.getBoolean(KEY_DATABASE_CONSOLE_ALLOW_OTHERS, false);
            method = org.h2.server.web.WebServer.class.getDeclaredMethod("setAllowOthers", boolean.class);
            method.setAccessible(true);
            method.invoke(server, allowOthers);
            
            LOGGER.info("H2 Console initialized successfully with allowOthers={}", allowOthers);

        } catch (NoSuchFieldException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            LOGGER.warn("Console reflection error", e);
        }
    }
    
    /**
     * Get database URL with support for service discovery
     */
    private String getDatabaseUrl() {
        String url = config.getString(Keys.DATABASE_URL);
        
        // Create a span for database URL resolution
        Span span = TRACER.spanBuilder("resolve_database_url")
                .setSpanKind(SpanKind.INTERNAL)
                .setAttribute("db.type", config.getString(Keys.DATABASE_DRIVER, ""))
                .setAttribute("db.operation", "connect")
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Check for service discovery placeholders
            if (url.contains("${service.")) {
                LOGGER.debug("Processing service discovery placeholders in database URL: {}", url);
                
                // Example: replace ${service.database} with actual service name from discovery
                // This is a simplified implementation - in a real system, you would use a service
                // discovery client to resolve the actual service address
                
                String serviceDiscoveryType = config.getString(KEY_SERVICE_DISCOVERY_TYPE, "kubernetes");
                span.setAttribute("service.discovery.type", serviceDiscoveryType);
                
                if ("kubernetes".equals(serviceDiscoveryType)) {
                    // In Kubernetes, services are typically accessed via DNS names like:
                    // service-name.namespace.svc.cluster.local
                    String namespace = System.getenv("KUBERNETES_NAMESPACE");
                    if (namespace == null) {
                        namespace = "default";
                    }
                    
                    url = url.replaceAll("\\$\\{service\\.([^}]+)\\}", "$1." + namespace + ".svc.cluster.local");
                    LOGGER.debug("Resolved database URL for Kubernetes: {}", url);
                } else if ("consul".equals(serviceDiscoveryType)) {
                    // For Consul, you would typically use the Consul API to resolve services
                    // This is a simplified placeholder implementation
                    url = url.replaceAll("\\$\\{service\\.([^}]+)\\}", "$1.service.consul");
                    LOGGER.debug("Resolved database URL for Consul: {}", url);
                }
                
                span.setAttribute("db.connection_string", url);
            }
            
            span.setStatus(StatusCode.OK);
            return url;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error resolving database URL", e);
            return url; // Return original URL on error
        } finally {
            span.end();
        }
    }

    @Override
    public void service(HttpServletRequest request, HttpServletResponse response) 
            throws ServletException, IOException {
        
        if (!enabled) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, 
                    "H2 Console is disabled in this environment");
            return;
        }
        
        // Create a span for the console request
        Span span = TRACER.spanBuilder("h2_console_request")
                .setSpanKind(SpanKind.SERVER)
                .setAttribute("http.method", request.getMethod())
                .setAttribute("http.url", request.getRequestURL().toString())
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Check IP-based access restrictions
            String remoteAddr = request.getRemoteAddr();
            span.setAttribute("client.ip", remoteAddr);
            
            if (!isIpAllowed(remoteAddr)) {
                LOGGER.warn("Blocked H2 Console access from unauthorized IP: {}", remoteAddr);
                span.setAttribute("security.blocked", true);
                span.setStatus(StatusCode.ERROR, "IP address not allowed");
                response.sendError(HttpServletResponse.SC_FORBIDDEN, 
                        "Access to H2 Console is not allowed from your IP address");
                return;
            }
            
            // Process the request
            span.setStatus(StatusCode.OK);
            super.service(request, response);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
    
    /**
     * Check if the given IP address is allowed to access the console
     */
    private boolean isIpAllowed(String ipAddress) {
        // Allow all if the allowed list is empty or contains a wildcard
        if (allowedIpAddresses.isEmpty() || allowedIpAddresses.contains("*")) {
            return true;
        }
        
        // Check exact match
        if (allowedIpAddresses.contains(ipAddress)) {
            return true;
        }
        
        // Check for CIDR notation or subnet masks (simplified implementation)
        // In a production environment, you would use a proper IP range checking library
        for (String allowedIp : allowedIpAddresses) {
            if (allowedIp.contains("/")) {
                // This is a simplified check - in production use a proper CIDR library
                String network = allowedIp.split("/")[0];
                if (ipAddress.startsWith(network.substring(0, network.lastIndexOf('.')))) {
                    return true;
                }
            }
        }
        
        return false;
    }
}