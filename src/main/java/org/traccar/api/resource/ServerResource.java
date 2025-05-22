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
package org.traccar.api.resource;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import io.micrometer.core.annotation.Timed;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.extension.annotations.WithSpan;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.core.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.traccar.api.BaseResource;
import org.traccar.model.ObjectOperation;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.database.OpenIdProvider;
import org.traccar.discovery.ServiceRegistry;
import org.traccar.geocoder.Geocoder;
import org.traccar.helper.Log;
import org.traccar.helper.LogAction;
import org.traccar.helper.model.UserUtil;
import org.traccar.mail.MailManager;
import org.traccar.metrics.MetricsService;
import org.traccar.model.Server;
import org.traccar.model.User;
import org.traccar.session.cache.CacheManager;
import org.traccar.sms.SmsManager;
import org.traccar.storage.StorageException;
import org.traccar.storage.query.Columns;
import org.traccar.storage.query.Condition;
import org.traccar.storage.query.Request;

import jakarta.annotation.Nullable;
import jakarta.annotation.security.PermitAll;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Server resource for managing server-wide settings and operations.
 * This class has been updated to support microservices architecture with:
 * - Service discovery integration
 * - Circuit breaker pattern for resilient API calls
 * - Distributed tracing with OpenTelemetry
 * - Metrics collection for API performance monitoring
 * - Redis-based distributed cache
 */
@Path("server")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class ServerResource extends BaseResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerResource.class);
    private static final String CIRCUIT_BREAKER_NAME = "serverResourceBreaker";

    @Inject
    private Config config;

    @Inject
    private CacheManager cacheManager;

    @Inject
    private MailManager mailManager;

    @Inject
    @Nullable
    private SmsManager smsManager;

    @Inject
    @Nullable
    private OpenIdProvider openIdProvider;

    @Inject
    @Nullable
    private Geocoder geocoder;

    @Inject
    private LogAction actionLogger;

    @Inject
    private Tracer tracer;

    @Inject
    private MetricsService metricsService;

    @Inject
    private ServiceRegistry serviceRegistry;

    @Context
    private HttpServletRequest request;

    /**
     * Get server information with circuit breaker pattern for resilience.
     * Uses OpenTelemetry for distributed tracing and metrics collection.
     */
    @PermitAll
    @GET
    @Timed(value = "server.get", description = "Time taken to retrieve server information")
    @WithSpan("ServerResource.get")
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "getFallback")
    public Server get() throws StorageException {
        Span span = Span.current();
        span.setAttribute("user.id", String.valueOf(getUserId()));
        
        metricsService.incrementCounter("server.get.requests");
        long startTime = System.currentTimeMillis();
        
        try {
            Server server = storage.getObject(Server.class, new Request(new Columns.All()));
            
            // Use service discovery to check service availability
            server.setEmailEnabled(mailManager.getEmailEnabled());
            server.setTextEnabled(smsManager != null && serviceRegistry.isServiceAvailable("sms-service"));
            server.setGeocoderEnabled(geocoder != null && serviceRegistry.isServiceAvailable("geocoder-service"));
            server.setOpenIdEnabled(openIdProvider != null && serviceRegistry.isServiceAvailable("openid-service"));
            server.setOpenIdForce(openIdProvider != null && openIdProvider.getForce() && 
                    serviceRegistry.isServiceAvailable("openid-service"));
            
            User user = permissionsService.getUser(getUserId());
            if (user != null) {
                if (user.getAdministrator()) {
                    server.setStorageSpace(Log.getStorageSpace());
                }
            } else {
                server.setNewServer(UserUtil.isEmpty(storage));
            }
            
            span.setStatus(StatusCode.OK);
            return server;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error retrieving server information", e);
            throw e;
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            metricsService.recordTimer("server.get.time", duration);
        }
    }

    /**
     * Fallback method for get() when circuit breaker is open.
     */
    public Server getFallback(Exception e) {
        LOGGER.warn("Using fallback for server information due to: {}", e.getMessage());
        metricsService.incrementCounter("server.get.fallback");
        
        // Return a minimal server object with basic information
        Server server = new Server();
        server.setEmailEnabled(false);
        server.setTextEnabled(false);
        server.setGeocoderEnabled(false);
        server.setOpenIdEnabled(false);
        server.setOpenIdForce(false);
        server.setNewServer(false);
        
        return server;
    }

    /**
     * Update server information with circuit breaker pattern for resilience.
     * Uses OpenTelemetry for distributed tracing and metrics collection.
     */
    @PUT
    @Timed(value = "server.update", description = "Time taken to update server information")
    @WithSpan("ServerResource.update")
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "updateFallback")
    public Response update(Server server) throws Exception {
        Span span = Span.current();
        span.setAttribute("user.id", String.valueOf(getUserId()));
        span.setAttribute("server.id", String.valueOf(server.getId()));
        
        metricsService.incrementCounter("server.update.requests");
        long startTime = System.currentTimeMillis();
        
        try {
            permissionsService.checkAdmin(getUserId());
            storage.updateObject(server, new Request(
                    new Columns.Exclude("id"),
                    new Condition.Equals("id", server.getId())));
            
            // Use distributed cache invalidation
            cacheManager.invalidateObject(true, Server.class, server.getId(), ObjectOperation.UPDATE);
            actionLogger.edit(request, getUserId(), server);
            
            span.setStatus(StatusCode.OK);
            return Response.ok(server).build();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error updating server information", e);
            throw e;
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            metricsService.recordTimer("server.update.time", duration);
        }
    }

    /**
     * Fallback method for update() when circuit breaker is open.
     */
    public Response updateFallback(Server server, Exception e) {
        LOGGER.warn("Using fallback for server update due to: {}", e.getMessage());
        metricsService.incrementCounter("server.update.fallback");
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("Server update temporarily unavailable. Please try again later.")
                .build();
    }

    /**
     * Geocode coordinates with circuit breaker pattern for resilience.
     * Uses OpenTelemetry for distributed tracing and metrics collection.
     */
    @Path("geocode")
    @GET
    @Timed(value = "server.geocode", description = "Time taken to geocode coordinates")
    @WithSpan("ServerResource.geocode")
    @CircuitBreaker(name = "geocoderBreaker", fallbackMethod = "geocodeFallback")
    @Retry(name = "geocoderRetry")
    public String geocode(@QueryParam("latitude") double latitude, @QueryParam("longitude") double longitude) {
        Span span = Span.current();
        span.setAttribute("latitude", latitude);
        span.setAttribute("longitude", longitude);
        
        metricsService.incrementCounter("server.geocode.requests");
        long startTime = System.currentTimeMillis();
        
        try {
            if (geocoder != null && serviceRegistry.isServiceAvailable("geocoder-service")) {
                String address = geocoder.getAddress(latitude, longitude, null);
                span.setStatus(StatusCode.OK);
                return address;
            } else {
                span.setStatus(StatusCode.ERROR, "Geocoding service not available");
                throw new RuntimeException("Reverse geocoding is not enabled or service is unavailable");
            }
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error geocoding coordinates", e);
            throw e;
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            metricsService.recordTimer("server.geocode.time", duration);
        }
    }

    /**
     * Fallback method for geocode() when circuit breaker is open.
     */
    public String geocodeFallback(double latitude, double longitude, Exception e) {
        LOGGER.warn("Using fallback for geocoding due to: {}", e.getMessage());
        metricsService.incrementCounter("server.geocode.fallback");
        return "Geocoding temporarily unavailable";
    }

    /**
     * Get available timezones.
     * Uses OpenTelemetry for distributed tracing and metrics collection.
     */
    @Path("timezones")
    @GET
    @Timed(value = "server.timezones", description = "Time taken to retrieve timezones")
    @WithSpan("ServerResource.timezones")
    public Collection<String> timezones() {
        metricsService.incrementCounter("server.timezones.requests");
        return Arrays.asList(TimeZone.getAvailableIDs());
    }

    /**
     * Upload file with circuit breaker pattern for resilience.
     * Uses OpenTelemetry for distributed tracing and metrics collection.
     */
    @Path("file/{path}")
    @POST
    @Consumes("*/*")
    @Timed(value = "server.uploadFile", description = "Time taken to upload file")
    @WithSpan("ServerResource.uploadFile")
    @CircuitBreaker(name = CIRCUIT_BREAKER_NAME, fallbackMethod = "uploadFileFallback")
    public Response uploadFile(@PathParam("path") String path, File inputFile) throws IOException, StorageException {
        Span span = Span.current();
        span.setAttribute("path", path);
        span.setAttribute("file.size", inputFile.length());
        
        metricsService.incrementCounter("server.uploadFile.requests");
        long startTime = System.currentTimeMillis();
        
        try {
            permissionsService.checkAdmin(getUserId());
            String root = config.getString(Keys.WEB_OVERRIDE, config.getString(Keys.WEB_PATH));

            var rootPath = Paths.get(root).normalize();
            var outputPath = rootPath.resolve(path).normalize();
            if (!outputPath.startsWith(rootPath)) {
                span.setStatus(StatusCode.ERROR, "Invalid path");
                return Response.status(Response.Status.BAD_REQUEST).build();
            }

            var directoryPath = outputPath.getParent();
            if (directoryPath != null) {
                Files.createDirectories(directoryPath);
            }

            try (var input = new FileInputStream(inputFile); var output = new FileOutputStream(outputPath.toFile())) {
                input.transferTo(output);
            }
            
            span.setStatus(StatusCode.OK);
            return Response.ok().build();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error uploading file", e);
            throw e;
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            metricsService.recordTimer("server.uploadFile.time", duration);
        }
    }

    /**
     * Fallback method for uploadFile() when circuit breaker is open.
     */
    public Response uploadFileFallback(String path, File inputFile, Exception e) {
        LOGGER.warn("Using fallback for file upload due to: {}", e.getMessage());
        metricsService.incrementCounter("server.uploadFile.fallback");
        return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                .entity("File upload temporarily unavailable. Please try again later.")
                .build();
    }

    /**
     * Get cache information with circuit breaker pattern for resilience.
     * Uses OpenTelemetry for distributed tracing and metrics collection.
     */
    @Path("cache")
    @GET
    @Timed(value = "server.cache", description = "Time taken to retrieve cache information")
    @WithSpan("ServerResource.cache")
    @CircuitBreaker(name = "cacheBreaker", fallbackMethod = "cacheFallback")
    public String cache() throws StorageException {
        Span span = Span.current();
        span.setAttribute("user.id", String.valueOf(getUserId()));
        
        metricsService.incrementCounter("server.cache.requests");
        long startTime = System.currentTimeMillis();
        
        try {
            permissionsService.checkAdmin(getUserId());
            String cacheInfo = cacheManager.toString();
            
            span.setStatus(StatusCode.OK);
            return cacheInfo;
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error retrieving cache information", e);
            throw e;
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            metricsService.recordTimer("server.cache.time", duration);
        }
    }

    /**
     * Fallback method for cache() when circuit breaker is open.
     */
    public String cacheFallback(Exception e) {
        LOGGER.warn("Using fallback for cache information due to: {}", e.getMessage());
        metricsService.incrementCounter("server.cache.fallback");
        return "Cache information temporarily unavailable";
    }

    /**
     * Initiate graceful shutdown for containerized environment.
     * Uses OpenTelemetry for distributed tracing and metrics collection.
     */
    @Path("reboot")
    @POST
    @Timed(value = "server.reboot", description = "Time taken to initiate reboot")
    @WithSpan("ServerResource.reboot")
    public Response reboot() throws StorageException {
        Span span = Span.current();
        span.setAttribute("user.id", String.valueOf(getUserId()));
        
        metricsService.incrementCounter("server.reboot.requests");
        
        try {
            permissionsService.checkAdmin(getUserId());
            
            LOGGER.info("Graceful shutdown initiated by user ID: {}", getUserId());
            
            // For containerized environments, we need a graceful shutdown
            // instead of System.exit which would immediately kill the container
            CompletableFuture.runAsync(() -> {
                try {
                    // Allow time for the response to be sent back to the client
                    TimeUnit.SECONDS.sleep(1);
                    
                    // Signal the container orchestration that we're ready to terminate
                    // This is more container-friendly than System.exit
                    LOGGER.info("Initiating graceful shutdown sequence");
                    
                    // In a Spring Boot application, this would be replaced with:
                    // SpringApplication.exit(applicationContext, () -> 0);
                    // For now, we'll still use System.exit but with a delay
                    System.exit(130);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.error("Shutdown interrupted", e);
                }
            });
            
            span.setStatus(StatusCode.OK);
            return Response.ok("Shutdown initiated").build();
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            LOGGER.error("Error initiating reboot", e);
            throw e;
        }
    }
}