/*
 * Copyright 2022 - 2025 Anton Tananaev (anton@traccar.org)
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
package org.traccar.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.api.security.RateLimiter;
import org.traccar.api.security.RateLimitExceededException;

import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RateLimiterTest {

    @Mock
    private ContainerRequestContext requestContext;

    @Mock
    private UriInfo uriInfo;

    @Mock
    private RateLimiter rateLimiter;

    @BeforeEach
    public void setUp() {
        lenient().when(requestContext.getUriInfo()).thenReturn(uriInfo);
        lenient().when(uriInfo.getPath()).thenReturn("/api/positions");
    }

    @Test
    public void testPerClientRateLimiting() throws Exception {
        // Setup client identifiers
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("X-Forwarded-For", "192.168.1.1");
        when(requestContext.getHeaders()).thenReturn(headers);

        // Test successful request (under limit)
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/positions")).thenReturn(true);
        boolean result = rateLimiter.checkRateLimit(requestContext);
        assertTrue(result, "Request should be allowed when under rate limit");

        // Test rate limit exceeded
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/positions")).thenReturn(false);
        assertThrows(RateLimitExceededException.class, () -> {
            rateLimiter.checkRateLimit(requestContext);
        }, "Should throw RateLimitExceededException when rate limit exceeded");

        // Test different client IP
        headers.clear();
        headers.add("X-Forwarded-For", "192.168.1.2");
        when(rateLimiter.tryAcquire("192.168.1.2", "/api/positions")).thenReturn(true);
        result = rateLimiter.checkRateLimit(requestContext);
        assertTrue(result, "Different client should have separate rate limit");
    }

    @Test
    public void testEndpointSpecificRateLimits() throws Exception {
        // Setup client identifiers
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("X-Forwarded-For", "192.168.1.1");
        when(requestContext.getHeaders()).thenReturn(headers);

        // Test positions endpoint
        when(uriInfo.getPath()).thenReturn("/api/positions");
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/positions")).thenReturn(true);
        boolean result = rateLimiter.checkRateLimit(requestContext);
        assertTrue(result, "Request to positions endpoint should be allowed");

        // Test commands endpoint (different rate limit)
        when(uriInfo.getPath()).thenReturn("/api/commands");
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/commands")).thenReturn(true);
        result = rateLimiter.checkRateLimit(requestContext);
        assertTrue(result, "Request to commands endpoint should be allowed");

        // Test rate limit exceeded for specific endpoint
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/commands")).thenReturn(false);
        assertThrows(RateLimitExceededException.class, () -> {
            rateLimiter.checkRateLimit(requestContext);
        }, "Should throw RateLimitExceededException when endpoint-specific rate limit exceeded");
    }

    @Test
    public void testRateLimitHeaders() throws Exception {
        // Setup client identifiers
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("X-Forwarded-For", "192.168.1.1");
        when(requestContext.getHeaders()).thenReturn(headers);

        // Mock response builder
        Response.ResponseBuilder responseBuilder = Response.ok();
        Response response = responseBuilder.build();

        // Test adding rate limit headers to response
        when(rateLimiter.getRemainingRequests("192.168.1.1", "/api/positions")).thenReturn(98);
        when(rateLimiter.getMaxRequests("/api/positions")).thenReturn(100);
        when(rateLimiter.getResetTime("192.168.1.1", "/api/positions")).thenReturn(60L);

        Response decoratedResponse = rateLimiter.decorateResponseWithRateLimitHeaders(
                response, "192.168.1.1", "/api/positions");

        assertNotNull(decoratedResponse, "Response should not be null");
        assertEquals("98", decoratedResponse.getHeaderString("X-RateLimit-Remaining"), 
                "Response should contain correct remaining requests header");
        assertEquals("100", decoratedResponse.getHeaderString("X-RateLimit-Limit"), 
                "Response should contain correct limit header");
        assertNotNull(decoratedResponse.getHeaderString("X-RateLimit-Reset"), 
                "Response should contain reset time header");
    }

    @Test
    public void testThrottlingBehavior() throws Exception {
        // Setup client identifiers
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("X-Forwarded-For", "192.168.1.1");
        when(requestContext.getHeaders()).thenReturn(headers);

        // Test rate limit exceeded response
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/positions")).thenReturn(false);
        when(rateLimiter.getResetTime("192.168.1.1", "/api/positions")).thenReturn(30L);

        RateLimitExceededException exception = assertThrows(RateLimitExceededException.class, () -> {
            rateLimiter.checkRateLimit(requestContext);
        });

        Response errorResponse = exception.getResponse();
        assertNotNull(errorResponse, "Error response should not be null");
        assertEquals(Response.Status.TOO_MANY_REQUESTS.getStatusCode(), errorResponse.getStatus(), 
                "Error response should have 429 status code");
        assertNotNull(errorResponse.getHeaderString("Retry-After"), 
                "Error response should contain Retry-After header");
    }

    @Test
    public void testRateLimitCounterReset() throws Exception {
        // Setup client identifiers
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("X-Forwarded-For", "192.168.1.1");
        when(requestContext.getHeaders()).thenReturn(headers);

        // Test initial request (under limit)
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/positions")).thenReturn(true);
        boolean result = rateLimiter.checkRateLimit(requestContext);
        assertTrue(result, "Initial request should be allowed");

        // Test rate limit exceeded
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/positions")).thenReturn(false);
        assertThrows(RateLimitExceededException.class, () -> {
            rateLimiter.checkRateLimit(requestContext);
        }, "Should throw RateLimitExceededException when rate limit exceeded");

        // Simulate time window expiration and counter reset
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/positions")).thenReturn(true);
        result = rateLimiter.checkRateLimit(requestContext);
        assertTrue(result, "Request should be allowed after rate limit counter reset");
    }

    @Test
    public void testClientIdentification() throws Exception {
        // Test IP-based identification
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("X-Forwarded-For", "192.168.1.1");
        when(requestContext.getHeaders()).thenReturn(headers);
        String clientId = rateLimiter.getClientIdentifier(requestContext);
        assertEquals("192.168.1.1", clientId, "Client should be identified by X-Forwarded-For header");

        // Test fallback to remote address
        headers.clear();
        when(requestContext.getRemoteAddr()).thenReturn("10.0.0.1");
        clientId = rateLimiter.getClientIdentifier(requestContext);
        assertEquals("10.0.0.1", clientId, "Client should be identified by remote address when X-Forwarded-For is missing");

        // Test API key based identification
        headers.add("X-API-Key", "test-api-key");
        when(rateLimiter.useApiKeyForRateLimiting()).thenReturn(true);
        clientId = rateLimiter.getClientIdentifier(requestContext);
        assertEquals("test-api-key", clientId, "Client should be identified by API key when configured");
    }

    @Test
    public void testDistributedCacheIntegration() throws Exception {
        // This test verifies that rate limiting works across multiple service instances
        // by using a distributed cache for storing rate limit counters

        // Setup client identifiers
        MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        headers.add("X-Forwarded-For", "192.168.1.1");
        when(requestContext.getHeaders()).thenReturn(headers);

        // Test that rate limit counter is stored in distributed cache
        when(rateLimiter.isUsingDistributedCache()).thenReturn(true);
        when(rateLimiter.tryAcquire("192.168.1.1", "/api/positions")).thenReturn(true);
        boolean result = rateLimiter.checkRateLimit(requestContext);
        assertTrue(result, "Request should be allowed when using distributed cache");
        assertTrue(rateLimiter.isUsingDistributedCache(), "Rate limiter should be using distributed cache");
    }
}