/*
 * Copyright 2022 - 2024 Anton Tananaev (anton@traccar.org)
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
package org.traccar;

import com.google.inject.Inject;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Refill;
import io.netty.handler.codec.http.HttpHeaderNames;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.CorsConfigurationSource;
import org.springframework.web.cors.UrlBasedCorsConfigurationSource;
import org.traccar.api.CorsResponseFilter;
import org.traccar.api.security.SecurityRequestFilter;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Security configuration for the API Gateway service.
 * Configures authentication, authorization, CORS, rate limiting, and security headers.
 */
@Configuration
@EnableWebSecurity
@Order(1)
public class ApiGatewaySecurityConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiGatewaySecurityConfig.class);

    private final SecurityRequestFilter securityRequestFilter;
    private final Config config;

    @Inject
    public ApiGatewaySecurityConfig(SecurityRequestFilter securityRequestFilter, Config config) {
        this.securityRequestFilter = securityRequestFilter;
        this.config = config;
    }

    /**
     * Configures the security filter chain for the API Gateway.
     * Sets up JWT authentication, authorization rules, CORS, and security headers.
     */
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        // Configure security settings
        http
            // Disable CSRF as we're using token-based authentication
            .csrf().disable()
            // Configure CORS
            .cors().configurationSource(corsConfigurationSource()).and()
            // Configure session management to be stateless
            .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS).and()
            // Configure authorization rules
            .authorizeHttpRequests()
                // Public endpoints that don't require authentication
                .requestMatchers("/api/session", "/api/session/**", "/api/users/token").permitAll()
                // Swagger/OpenAPI documentation endpoints
                .requestMatchers("/v3/api-docs/**", "/swagger-ui/**", "/swagger-ui.html").permitAll()
                // Health check endpoints
                .requestMatchers("/actuator/health", "/actuator/info").permitAll()
                // All other endpoints require authentication
                .anyRequest().authenticated().and()
            // Add custom security filters
            .addFilterBefore(securityRequestFilter, UsernamePasswordAuthenticationFilter.class)
            .addFilterAfter(new RateLimitingFilter(), SecurityRequestFilter.class)
            // Configure security headers
            .headers()
                .xssProtection().and()
                .contentSecurityPolicy("default-src 'self'").and()
                .frameOptions().deny().and()
                .contentTypeOptions().and();

        return http.build();
    }

    /**
     * Configures CORS settings for the API Gateway.
     * Allows cross-origin requests with appropriate restrictions.
     */
    @Bean
    public CorsConfigurationSource corsConfigurationSource() {
        CorsConfiguration configuration = new CorsConfiguration();
        String allowedOrigin = config.getString(Keys.WEB_ORIGIN);
        
        if (allowedOrigin != null) {
            if (allowedOrigin.equals("*")) {
                configuration.setAllowedOrigins(Collections.singletonList("*"));
            } else {
                configuration.setAllowedOrigins(Arrays.asList(allowedOrigin.split(",")));
            }
        }
        
        configuration.setAllowedMethods(Arrays.asList("GET", "POST", "PUT", "DELETE", "OPTIONS"));
        configuration.setAllowedHeaders(Arrays.asList(
                "Origin", "Content-Type", "Accept", "Authorization", "X-Requested-With"));
        configuration.setExposedHeaders(Arrays.asList(
                "Access-Control-Allow-Origin", "Access-Control-Allow-Credentials"));
        configuration.setAllowCredentials(true);
        configuration.setMaxAge(3600L);

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return source;
    }

    /**
     * Filter implementation for API rate limiting.
     * Prevents abuse by limiting the number of requests per client.
     */
    public class RateLimitingFilter implements Filter {

        private final Map<String, Bucket> buckets = new ConcurrentHashMap<>();
        private final int requestsPerMinute;
        private final int burstCapacity;

        public RateLimitingFilter() {
            // Default values if not configured
            this.requestsPerMinute = config.getInteger(Keys.WEB_RATE_LIMIT_REQUESTS_PER_MINUTE, 60);
            this.burstCapacity = config.getInteger(Keys.WEB_RATE_LIMIT_BURST_CAPACITY, 10);
            LOGGER.info("Rate limiting configured with {} requests per minute and burst capacity of {}",
                    requestsPerMinute, burstCapacity);
        }

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
                throws IOException, ServletException {
            
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            HttpServletResponse httpResponse = (HttpServletResponse) response;

            // Skip rate limiting for OPTIONS requests (CORS preflight)
            if (httpRequest.getMethod().equals("OPTIONS")) {
                chain.doFilter(request, response);
                return;
            }

            // Get client identifier (IP address or user ID if authenticated)
            String clientId = getClientIdentifier(httpRequest);
            
            // Get or create rate limit bucket for this client
            Bucket bucket = buckets.computeIfAbsent(clientId, this::createBucket);
            
            // Try to consume a token from the bucket
            if (bucket.tryConsume(1)) {
                // Request is allowed, proceed with the filter chain
                chain.doFilter(request, response);
            } else {
                // Rate limit exceeded, return 429 Too Many Requests
                httpResponse.setStatus(HttpServletResponse.SC_TOO_MANY_REQUESTS);
                httpResponse.setContentType("application/json");
                httpResponse.setHeader("Retry-After", "60"); // Suggest retry after 60 seconds
                httpResponse.getWriter().write("{\"error\":\"Rate limit exceeded. Please try again later.\"}");
                LOGGER.debug("Rate limit exceeded for client: {}", clientId);
            }
        }

        private String getClientIdentifier(HttpServletRequest request) {
            // Use user ID if authenticated, otherwise use IP address
            Object userId = request.getAttribute("userId");
            if (userId != null) {
                return "user-" + userId;
            } else {
                return "ip-" + request.getRemoteAddr();
            }
        }

        private Bucket createBucket(String clientId) {
            // Create a token bucket with the configured rate limit
            Bandwidth limit = Bandwidth.classic(burstCapacity, 
                    Refill.intervally(requestsPerMinute, Duration.ofMinutes(1)));
            return Bucket.builder().addLimit(limit).build();
        }

        @Override
        public void destroy() {
            // Clean up resources
            buckets.clear();
        }
    }
}