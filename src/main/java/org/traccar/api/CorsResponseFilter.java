/*
 * Copyright 2015 - 2022 Anton Tananaev (anton@traccar.org)
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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.prometheus.client.Counter;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import java.io.IOException;

/**
 * CORS response filter that adds appropriate headers to enable cross-origin requests.
 * This implementation supports distributed tracing headers and collects metrics for CORS requests.
 */
@Singleton
public class CorsResponseFilter implements ContainerResponseFilter {

    private final String allowed;

    // Prometheus metrics for CORS requests
    private static final Counter CORS_REQUESTS = Counter.build()
            .name("api_cors_requests_total")
            .help("Total number of CORS requests")
            .labelNames("origin", "method")
            .register();

    @Inject
    public CorsResponseFilter(Config config) {
        allowed = config.getString(Keys.WEB_ORIGIN);
    }

    private static final String ORIGIN_ALL = "*";
    
    // Updated to include distributed tracing headers
    private static final String HEADERS_ALL = "origin, content-type, accept, authorization, traceparent, tracestate, "
            + "x-b3-traceid, x-b3-spanid, x-b3-parentspanid, x-b3-sampled, x-request-id, x-ot-span-context, "
            + "x-cloud-trace-context, grpc-trace-bin, baggage, sentry-trace, x-forwarded-for, x-forwarded-proto, "
            + "x-forwarded-host, x-forwarded-port";
    
    private static final String METHODS_ALL = "GET, POST, PUT, DELETE, OPTIONS";

    @Override
    public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
        // Record metrics for CORS requests
        String origin = request.getHeaderString(HttpHeaderNames.ORIGIN.toString());
        String method = request.getMethod();
        
        if (origin != null) {
            // Only count actual CORS requests (those with Origin header)
            CORS_REQUESTS.labels(origin, method).inc();
        }

        if (!response.getHeaders().containsKey(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS.toString())) {
            response.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS.toString(), HEADERS_ALL);
        }

        if (!response.getHeaders().containsKey(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS.toString())) {
            response.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_CREDENTIALS.toString(), true);
        }

        if (!response.getHeaders().containsKey(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS.toString())) {
            response.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS.toString(), METHODS_ALL);
        }

        // Add header to expose tracing headers to clients
        if (!response.getHeaders().containsKey(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS.toString())) {
            response.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_EXPOSE_HEADERS.toString(), 
                    "traceparent, tracestate, x-request-id");
        }

        if (!response.getHeaders().containsKey(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN.toString())) {
            if (origin == null) {
                response.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN.toString(), ORIGIN_ALL);
            } else if (allowed == null || allowed.equals(ORIGIN_ALL) || allowed.contains(origin)) {
                response.getHeaders().add(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN.toString(), origin);
            }
        }
    }
}