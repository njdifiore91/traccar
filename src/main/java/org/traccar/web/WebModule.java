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
package org.traccar.web;

import com.google.inject.servlet.ServletModule;
import org.traccar.api.AsyncSocketServlet;
import org.traccar.api.MediaFilter;
import org.traccar.api.CircuitBreakerFilter;
import org.traccar.api.DistributedTracingFilter;
import org.traccar.api.MetricsFilter;
import org.traccar.api.ServiceDiscoveryFilter;
import org.traccar.api.WebSocketMultiplexerServlet;

/**
 * Web module for configuring the API Gateway HTTP request pipeline.
 * This module sets up filters and servlets for the microservices architecture.
 */
public class WebModule extends ServletModule {

    @Override
    protected void configureServlets() {
        // Global filters applied to all requests
        filter("/*").through(OverrideFilter.class);
        filter("/*").through(DistributedTracingFilter.class);
        filter("/*").through(MetricsFilter.class);
        
        // API-specific filters
        filter("/api/*").through(ServiceDiscoveryFilter.class);
        filter("/api/*").through(CircuitBreakerFilter.class);
        filter("/api/*").through(ThrottlingFilter.class);
        filter("/api/media/*").through(MediaFilter.class);
        
        // Servlet mappings
        serve("/api/socket").with(AsyncSocketServlet.class);
        serve("/api/ws").with(WebSocketMultiplexerServlet.class);
    }
}