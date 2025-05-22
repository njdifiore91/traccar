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
package org.traccar.web.api;

import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * Servlet that exposes Prometheus metrics for monitoring.
 */
public class MetricsServlet extends HttpServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetricsServlet.class);

    private final PrometheusMeterRegistry meterRegistry;

    public MetricsServlet(PrometheusMeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType("text/plain; version=0.0.4; charset=utf-8");
        
        try {
            // Write the scrape data in Prometheus text format
            resp.getWriter().write(meterRegistry.scrape());
        } catch (Exception e) {
            LOGGER.error("Error scraping Prometheus metrics", e);
            resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error scraping metrics");
        }
    }
}