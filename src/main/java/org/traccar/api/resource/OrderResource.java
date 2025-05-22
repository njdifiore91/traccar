/*
 * Copyright 2021 - 2024 Anton Tananaev (anton@traccar.org)
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

import io.micrometer.core.annotation.Timed;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.SimpleObjectResource;
import org.traccar.discovery.ServiceDiscovery;
import org.traccar.model.Order;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * REST resource for managing orders with service discovery, circuit breaking,
 * distributed tracing, and metrics collection.
 */
@Path("orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderResource extends SimpleObjectResource<Order> {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrderResource.class);

    /**
     * Initializes the order resource with required dependencies.
     * Dependencies are injected by the container.
     */
    @Inject
    public OrderResource(ServiceDiscovery serviceDiscovery, Tracer tracer, MeterRegistry meterRegistry) {
        super(Order.class, "description");
        LOGGER.info("Initializing OrderResource with service discovery, tracing, and metrics");
    }

    /**
     * Default constructor for container initialization.
     * Dependencies will be injected after construction.
     */
    public OrderResource() {
        super(Order.class, "description");
    }
}
