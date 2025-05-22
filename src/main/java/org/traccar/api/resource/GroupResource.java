/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
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
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.api.SimpleObjectResource;
import org.traccar.model.Group;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

/**
 * JAX-RS resource for managing groups.
 * Extends SimpleObjectResource to inherit service discovery, circuit breaking,
 * distributed tracing, and metrics collection capabilities.
 */
@Path("groups")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class GroupResource extends SimpleObjectResource<Group> {

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupResource.class);
    
    @Inject
    private Tracer tracer;

    /**
     * Initializes the GroupResource with Group class and "name" as the sort field.
     * Inherits service discovery, circuit breaking, distributed tracing, and metrics
     * collection from SimpleObjectResource.
     */
    public GroupResource() {
        super(Group.class, "name");
        LOGGER.debug("Initializing GroupResource with service discovery and observability support");
    }

    /**
     * Creates a span for group-specific operations.
     * This method can be used for custom group operations that need tracing.
     * 
     * @param operationName The name of the operation being performed
     * @return A new span for the operation
     */
    protected Span createGroupOperationSpan(String operationName) {
        return tracer.spanBuilder("group_" + operationName)
                .setParent(Context.current())
                .setAttribute("resource.type", "Group")
                .startSpan();
    }
}
