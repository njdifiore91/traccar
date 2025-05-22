/*
 * Copyright 2015 - 2024 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.GlobalTracer;
import org.eclipse.jetty.websocket.server.JettyWebSocketServlet;
import org.eclipse.jetty.websocket.server.JettyWebSocketServletFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.traccar.api.security.LoginService;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.SessionHelper;
import org.traccar.messaging.MessageBrokerManager;
import org.traccar.session.ConnectionManager;
import org.traccar.session.discovery.ServiceDiscoveryManager;
import org.traccar.session.store.DistributedSessionStore;
import org.traccar.storage.Storage;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.servlet.http.HttpSession;
import org.traccar.storage.StorageException;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Singleton
public class AsyncSocketServlet extends JettyWebSocketServlet {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncSocketServlet.class);

    private final Config config;
    private final ObjectMapper objectMapper;
    private final ConnectionManager connectionManager;
    private final Storage storage;
    private final LoginService loginService;
    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final DistributedSessionStore webSocketSessionStore;
    private final MessageBrokerManager messageBrokerManager;
    private final MeterRegistry meterRegistry;

    // Metrics
    private final Counter webSocketConnectionCounter;
    private final Counter webSocketDisconnectionCounter;
    private final Timer authenticationTimer;
    private final Timer sessionOperationTimer;

    @Inject
    public AsyncSocketServlet(
            Config config, ObjectMapper objectMapper, ConnectionManager connectionManager, Storage storage,
            LoginService loginService, ServiceDiscoveryManager serviceDiscoveryManager,
            DistributedSessionStore webSocketSessionStore, MessageBrokerManager messageBrokerManager,
            MeterRegistry meterRegistry) {
        this.config = config;
        this.objectMapper = objectMapper;
        this.connectionManager = connectionManager;
        this.storage = storage;
        this.loginService = loginService;
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.webSocketSessionStore = webSocketSessionStore;
        this.messageBrokerManager = messageBrokerManager;
        this.meterRegistry = meterRegistry;

        // Initialize metrics
        this.webSocketConnectionCounter = meterRegistry.counter("websocket.connections");
        this.webSocketDisconnectionCounter = meterRegistry.counter("websocket.disconnections");
        this.authenticationTimer = meterRegistry.timer("websocket.authentication.time");
        this.sessionOperationTimer = meterRegistry.timer("websocket.session.operation.time");

        // Register with service discovery
        serviceDiscoveryManager.register("websocket-service", "api");
    }

    @Override
    public void configure(JettyWebSocketServletFactory factory) {
        factory.setIdleTimeout(Duration.ofMillis(config.getLong(Keys.WEB_TIMEOUT)));
        factory.setCreator((req, resp) -> {
            String correlationId = UUID.randomUUID().toString();
            MDC.put("correlationId", correlationId);
            Tracer tracer = GlobalTracer.get();
            Span span = tracer.buildSpan("createWebSocket").start();
            
            try {
                Long userId = null;
                List<String> tokens = req.getParameterMap().get("token");
                
                // Authenticate user
                if (tokens != null && !tokens.isEmpty()) {
                    String token = tokens.iterator().next();
                    span.setTag("authenticationType", "token");
                    try {
                        userId = authenticationTimer.record(() -> {
                            try {
                                return loginService.login(token).getUser().getId();
                            } catch (StorageException | GeneralSecurityException | IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
                    } catch (RuntimeException e) {
                        Tags.ERROR.set(span, true);
                        span.log(Map.of("error.message", e.getMessage()));
                        LOGGER.warn("WebSocket authentication failed with token: {}", e.getMessage());
                        return null;
                    }
                } else if (req.getSession() != null) {
                    span.setTag("authenticationType", "session");
                    userId = (Long) ((HttpSession) req.getSession()).getAttribute(SessionHelper.USER_ID_KEY);
                }
                
                if (userId != null) {
                    span.setTag("userId", userId);
                    
                    // Generate a unique connection ID for this WebSocket
                    String connectionId = UUID.randomUUID().toString();
                    span.setTag("connectionId", connectionId);
                    
                    // Store connection metadata in distributed session store
                    Map<String, Object> metadata = new HashMap<>();
                    metadata.put("userId", userId);
                    metadata.put("remoteAddress", req.getRemoteAddress());
                    metadata.put("userAgent", req.getHeader("User-Agent"));
                    metadata.put("connectionTime", System.currentTimeMillis());
                    
                    sessionOperationTimer.record(() -> {
                        webSocketSessionStore.storeWebSocketSession(connectionId, metadata);
                        return null;
                    });
                    
                    // Increment connection counter
                    webSocketConnectionCounter.increment();
                    
                    // Create AsyncSocket with connection ID and message broker for multiplexing
                    return new AsyncSocket(objectMapper, connectionManager, storage, userId, 
                            connectionId, webSocketSessionStore, messageBrokerManager, meterRegistry);
                }
                return null;
            } finally {
                span.finish();
                MDC.remove("correlationId");
            }
        });
    }
}
