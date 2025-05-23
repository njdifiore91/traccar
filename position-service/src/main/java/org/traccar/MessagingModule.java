/*
 * Copyright 2024 Anton Tananaev (anton@traccar.org)
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr353.JSR353Module;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import jakarta.inject.Singleton;

/**
 * Guice module for messaging-related dependencies.
 * Configures message publisher and related components.
 */
public class MessagingModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(MessagePublisher.class).toProvider(MessagePublisherProvider.class).in(Scopes.SINGLETON);
        bind(EnrichedPositionProducer.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public ObjectMapper provideObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JSR353Module());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        return objectMapper;
    }

    @Provides
    @Singleton
    public Tracer provideTracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer("org.traccar.position-service");
    }
}