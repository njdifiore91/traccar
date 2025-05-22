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
package org.traccar.storage;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.OpenTelemetry;
import jakarta.inject.Singleton;
import org.traccar.config.Config;

/**
 * Guice module for providing ObjectStorage implementation.
 */
public class ObjectStorageProvider extends AbstractModule {

    /**
     * Provides the appropriate ObjectStorage implementation based on configuration.
     *
     * @param config Configuration
     * @param openTelemetry OpenTelemetry instance
     * @param meterRegistry Metrics registry
     * @return ObjectStorage implementation
     */
    @Provides
    @Singleton
    public ObjectStorage provideObjectStorage(
            Config config, OpenTelemetry openTelemetry, MeterRegistry meterRegistry) {
        String storageType = config.getString("storage.type", "file");
        
        if ("s3".equalsIgnoreCase(storageType)) {
            return new S3ObjectStorage(config, openTelemetry.getTracer("org.traccar.storage"), meterRegistry);
        } else {
            return new FileObjectStorage(config, openTelemetry.getTracer("org.traccar.storage"), meterRegistry);
        }
    }
}