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
package org.traccar.handler;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.Date;

/**
 * Handler for processing outdated positions by restoring GPS data from the last known position.
 * This handler is part of the position processing pipeline in the position-service.
 */
@Singleton
public class OutdatedHandler extends BasePositionHandler {

    private final CacheManager cacheManager;
    private final Tracer tracer;

    private static final AttributeKey<Long> DEVICE_ID_KEY = AttributeKey.longKey("device.id");
    private static final AttributeKey<Boolean> OUTDATED_KEY = AttributeKey.booleanKey("position.outdated");
    private static final AttributeKey<Boolean> RESTORED_KEY = AttributeKey.booleanKey("position.restored");

    /**
     * Constructs a new OutdatedHandler with the specified cache manager.
     *
     * @param cacheManager The cache manager used to retrieve the last known position.
     */
    @Inject
    public OutdatedHandler(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
        this.tracer = GlobalOpenTelemetry.getTracer("org.traccar.handler.OutdatedHandler");
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        Span span = tracer.spanBuilder("OutdatedHandler.onPosition").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAllAttributes(Attributes.of(
                    DEVICE_ID_KEY, position.getDeviceId(),
                    OUTDATED_KEY, position.getOutdated()));

            if (position.getOutdated()) {
                Position last = cacheManager.getPosition(position.getDeviceId());
                boolean restored = false;
                
                if (last != null) {
                    position.setFixTime(last.getFixTime());
                    position.setValid(last.getValid());
                    position.setLatitude(last.getLatitude());
                    position.setLongitude(last.getLongitude());
                    position.setAltitude(last.getAltitude());
                    position.setSpeed(last.getSpeed());
                    position.setCourse(last.getCourse());
                    position.setAccuracy(last.getAccuracy());
                    restored = true;
                } else {
                    position.setFixTime(new Date(315964819000L)); // gps epoch 1980-01-06
                }
                
                if (position.getDeviceTime() == null) {
                    position.setDeviceTime(position.getServerTime());
                }
                
                span.setAttribute(RESTORED_KEY, restored);
            }
            
            callback.processed(false);
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR, e.getMessage());
            throw e;
        } finally {
            span.end();
        }
    }
}