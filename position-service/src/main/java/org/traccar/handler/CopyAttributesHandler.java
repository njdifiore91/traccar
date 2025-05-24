/*
 * Copyright 2016 - 2024 Anton Tananaev (anton@traccar.org)
 * Copyright 2016 - 2017 Andrey Kunitsyn (andrey@traccar.org)
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

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.model.AttributeUtil;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

import java.util.Arrays;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CopyAttributesHandler extends BasePositionHandler {

    private final CacheManager cacheManager;
    private final Tracer tracer;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    
    private static final AttributeKey<String> ATTRIBUTE_NAMES = AttributeKey.stringKey("attribute.names");
    private static final AttributeKey<Long> DEVICE_ID = AttributeKey.longKey("device.id");
    private static final AttributeKey<Integer> ATTRIBUTES_COPIED = AttributeKey.longKey("attributes.copied");

    @Inject
    public CopyAttributesHandler(Config config, CacheManager cacheManager, Tracer tracer) {
        this.cacheManager = cacheManager;
        this.tracer = tracer;
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        Span span = tracer.spanBuilder("CopyAttributesHandler.onPosition").startSpan();
        try (Scope scope = span.makeCurrent()) {
            span.setAttribute(DEVICE_ID, position.getDeviceId());
            
            String attributesString = AttributeUtil.lookup(
                    cacheManager, Keys.PROCESSING_COPY_ATTRIBUTES, position.getDeviceId());
            
            if (attributesString != null) {
                span.setAttribute(ATTRIBUTE_NAMES, attributesString);
                
                // Use read lock for retrieving the last position
                Position last;
                lock.readLock().lock();
                try {
                    last = cacheManager.getPosition(position.getDeviceId());
                } finally {
                    lock.readLock().unlock();
                }
                
                if (last != null) {
                    int attributesCopied = 0;
                    String[] attributes = attributesString.split("[ ,]");
                    
                    // Use write lock when modifying the position attributes
                    lock.writeLock().lock();
                    try {
                        for (String attribute : attributes) {
                            if (last.hasAttribute(attribute) && !position.hasAttribute(attribute)) {
                                position.getAttributes().put(attribute, last.getAttributes().get(attribute));
                                attributesCopied++;
                            }
                        }
                    } finally {
                        lock.writeLock().unlock();
                    }
                    
                    span.setAttribute(ATTRIBUTES_COPIED, attributesCopied);
                }
            }
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            span.recordException(e);
            throw e;
        } finally {
            span.end();
            callback.processed(false);
        }
    }
}