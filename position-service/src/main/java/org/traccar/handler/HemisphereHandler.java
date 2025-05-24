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
package org.traccar.handler;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import jakarta.inject.Inject;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Position;

/**
 * Handler responsible for enforcing hemisphere-based sign normalization of Position objects.
 * This class is thread-safe as it only uses immutable fields set during construction.
 */
public class HemisphereHandler extends BasePositionHandler {

    private static final String TRACER_NAME = "org.traccar.handler.HemisphereHandler";
    private static final AttributeKey<String> LATITUDE_HEMISPHERE_KEY = AttributeKey.stringKey("latitudeHemisphere");
    private static final AttributeKey<String> LONGITUDE_HEMISPHERE_KEY = AttributeKey.stringKey("longitudeHemisphere");
    private static final AttributeKey<Boolean> LATITUDE_ADJUSTED_KEY = AttributeKey.booleanKey("latitudeAdjusted");
    private static final AttributeKey<Boolean> LONGITUDE_ADJUSTED_KEY = AttributeKey.booleanKey("longitudeAdjusted");

    private final int latitudeFactor;
    private final int longitudeFactor;
    private final Tracer tracer;
    private final String latitudeHemisphere;
    private final String longitudeHemisphere;

    /**
     * Constructs a new HemisphereHandler with the specified configuration.
     * 
     * @param config The configuration containing hemisphere settings
     */
    @Inject
    public HemisphereHandler(Config config) {
        latitudeHemisphere = config.getString(Keys.LOCATION_LATITUDE_HEMISPHERE);
        latitudeFactor = parseHemisphereFactor(latitudeHemisphere, "N", "S");
        
        longitudeHemisphere = config.getString(Keys.LOCATION_LONGITUDE_HEMISPHERE);
        longitudeFactor = parseHemisphereFactor(longitudeHemisphere, "E", "W");
        
        tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
    }
    
    /**
     * Parses a hemisphere string into a sign factor (1, -1, or 0).
     * 
     * @param hemisphere The hemisphere string from configuration
     * @param positive The string representing the positive hemisphere
     * @param negative The string representing the negative hemisphere
     * @return 1 for positive hemisphere, -1 for negative hemisphere, 0 if not specified
     */
    private int parseHemisphereFactor(String hemisphere, String positive, String negative) {
        if (hemisphere != null) {
            if (hemisphere.equalsIgnoreCase(positive)) {
                return 1;
            } else if (hemisphere.equalsIgnoreCase(negative)) {
                return -1;
            }
        }
        return 0;
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        Span span = tracer.spanBuilder("normalizeHemisphere")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            boolean latitudeAdjusted = false;
            boolean longitudeAdjusted = false;
            
            if (latitudeFactor != 0) {
                position.setLatitude(Math.abs(position.getLatitude()) * latitudeFactor);
                latitudeAdjusted = true;
            }
            
            if (longitudeFactor != 0) {
                position.setLongitude(Math.abs(position.getLongitude()) * longitudeFactor);
                longitudeAdjusted = true;
            }
            
            span.setAllAttributes(Attributes.of(
                    LATITUDE_HEMISPHERE_KEY, latitudeHemisphere != null ? latitudeHemisphere : "unspecified",
                    LONGITUDE_HEMISPHERE_KEY, longitudeHemisphere != null ? longitudeHemisphere : "unspecified",
                    LATITUDE_ADJUSTED_KEY, latitudeAdjusted,
                    LONGITUDE_ADJUSTED_KEY, longitudeAdjusted));
            
            span.setStatus(StatusCode.OK);
            callback.processed(false);
        } catch (Exception e) {
            span.recordException(e);
            span.setStatus(StatusCode.ERROR);
            throw e;
        } finally {
            span.end();
        }
    }
}