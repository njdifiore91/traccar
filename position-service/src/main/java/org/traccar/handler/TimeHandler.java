/*
 * Copyright 2019 - 2024 Anton Tananaev (anton@traccar.org)
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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TimeHandler extends BasePositionHandler {

    private static final String TRACER_NAME = "org.traccar.handler.TimeHandler";
    private static final AttributeKey<String> PROTOCOL_KEY = AttributeKey.stringKey("protocol");
    private static final AttributeKey<Boolean> USE_SERVER_TIME_KEY = AttributeKey.booleanKey("useServerTime");
    private static final AttributeKey<Boolean> PROTOCOL_MATCHED_KEY = AttributeKey.booleanKey("protocolMatched");

    private final boolean useServerTime;
    private final Set<String> protocols;
    private final Tracer tracer;

    @Inject
    public TimeHandler(Config config) {
        useServerTime = config.getString(Keys.TIME_OVERRIDE).equalsIgnoreCase("serverTime");
        String protocolList = config.getString(Keys.TIME_PROTOCOLS);
        if (protocolList != null) {
            protocols = new HashSet<>(Arrays.asList(protocolList.split("[, ]")));
        } else {
            protocols = null;
        }
        tracer = GlobalOpenTelemetry.getTracer(TRACER_NAME);
    }

    @Override
    public void onPosition(Position position, Callback callback) {
        Span span = tracer.spanBuilder("adjustTimestamp")
                .setSpanKind(SpanKind.INTERNAL)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            boolean protocolMatched = protocols == null || protocols.contains(position.getProtocol());
            span.setAllAttributes(Attributes.of(
                    PROTOCOL_KEY, position.getProtocol(),
                    USE_SERVER_TIME_KEY, useServerTime,
                    PROTOCOL_MATCHED_KEY, protocolMatched));

            if (protocolMatched) {
                if (useServerTime) {
                    position.setDeviceTime(position.getServerTime());
                    position.setFixTime(position.getServerTime());
                } else {
                    position.setFixTime(position.getDeviceTime());
                }
            }
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