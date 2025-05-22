/*
 * Copyright 2016 - 2023 Anton Tananaev (anton@traccar.org)
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
package org.traccar.protocol;

import java.util.TimeZone;

import org.traccar.StringProtocolEncoder;
import org.traccar.model.Command;
import org.traccar.Protocol;
import org.traccar.discovery.ServiceDiscoveryManager;
import org.traccar.messaging.MessageProducer;
import org.traccar.messaging.MessageType;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import javax.inject.Inject;

public class Jt600ProtocolEncoder extends StringProtocolEncoder {

    private final ServiceDiscoveryManager serviceDiscoveryManager;
    private final MessageProducer messageProducer;
    private final Tracer tracer;
    private final MeterRegistry meterRegistry;
    
    private final Counter commandsCounter;
    private final Timer commandsTimer;

    @Inject
    public Jt600ProtocolEncoder(
            Protocol protocol, 
            ServiceDiscoveryManager serviceDiscoveryManager,
            MessageProducer messageProducer,
            Tracer tracer,
            MeterRegistry meterRegistry) {
        super(protocol);
        this.serviceDiscoveryManager = serviceDiscoveryManager;
        this.messageProducer = messageProducer;
        this.tracer = tracer;
        this.meterRegistry = meterRegistry;
        
        // Initialize metrics
        this.commandsCounter = Counter.builder("protocol.jt600.commands")
                .description("Number of commands encoded by JT600 protocol")
                .register(meterRegistry);
        this.commandsTimer = Timer.builder("protocol.jt600.command.duration")
                .description("Time taken to encode JT600 commands")
                .register(meterRegistry);
    }

    @Override
    protected Object encodeCommand(Command command) {
        // Start the timer for this operation
        Timer.Sample sample = Timer.start(meterRegistry);
        
        // Create a span for distributed tracing
        Span span = tracer.spanBuilder("jt600.encode.command")
                .setAttribute("protocol.command.type", command.getType())
                .setAttribute("protocol.device.id", String.valueOf(command.getDeviceId()))
                .startSpan();
        
        try (Scope scope = span.makeCurrent()) {
            // Increment the commands counter
            commandsCounter.increment();
            
            String result = switch (command.getType()) {
                case Command.TYPE_ENGINE_STOP -> "(S07,0)";
                case Command.TYPE_ENGINE_RESUME -> "(S07,1)";
                case Command.TYPE_SET_TIMEZONE -> {
                    int offset = TimeZone.getTimeZone(command.getString(Command.KEY_TIMEZONE)).getRawOffset() / 60000;
                    yield "(S09,1," + offset + ")";
                }
                case Command.TYPE_REBOOT_DEVICE -> "(S17)";
                default -> null;
            };
            
            // If command was successfully encoded, publish to message broker for async processing
            if (result != null) {
                publishCommandStatus(command, true);
                span.setAttribute("protocol.command.success", true);
            } else {
                publishCommandStatus(command, false);
                span.setAttribute("protocol.command.success", false);
            }
            
            return result;
        } catch (Exception e) {
            // Record the exception in the span
            span.recordException(e);
            span.setAttribute("protocol.command.success", false);
            
            // Publish failure status to message broker
            publishCommandStatus(command, false);
            
            // Re-throw the exception
            throw e;
        } finally {
            // Stop the span
            span.end();
            
            // Record the final timing
            sample.stop(commandsTimer);
        }
    }
    
    /**
     * Publishes command status to the message broker for asynchronous processing
     * 
     * @param command The command being processed
     * @param success Whether the command was successfully encoded
     */
    private void publishCommandStatus(Command command, boolean success) {
        try {
            // Only publish if message producer is available (service-based mode)
            if (messageProducer != null) {
                messageProducer.publish(
                    MessageType.COMMAND_RESULT,
                    command.getDeviceId(),
                    command.toJson()
                );
            }
        } catch (Exception e) {
            // Log but don't fail the command processing if publishing fails
            logger.warn("Failed to publish command status to message broker", e);
        }
    }
}