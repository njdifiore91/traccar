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
package org.traccar.observability;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Netty channel handler that implements circuit breaker pattern using Resilience4j.
 * Prevents cascading failures when external services are unavailable.
 */
public class CircuitBreakerHandler extends ChannelDuplexHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreakerHandler.class);

    private final CircuitBreaker circuitBreaker;

    public CircuitBreakerHandler(CircuitBreaker circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
        
        // Register event listener for state transitions
        circuitBreaker.getEventPublisher()
                .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' state changed from {} to {}",
                        event.getCircuitBreakerName(), event.getStateTransition().getFromState(),
                        event.getStateTransition().getToState()));
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        try {
            // Execute write operation through circuit breaker
            Supplier<Void> decoratedSupplier = CircuitBreaker.decorateSupplier(circuitBreaker, () -> {
                ctx.write(msg, promise);
                return null;
            });
            
            decoratedSupplier.get();
        } catch (CallNotPermittedException e) {
            // Circuit is open, implement fallback behavior
            LOGGER.warn("Circuit breaker is open, using fallback for write operation");
            handleFallback(ctx, msg, promise);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Record failure in circuit breaker if it's a relevant exception
        if (isRelevantException(cause)) {
            circuitBreaker.onError(0, TimeUnit.MILLISECONDS, cause);
        }
        
        ctx.fireExceptionCaught(cause);
    }

    /**
     * Handles fallback behavior when circuit breaker is open
     */
    private void handleFallback(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
        // Implement graceful degradation
        // For example, we could:
        // 1. Use cached data if available
        // 2. Return a default response
        // 3. Queue the message for later processing
        
        // For now, we'll just complete the promise with failure
        promise.setFailure(new CallNotPermittedException(circuitBreaker));
    }

    /**
     * Determines if an exception should be counted by the circuit breaker
     */
    private boolean isRelevantException(Throwable cause) {
        // Consider network and timeout exceptions as relevant for circuit breaking
        return cause instanceof java.io.IOException
                || cause instanceof TimeoutException
                || cause instanceof java.net.ConnectException;
    }
}