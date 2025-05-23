package org.traccar;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.github.resilience4j.core.registry.EntryAddedEvent;
import io.github.resilience4j.core.registry.EntryRemovedEvent;
import io.github.resilience4j.core.registry.EntryReplacedEvent;
import io.github.resilience4j.core.registry.RegistryEventConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

/**
 * Configuration class for circuit breakers in the Position Processing Service.
 * 
 * This class configures Resilience4j circuit breakers for external service calls
 * to ensure the service remains resilient when external dependencies fail or
 * experience performance degradation.
 */
@Configuration
public class CircuitBreakerConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(CircuitBreakerConfig.class);

    /**
     * Creates a CircuitBreakerRegistry with default and custom configurations.
     * 
     * @return The configured CircuitBreakerRegistry
     */
    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        return CircuitBreakerRegistry.of(defaultCircuitBreakerConfig());
    }

    /**
     * Creates the default CircuitBreakerConfig with standard settings.
     * 
     * @return The default CircuitBreakerConfig
     */
    private io.github.resilience4j.circuitbreaker.CircuitBreakerConfig defaultCircuitBreakerConfig() {
        return io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                .slidingWindowType(SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(10)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
    }

    /**
     * Creates a CircuitBreakerConfig for geocoding service calls.
     * 
     * @return The geocoding service CircuitBreakerConfig
     */
    @Bean
    public io.github.resilience4j.circuitbreaker.CircuitBreakerConfig geocodingServiceCircuitBreakerConfig() {
        return io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                .slidingWindowType(SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(50)
                .failureRateThreshold(25)
                .waitDurationInOpenState(Duration.ofSeconds(60))
                .permittedNumberOfCallsInHalfOpenState(5)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
    }

    /**
     * Creates a CircuitBreakerConfig for speed limit service calls.
     * 
     * @return The speed limit service CircuitBreakerConfig
     */
    @Bean
    public io.github.resilience4j.circuitbreaker.CircuitBreakerConfig speedLimitServiceCircuitBreakerConfig() {
        return io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                .slidingWindowType(SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(50)
                .failureRateThreshold(30)
                .waitDurationInOpenState(Duration.ofSeconds(45))
                .permittedNumberOfCallsInHalfOpenState(5)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
    }

    /**
     * Creates a CircuitBreakerConfig for geolocation service calls.
     * 
     * @return The geolocation service CircuitBreakerConfig
     */
    @Bean
    public io.github.resilience4j.circuitbreaker.CircuitBreakerConfig geolocationServiceCircuitBreakerConfig() {
        return io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                .slidingWindowType(SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(50)
                .failureRateThreshold(25)
                .waitDurationInOpenState(Duration.ofSeconds(60))
                .permittedNumberOfCallsInHalfOpenState(5)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
    }

    /**
     * Creates a CircuitBreakerConfig for forward service calls.
     * 
     * @return The forward service CircuitBreakerConfig
     */
    @Bean
    public io.github.resilience4j.circuitbreaker.CircuitBreakerConfig forwardServiceCircuitBreakerConfig() {
        return io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.custom()
                .slidingWindowType(SlidingWindowType.COUNT_BASED)
                .slidingWindowSize(100)
                .failureRateThreshold(40)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(10)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build();
    }

    /**
     * Registers event consumers for circuit breaker events to monitor state changes.
     * 
     * @return The registry event consumer for circuit breakers
     */
    @Bean
    public RegistryEventConsumer<CircuitBreaker> circuitBreakerEventConsumer() {
        return new RegistryEventConsumer<>() {
            @Override
            public void onEntryAddedEvent(EntryAddedEvent<CircuitBreaker> entryAddedEvent) {
                CircuitBreaker circuitBreaker = entryAddedEvent.getAddedEntry();
                LOGGER.info("Circuit breaker '{}' added", circuitBreaker.getName());
                
                // Register state transition listener
                circuitBreaker.getEventPublisher()
                        .onStateTransition(event -> LOGGER.info("Circuit breaker '{}' state changed from {} to {}", 
                                event.getCircuitBreakerName(), 
                                event.getStateTransition().getFromState(),
                                event.getStateTransition().getToState()));
                
                // Register success/failure listeners
                circuitBreaker.getEventPublisher()
                        .onSuccess(event -> LOGGER.debug("Circuit breaker '{}' recorded success in {}ms", 
                                event.getCircuitBreakerName(), 
                                event.getElapsedDuration().toMillis()));
                
                circuitBreaker.getEventPublisher()
                        .onError(event -> LOGGER.warn("Circuit breaker '{}' recorded error: {}", 
                                event.getCircuitBreakerName(), 
                                event.getThrowable().getMessage()));
            }

            @Override
            public void onEntryRemovedEvent(EntryRemovedEvent<CircuitBreaker> entryRemoveEvent) {
                CircuitBreaker circuitBreaker = entryRemoveEvent.getRemovedEntry();
                LOGGER.info("Circuit breaker '{}' removed", circuitBreaker.getName());
            }

            @Override
            public void onEntryReplacedEvent(EntryReplacedEvent<CircuitBreaker> entryReplacedEvent) {
                CircuitBreaker oldCircuitBreaker = entryReplacedEvent.getOldEntry();
                CircuitBreaker newCircuitBreaker = entryReplacedEvent.getNewEntry();
                LOGGER.info("Circuit breaker '{}' replaced with '{}'", 
                        oldCircuitBreaker.getName(), 
                        newCircuitBreaker.getName());
            }
        };
    }
}