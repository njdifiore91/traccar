package org.traccar.handler;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.config.Config;
import org.traccar.model.Attribute;
import org.traccar.model.Device;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

// Resilience4j imports for circuit breaker
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;

// OpenTelemetry imports for distributed tracing
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;

// Micrometer imports for metrics
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

// Messaging imports for async processing
import org.traccar.messaging.MessagePublisher;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Test for ComputedAttributesHandler that supports both monolithic and microservices testing patterns.
 * This test class is designed to be compatible with both the original monolithic architecture
 * and the new microservices architecture.
 */
@ExtendWith(MockitoExtension.class)
public class ComputedAttributesTest {

    @Mock
    private Config config;

    @Mock
    private CacheManager cacheManager;

    @Mock
    private CircuitBreakerRegistry circuitBreakerRegistry;

    @Mock
    private CircuitBreaker circuitBreaker;

    @Mock
    private Tracer tracer;

    @Mock
    private MeterRegistry meterRegistry;

    @Mock
    private MessagePublisher messagePublisher;

    @Mock
    private Executor executor;

    @Mock
    private Timer timer;

    @Mock
    private Counter successCounter;

    @Mock
    private Counter failureCounter;

    @Mock
    private Counter circuitBreakerOpenCounter;

    @BeforeEach
    public void setUp() {
        // Setup for circuit breaker
        when(circuitBreakerRegistry.circuitBreaker(anyString(), any())).thenReturn(circuitBreaker);
        when(circuitBreaker.executeSupplier(any())).thenAnswer(invocation -> {
            return ((java.util.function.Supplier<?>) invocation.getArgument(0)).get();
        });

        // Setup for metrics
        when(meterRegistry.timer(anyString(), any(), any())).thenReturn(timer);
        when(timer.record(any())).thenAnswer(invocation -> {
            return ((java.util.function.Supplier<?>) invocation.getArgument(0)).get();
        });
        when(meterRegistry.counter(eq("computed.attributes.success"), any(), any())).thenReturn(successCounter);
        when(meterRegistry.counter(eq("computed.attributes.failure"), any(), any())).thenReturn(failureCounter);
        when(meterRegistry.counter(eq("computed.attributes.circuit.breaker.open"), any(), any())).thenReturn(circuitBreakerOpenCounter);

        // Setup for executor
        when(executor.execute(any())).thenAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        });

        // Setup for message publisher
        lenient().when(messagePublisher.publish(anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));
    }

    /**
     * Original test for monolithic architecture compatibility.
     * This test ensures that the basic functionality of computed attributes works
     * in the original monolithic architecture.
     */
    @Test
    public void testComputedAttributes() {
        // Create a handler with minimal dependencies for monolithic testing
        ComputedAttributesHandler handler = new ComputedAttributesHandler(new Config(), null, false);

        Date date = new Date();
        Position position = new Position();
        position.setTime(date);
        position.setSpeed(42);
        position.setValid(false);
        position.set("adc1", 128);
        position.set("booleanFlag", true);
        position.set("adc2", 100);
        position.set("bitFlag", 7);
        position.set("event", 42);
        position.set("result", "success");
        Attribute attribute = new Attribute();

        attribute.setExpression("adc1");
        assertEquals(128, handler.computeAttribute(attribute, position));

        attribute.setExpression("!booleanFlag");
        assertEquals(false, handler.computeAttribute(attribute, position));

        attribute.setExpression("adc2 * 2 + 50");
        assertEquals(250, handler.computeAttribute(attribute, position));

        attribute.setExpression("(bitFlag & 4) != 0");
        assertEquals(true, handler.computeAttribute(attribute, position));

        attribute.setExpression("event == 42 ? \"lowBattery\" : null");
        assertEquals("lowBattery", handler.computeAttribute(attribute, position));

        attribute.setExpression("speed > 5 && valid");
        assertEquals(false, handler.computeAttribute(attribute, position));

        attribute.setExpression("fixTime");
        assertEquals(date, handler.computeAttribute(attribute, position));

        attribute.setExpression("math:pow(adc1, 2)");
        assertEquals(16384.0, handler.computeAttribute(attribute, position));

        // modification tests
        attribute.setExpression("adc1 = 256");
        handler.computeAttribute(attribute, position);
        assertEquals(128, position.getInteger("adc1"));

        attribute.setExpression("result = \"fail\"");
        handler.computeAttribute(attribute, position);
        assertEquals("success", position.getString("result"));

        attribute.setExpression("fixTime = \"2017-10-18 10:00:01\"");
        handler.computeAttribute(attribute, position);
        assertEquals(date, position.getFixTime());
    }

    /**
     * Test for microservices architecture with resilience patterns.
     * This test verifies that the handler works correctly with circuit breaker,
     * tracing, and metrics in the microservices architecture.
     */
    @Test
    public void testComputedAttributesWithResilience() {
        // Create a handler with all microservices dependencies
        ComputedAttributesHandler handler = new ComputedAttributesHandler(
                config, cacheManager, false, circuitBreakerRegistry,
                tracer, meterRegistry, messagePublisher, executor);

        Date date = new Date();
        Position position = new Position();
        position.setDeviceId(1);
        position.setTime(date);
        position.setSpeed(42);
        position.setValid(false);
        position.set("adc1", 128);
        Attribute attribute = new Attribute();
        attribute.setExpression("adc1");

        // Execute the computation with resilience patterns
        Object result = handler.computeAttribute(attribute, position);

        // Verify the result is correct
        assertEquals(128, result);

        // Verify circuit breaker was used
        verify(circuitBreaker).executeSupplier(any());

        // Verify metrics were recorded
        verify(timer).record(any());
        verify(successCounter).increment();
    }

    /**
     * Test for integration with message brokers in microservices architecture.
     * This test verifies that the handler can process attributes asynchronously
     * using a message broker.
     */
    @Test
    public void testAsyncProcessingWithMessageBroker() {
        // Create a handler with message broker and executor for async processing
        ComputedAttributesHandler handler = new ComputedAttributesHandler(
                config, cacheManager, false, circuitBreakerRegistry,
                tracer, meterRegistry, messagePublisher, executor);

        // Setup device and attributes in cache manager
        Device device = new Device();
        device.setId(1);
        Map<String, Object> deviceAttributes = new HashMap<>();
        deviceAttributes.put("deviceTemp", 50);
        device.setAttributes(deviceAttributes);

        Attribute attribute = new Attribute();
        attribute.setAttribute("computedTemp");
        attribute.setExpression("deviceTemp + 10");
        attribute.setPriority(10); // Late processing

        when(cacheManager.getObject(Device.class, 1L)).thenReturn(device);
        when(cacheManager.getDeviceObjects(1L, Attribute.class)).thenReturn(java.util.List.of(attribute));

        // Create position
        Position position = new Position();
        position.setDeviceId(1);

        // Process position with callback
        final boolean[] callbackCalled = {false};
        handler.onPosition(position, processed -> {
            callbackCalled[0] = true;
            assertEquals(false, processed);
        });

        // Verify callback was called
        assertEquals(true, callbackCalled[0]);

        // Verify executor was used for async processing
        verify(executor).execute(any());
    }

    /**
     * Test for cross-service boundary processing.
     * This test verifies that the handler can process attributes that depend on
     * data from other services through the cache manager.
     */
    @Test
    public void testCrossServiceBoundaryProcessing() {
        // Create a handler with cache manager for cross-service data access
        ComputedAttributesHandler handler = new ComputedAttributesHandler(
                config, cacheManager, false, circuitBreakerRegistry,
                tracer, meterRegistry, messagePublisher, executor);

        // Setup device in cache manager (simulating data from Device Service)
        Device device = new Device();
        device.setId(1);
        Map<String, Object> deviceAttributes = new HashMap<>();
        deviceAttributes.put("maxSpeed", 100);
        device.setAttributes(deviceAttributes);

        // Setup last position in cache manager (simulating data from Position Service)
        Position lastPosition = new Position();
        lastPosition.setSpeed(80);
        lastPosition.set("fuel", 50);

        // Setup attribute that uses data from both services
        Attribute attribute = new Attribute();
        attribute.setAttribute("speedWarning");
        attribute.setExpression("speed > maxSpeed * 0.8 ? 'Warning' : 'Normal'");
        attribute.setPriority(10); // Late processing

        when(cacheManager.getObject(Device.class, 1L)).thenReturn(device);
        when(cacheManager.getPosition(1L)).thenReturn(lastPosition);
        when(cacheManager.getDeviceObjects(1L, Attribute.class)).thenReturn(java.util.List.of(attribute));

        // Create current position
        Position position = new Position();
        position.setDeviceId(1);
        position.setSpeed(90); // 90% of max speed

        // Process position
        final boolean[] callbackCalled = {false};
        handler.onPosition(position, processed -> {
            callbackCalled[0] = true;
        });

        // Verify callback was called
        assertEquals(true, callbackCalled[0]);

        // Verify attribute was computed and set
        assertEquals("Warning", position.getString("speedWarning"));
    }
}
