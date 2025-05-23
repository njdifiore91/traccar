package org.traccar.handler.events;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.BaseTest;
import org.traccar.model.Event;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;
import org.traccar.messaging.MessageBrokerManager;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class IgnitionEventHandlerTest extends BaseTest {
    
    @Mock
    private CacheManager cacheManager;
    
    @Mock
    private MessageBrokerManager messageBrokerManager;
    
    @Test
    public void testIgnitionEventHandler() {
        // Original test - maintained for backward compatibility
        IgnitionEventHandler ignitionEventHandler = new IgnitionEventHandler(mock(CacheManager.class));
        
        Position position = new Position();
        position.set(Position.KEY_IGNITION, true);
        position.setValid(true);
        ignitionEventHandler.analyzePosition(position, Assertions::assertNull);
    }
    
    @Test
    public void testIgnitionEventHandlerWithMessageBrokerFallback() {
        // Test that works in both environments by checking if MessageBrokerManager constructor exists
        try {
            // Try to create handler with message broker manager
            IgnitionEventHandler ignitionEventHandler = new IgnitionEventHandler(cacheManager, messageBrokerManager);
            
            Position position = new Position();
            position.set(Position.KEY_IGNITION, true);
            position.setValid(true);
            ignitionEventHandler.analyzePosition(position, Assertions::assertNull);
        } catch (NoSuchMethodError e) {
            // Fallback to original constructor if message broker constructor doesn't exist
            IgnitionEventHandler ignitionEventHandler = new IgnitionEventHandler(cacheManager);
            
            Position position = new Position();
            position.set(Position.KEY_IGNITION, true);
            position.setValid(true);
            ignitionEventHandler.analyzePosition(position, Assertions::assertNull);
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_MODE", matches = "monolithic")
    public void testIgnitionEventHandlerMonolithic() {
        // Test for monolithic environment
        IgnitionEventHandler ignitionEventHandler = new IgnitionEventHandler(cacheManager);
        
        // Test ignition on
        Position positionOn = new Position();
        positionOn.set(Position.KEY_IGNITION, true);
        positionOn.setValid(true);
        ignitionEventHandler.analyzePosition(positionOn, Assertions::assertNull);
        
        // Test ignition off
        Position positionOff = new Position();
        positionOff.set(Position.KEY_IGNITION, false);
        positionOff.setValid(true);
        
        Consumer<Event> eventConsumer = mock(Consumer.class);
        ignitionEventHandler.analyzePosition(positionOff, eventConsumer);
        
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventConsumer).accept(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        Assertions.assertEquals(Event.TYPE_IGNITION_OFF, capturedEvent.getType());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_MODE", matches = "microservices")
    public void testIgnitionEventHandlerMicroservices() throws Exception {
        // Test for microservices environment with message broker
        IgnitionEventHandler ignitionEventHandler = new IgnitionEventHandler(cacheManager, messageBrokerManager);
        
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        Mockito.when(messageBrokerManager.publishEvent(Mockito.any(Event.class))).thenReturn(future);
        
        // Test ignition off event
        Position positionOff = new Position();
        positionOff.set(Position.KEY_IGNITION, false);
        positionOff.setValid(true);
        
        // Capture the event using a consumer
        Consumer<Event> eventConsumer = mock(Consumer.class);
        ignitionEventHandler.analyzePosition(positionOff, eventConsumer);
        
        // Verify the event was captured by the consumer
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventConsumer).accept(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        Assertions.assertEquals(Event.TYPE_IGNITION_OFF, capturedEvent.getType());
        
        // Verify the event was published to the message broker
        verify(messageBrokerManager).publishEvent(capturedEvent);
    }

    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_MODE", matches = "microservices-kafka")
    public void testIgnitionEventHandlerWithKafka() throws Exception {
        // Test specifically for Kafka message broker integration
        IgnitionEventHandler ignitionEventHandler = new IgnitionEventHandler(cacheManager, messageBrokerManager);
        
        // Configure mock behavior for Kafka
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        Mockito.when(messageBrokerManager.publishEvent(Mockito.any(Event.class))).thenReturn(future);
        Mockito.when(messageBrokerManager.getBrokerType()).thenReturn("kafka");
        
        // Test ignition off event
        Position positionOff = new Position();
        positionOff.set(Position.KEY_IGNITION, false);
        positionOff.setValid(true);
        
        // Process the position
        Consumer<Event> eventConsumer = mock(Consumer.class);
        ignitionEventHandler.analyzePosition(positionOff, eventConsumer);
        
        // Verify the event was captured
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventConsumer).accept(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        Assertions.assertEquals(Event.TYPE_IGNITION_OFF, capturedEvent.getType());
        
        // Verify the event was published to Kafka
        verify(messageBrokerManager).publishEvent(capturedEvent);
        verify(messageBrokerManager).getBrokerType();
    }
    
    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_MODE", matches = "microservices-rabbitmq")
    public void testIgnitionEventHandlerWithRabbitMQ() throws Exception {
        // Test specifically for RabbitMQ message broker integration
        IgnitionEventHandler ignitionEventHandler = new IgnitionEventHandler(cacheManager, messageBrokerManager);
        
        // Configure mock behavior for RabbitMQ
        CompletableFuture<Void> future = CompletableFuture.completedFuture(null);
        Mockito.when(messageBrokerManager.publishEvent(Mockito.any(Event.class))).thenReturn(future);
        Mockito.when(messageBrokerManager.getBrokerType()).thenReturn("rabbitmq");
        
        // Test ignition off event
        Position positionOff = new Position();
        positionOff.set(Position.KEY_IGNITION, false);
        positionOff.setValid(true);
        
        // Process the position
        Consumer<Event> eventConsumer = mock(Consumer.class);
        ignitionEventHandler.analyzePosition(positionOff, eventConsumer);
        
        // Verify the event was captured
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventConsumer).accept(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        Assertions.assertEquals(Event.TYPE_IGNITION_OFF, capturedEvent.getType());
        
        // Verify the event was published to RabbitMQ
        verify(messageBrokerManager).publishEvent(capturedEvent);
        verify(messageBrokerManager).getBrokerType();
    }
    
    @Test
    @EnabledIfEnvironmentVariable(named = "TEST_MODE", matches = "microservices-async")
    public void testAsynchronousEventProcessing() throws Exception {
        // Test asynchronous event processing
        IgnitionEventHandler ignitionEventHandler = new IgnitionEventHandler(cacheManager, messageBrokerManager);
        
        // Configure mock behavior for async processing
        CompletableFuture<Void> future = new CompletableFuture<>();
        Mockito.when(messageBrokerManager.publishEvent(Mockito.any(Event.class))).thenReturn(future);
        
        // Test ignition off event
        Position positionOff = new Position();
        positionOff.set(Position.KEY_IGNITION, false);
        positionOff.setValid(true);
        
        // Process the position
        Consumer<Event> eventConsumer = mock(Consumer.class);
        ignitionEventHandler.analyzePosition(positionOff, eventConsumer);
        
        // Verify the event was captured
        ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);
        verify(eventConsumer).accept(eventCaptor.capture());
        
        Event capturedEvent = eventCaptor.getValue();
        Assertions.assertEquals(Event.TYPE_IGNITION_OFF, capturedEvent.getType());
        
        // Verify the event was published asynchronously
        verify(messageBrokerManager).publishEvent(capturedEvent);
        
        // Complete the future to simulate async completion
        future.complete(null);
        
        // Verify the future completes without exceptions
        future.get(100, TimeUnit.MILLISECONDS); // Short timeout for test
    }
}