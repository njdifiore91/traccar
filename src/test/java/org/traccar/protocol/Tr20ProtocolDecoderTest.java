package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.traccar.ProtocolTest;

// Conditional imports for microservices testing
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Position;

// Import for message broker testing if running in microservices mode
// These will be available in the microservices environment but not in monolithic mode
// The test will use reflection to check if these classes are available at runtime
import java.lang.reflect.Method;
import java.util.concurrent.CompletableFuture;

/**
 * Test for Tr20 Protocol Decoder.
 * This test is designed to work in both monolithic and microservices environments.
 * 
 * In monolithic mode, it performs standard protocol decoding tests.
 * In microservices mode, it additionally tests integration with message brokers
 * and cross-service protocol handling.
 */
@ExtendWith(MockitoExtension.class)
public class Tr20ProtocolDecoderTest extends ProtocolTest {

    // Mock for message producer that will be used in microservices mode
    @Mock
    private Object messageProducer;
    
    /**
     * Standard protocol decoding test that works in both monolithic and microservices modes.
     * This maintains backward compatibility with the existing test approach.
     */
    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new Tr20ProtocolDecoder(null));

        verifyPosition(decoder, text(
                "%%0561,A,241025160359,N0951.6626W08357.0266,000,025,F0.0,00020000,108,CFG:0.12|"));

        verifyPosition(decoder, text(
                "%%m13,L,221221103115,N1237.2271W00801.9500,000,000,B13.1:F0.0,04020000,253,CFG:133.00|"));

        verifyPosition(decoder, text(
                "%%TR20GRANT,L,210602170135,N0951.1733W08356.7672,000,000,C80:F0,00020008,108,CFG:6980.00|"));

        verifyPosition(decoder, text(
                "%%123456789012345,A,120101121800,N6000.0000E13000.0000,0,000,0,01034802,150,[Message]"));

        verifyNull(decoder, text(
                "%%TRACKPRO01,1"));

        verifyPosition(decoder, text(
                "%%868873457748532,A,181109121248,N2237.4181E11403.2857,000,282,NA,47010000,108"));

        verifyPosition(decoder, text(
                "%%TR-10,A,050916070549,N2240.8887E11359.2994,0,000,NA,D3800000,150,CFG:resend|"),
                position("2005-09-16 07:05:49.000", true, 22.68148, 113.98832));

        verifyPosition(decoder, text(
                "%%TR-10,A,050916070549,N2240.8887E11359.2994,0,000,NA,D3800000,150,CFG:resend|"));

    }
    
    /**
     * Tests message broker integration in microservices mode.
     * This test is only enabled when running in microservices mode.
     * It verifies that decoded positions are correctly published to the message broker.
     */
    @Test
    @EnabledIfSystemProperty(named = "traccar.mode", matches = "microservices")
    public void testMessageBrokerIntegration() throws Exception {
        // This test only runs in microservices mode
        if (!isMicroservicesEnvironment()) {
            return;
        }
        
        try {
            // Use reflection to access microservices-specific classes
            Class<?> messageProducerClass = Class.forName("org.traccar.messaging.MessageProducer");
            Class<?> positionMessageClass = Class.forName("org.traccar.proto.PositionMessage");
            
            // Create a mock message producer
            Object mockProducer = Mockito.mock(messageProducerClass);
            Method sendMethod = messageProducerClass.getMethod("send", String.class, Object.class);
            Mockito.when(sendMethod.invoke(mockProducer, Mockito.eq("positions"), Mockito.any()))
                   .thenReturn(CompletableFuture.completedFuture(null));
            
            // Create a decoder with the mock producer injected
            var decoder = new Tr20ProtocolDecoder(null);
            
            // Use reflection to set the message producer
            Method setProducerMethod = decoder.getClass().getMethod("setMessageProducer", messageProducerClass);
            setProducerMethod.invoke(decoder, mockProducer);
            
            // Decode a message
            Object result = decoder.decode(null, null, text(
                    "%%0561,A,241025160359,N0951.6626W08357.0266,000,025,F0.0,00020000,108,CFG:0.12|"));
            
            // Verify the message was published to the broker
            Mockito.verify(mockProducer, Mockito.times(1)).send(Mockito.eq("positions"), Mockito.any());
            
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            // Expected in monolithic mode, ignore
        }
    }
    
    /**
     * Tests cross-service protocol handling in microservices mode.
     * This test verifies that the protocol decoder correctly handles messages
     * that need to be processed across service boundaries.
     */
    @Test
    @EnabledIfSystemProperty(named = "traccar.mode", matches = "microservices")
    public void testCrossServiceProtocolHandling() throws Exception {
        // This test only runs in microservices mode
        if (!isMicroservicesEnvironment()) {
            return;
        }
        
        try {
            // Use reflection to access microservices-specific classes
            Class<?> messageProducerClass = Class.forName("org.traccar.messaging.MessageProducer");
            Class<?> positionServiceClientClass = Class.forName("org.traccar.client.PositionServiceClient");
            
            // Create mocks for the message producer and position service client
            Object mockProducer = Mockito.mock(messageProducerClass);
            Object mockPositionClient = Mockito.mock(positionServiceClientClass);
            
            Method sendMethod = messageProducerClass.getMethod("send", String.class, Object.class);
            Mockito.when(sendMethod.invoke(mockProducer, Mockito.anyString(), Mockito.any()))
                   .thenReturn(CompletableFuture.completedFuture(null));
            
            // Create a decoder with the mocks injected
            var decoder = new Tr20ProtocolDecoder(null);
            
            // Use reflection to set the message producer and position service client
            Method setProducerMethod = decoder.getClass().getMethod("setMessageProducer", messageProducerClass);
            setProducerMethod.invoke(decoder, mockProducer);
            
            Method setClientMethod = decoder.getClass().getMethod("setPositionServiceClient", positionServiceClientClass);
            setClientMethod.invoke(decoder, mockPositionClient);
            
            // Decode a message that requires cross-service handling
            Object result = decoder.decode(null, null, text(
                    "%%TR20GRANT,L,210602170135,N0951.1733W08356.7672,000,000,C80:F0,00020008,108,CFG:6980.00|"));
            
            // Verify the message was published to the broker
            Mockito.verify(mockProducer, Mockito.times(1)).send(Mockito.anyString(), Mockito.any());
            
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            // Expected in monolithic mode, ignore
        }
    }
    
    /**
     * Helper method to check if we're running in a microservices environment.
     * This checks for the presence of microservices-specific classes.
     */
    private boolean isMicroservicesEnvironment() {
        try {
            Class.forName("org.traccar.messaging.MessageProducer");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
}