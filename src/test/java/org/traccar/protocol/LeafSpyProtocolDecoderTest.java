package org.traccar.protocol;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mockito;
import org.traccar.ProtocolTest;
import org.traccar.model.Position;

/**
 * Test for LeafSpy protocol decoder.
 * 
 * This test is designed to work in both monolithic and microservices environments.
 * It supports gradual migration to the Protocol Service architecture.
 */
public class LeafSpyProtocolDecoderTest extends ProtocolTest {

    /**
     * Basic decode test for LeafSpy protocol.
     * This test works in both monolithic and microservices environments.
     */
    @Test
    public void testDecode() throws Exception {
        var decoder = inject(new LeafSpyProtocolDecoder(null));

        verifyNull(decoder, request(
                "/?Lat=60.0&Long=30.0"));

        verifyPosition(decoder, request(
                "/?user=driver&pass=123456&DevBat=80&Gids=200&Lat=60.0&Long=30.0&Elv=5&Seq=50&Trip=1&Odo=10000&SOC=99.99&AHr=55.00&BatTemp=15.2&Amb=12.0&Wpr=12&PlugState=0&ChrgMode=0&ChrgPwr=0&VIN=ZE0-000000&PwrSw=1&Tunits=C&RPM=1000"));
    }
    
    /**
     * Test for message broker integration.
     * This test verifies that the protocol decoder correctly publishes position data to the message broker.
     * Only runs when the 'test.broker' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.broker", matches = "true")
    public void testMessageBrokerIntegration() throws Exception {
        // Create a mock message publisher that would be injected in a microservices environment
        var messagePublisher = Mockito.mock(MessagePublisher.class);
        
        // Create decoder with message publisher
        var decoder = new LeafSpyProtocolDecoder(null);
        decoder.setMessagePublisher(messagePublisher);
        inject(decoder);
        
        // Process a valid message
        Position position = decoder.decode(null, null, request(
                "/?user=driver&pass=123456&DevBat=80&Gids=200&Lat=60.0&Long=30.0&Elv=5&Seq=50&Trip=1&Odo=10000&SOC=99.99&AHr=55.00&BatTemp=15.2&Amb=12.0&Wpr=12&PlugState=0&ChrgMode=0&ChrgPwr=0&VIN=ZE0-000000&PwrSw=1&Tunits=C&RPM=1000"));
        
        // Verify position was created
        verifyPosition(position);
        
        // Verify message was published to broker
        Mockito.verify(messagePublisher).publishPosition(Mockito.eq(position));
    }
    
    /**
     * Test for cross-service boundary handling.
     * This test verifies that the protocol decoder correctly handles data across service boundaries.
     * Only runs when the 'test.microservices' system property is set to 'true'.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.microservices", matches = "true")
    public void testCrossServiceBoundary() throws Exception {
        // Create mocks for cross-service dependencies
        var deviceService = Mockito.mock(DeviceService.class);
        var positionService = Mockito.mock(PositionService.class);
        
        // Setup device service mock to return a device ID for the test VIN
        Mockito.when(deviceService.getDeviceByUniqueId(Mockito.eq("ZE0-000000"))).thenReturn(1L);
        
        // Create decoder with cross-service dependencies
        var decoder = new LeafSpyProtocolDecoder(null);
        decoder.setDeviceService(deviceService);
        decoder.setPositionService(positionService);
        inject(decoder);
        
        // Process a valid message
        Position position = decoder.decode(null, null, request(
                "/?user=driver&pass=123456&DevBat=80&Gids=200&Lat=60.0&Long=30.0&Elv=5&Seq=50&Trip=1&Odo=10000&SOC=99.99&AHr=55.00&BatTemp=15.2&Amb=12.0&Wpr=12&PlugState=0&ChrgMode=0&ChrgPwr=0&VIN=ZE0-000000&PwrSw=1&Tunits=C&RPM=1000"));
        
        // Verify position was created
        verifyPosition(position);
        
        // Verify cross-service interactions
        Mockito.verify(deviceService).getDeviceByUniqueId(Mockito.eq("ZE0-000000"));
        Mockito.verify(positionService).processPosition(Mockito.eq(position));
    }
    
    /**
     * Interface for message publishing in microservices environment.
     * This would be implemented by the actual message broker integration in the Protocol Service.
     */
    public interface MessagePublisher {
        void publishPosition(Position position);
    }
    
    /**
     * Interface for device service in microservices environment.
     * This would be implemented by the client that communicates with the Device Service.
     */
    public interface DeviceService {
        Long getDeviceByUniqueId(String uniqueId);
    }
    
    /**
     * Interface for position service in microservices environment.
     * This would be implemented by the client that communicates with the Position Service.
     */
    public interface PositionService {
        void processPosition(Position position);
    }
}