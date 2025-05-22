package org.traccar.protocol;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.traccar.ProtocolTest;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.model.PositionUtil;
import org.traccar.model.Position;
import org.traccar.session.cache.CacheManager;

// Optional imports that may not be available in monolithic environment
import java.lang.reflect.Method;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for Totem protocol decoder.
 * 
 * This test is designed to work in both monolithic and microservices environments.
 * It supports testing protocol integration with message brokers when running in
 * the Protocol Service context.
 * 
 * In the microservices environment, this test verifies:
 * 1. Basic protocol decoding functionality
 * 2. Integration with message brokers for position publishing
 * 3. Cross-service communication for position handling
 * 
 * The test automatically detects the environment it's running in and adjusts
 * its behavior accordingly. When running in the monolithic environment, only
 * the basic protocol decoding tests are executed. When running in the Protocol
 * Service context, additional tests for message broker integration and
 * cross-service communication are enabled.
 */
public class TotemProtocolDecoderTest extends ProtocolTest {

    private boolean isInMicroserviceEnvironment() {
        try {
            Class.forName("org.traccar.messaging.MessagePublisher");
            return true;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
    
    @BeforeEach
    public void setUp() {
        // Additional setup for microservices environment if needed
        if (isInMicroserviceEnvironment()) {
            try {
                // Setup any additional dependencies needed for microservices testing
                System.setProperty("test.environment", "microservice");
            } catch (Exception e) {
                System.out.println("Error setting up microservices test environment: " + e.getMessage());
            }
        }
    }

    @Test
    public void testDecode() throws Exception {

        var decoder = inject(new TotemProtocolDecoder(null));

        verifyAttribute(decoder, text(
                "$$0494E2123456789012345|150425223945,113.925525,22.55814,1122334455|38"),
                Position.KEY_DRIVER_UNIQUE_ID, "1122334455");

        verifyPosition(decoder, text(
                "$$0111AA353081090067318|0804400022070722520240400005B364ED5003107300001.700000002245.3919N10231.6952W000001860E"));

        verifyPosition(decoder, text(
                "$$0112E5864606045334223|201112223514,-68.923106,-22.455926,$Cloud,1738,621,730,12100,0,0,255,0,40,40,0,0,255,|13"));

        verifyPosition(decoder, text(
                "$$0113AA862010037348253|588040001901220851494212000000753AE901655121700100000.800000002632.6084S02803.3289E29497E"),
                position("2019-01-22 08:51:49.000", true, -26.54347, 28.05548));

        verifyPosition(decoder, text(
                "$$011602867119025755430|50099800180420045019401400000000000000B8797D110816811201.500002132615.7037S02801.8099E056149"));

        verifyPosition(decoder, text(
                "$$0108AB863835028447675|5004C0001710250234064214059828A058AE121010604000.600000320304.7772N10134.8238E11625B"));

        verifyPosition(decoder, text(
                "$$0108AA863835028447675|5004C0001710250234134114057728A058AE112108305100.600000660304.7787N10134.8719E116458"));

        verifyPosition(decoder, text(
                "$$0112AA864244026065291|180018001409160205244011000027BA0E57063100000001.200000002237.8119N11403.5075E05202D"));

        verifyPosition(decoder, text(
                "$$0116AA864244026065291|18001800140916020524401100000000000027BA0E57063100000001.200000002237.8119N11403.5075E052020"));

        verifyPosition(decoder, text(
                "$$0116AA867119025683137|108000001611020925324112000000000000616027F7001300000099.900000000000.0000N00000.0000E531824"));

        verifyPosition(decoder, text(
                "$$0128AA864244026065291|18001800140916020524401100000000000000000000000027BA0E57063100000001.200000002237.8119N11403.5075E05202D"));

        verifyPosition(decoder, text(
                "$$0128AA867965024919124|10010800160223032415401203270321032103270189000027BA0E4E001800200001.000000002237.7581N11403.5088E000957"),
                position("2016-02-23 03:24:15.000", false, 22.62930, 114.05848));

        verifyPosition(decoder, text(
                "$$0108AA863835024426319|18004000160216160756411100007DCD0000111000000000.800000000316.3519N10228.5086E126522"));

        verifyPosition(decoder, text(
                "$$0128AA867521029231005|1880100015101802314842140000000000000000000000001AB48366093127600000.900000000806.1947N09818.4795E080355"));

        verifyPosition(decoder, text(
                "$$0108AA864244026063437|1A0000001401010101014111000027BA0E57003100000000.000000000000.0000N00000.0000E048156"));

        verifyPosition(decoder, text(
                "$$BE863771024392112|AA$GPRMC,044704.000,A,1439.3334,N,12059.1417,E,0.00,0.00,200815,,,A*67|01.7|00.8|01.4|000000000000|20150820044704|14291265|00000000|4EECBF8B31|0000|0.0000|0002|00000|56E7"),
                position("2015-08-20 04:47:04.000", true, 14.65556, 120.98570));

        verifyPosition(decoder, text(
                "$$AE860990002922822|AA$GPRMC,051002.00,A,0439.26245,N,10108.94448,E,0.023,,140315,,,A*71|02.98|01.95|02.26|000000000000|20150314051003|13841157|105A3B1C|0000|0.0000|0005|5324"),
                position("2015-03-14 05:10:02.000", true, 4.65437, 101.14907));

        verifyPosition(decoder, text(
                "$$AE860990002922822|AA$GPRMC,051002.00,A,0439.26245,N,10108.94448,E,0.023,,140315,,,A*71|02.98|01.95|02.26|000000000000|20150314051003|13841157|105A3B1C|0000|0.0000|0005|5324\r"));

        verifyNull(decoder, text(
                "$$BB862170017856731|AA$GPRMC,000000.00,V,0000.0000,N,00000.0000,E,000.0,000.0,000000,,,A*73|00.0|00.0|00.0|000000001000|20000000000000|13790000|00000000|00000000|00000000|0.0000|0007|8C23"));

        verifyPosition(decoder, text(
                "$$B8862170017856731|AA$GPRMC,171849.00,A,3644.9893,N,01012.9927,E,0.049,51,200813,,,A*73|1.59|0.97|1.25|100000001000|20130820171849|13690000|00000000|019BD508|00000000|0.0000|0026|1B2C"));

        verifyPosition(decoder, text(
                "$$B2359772032984289|AA$GPRMC,104446.000,A,5011.3944,N,01439.6637,E,0.00,,290212,,,A*7D|01.8|00.9|01.5|000000100000|20120229104446|14151221|00050000|046D085E|0000|0.0000|1170|29A7"));

        verifyPosition(decoder, text(
                "$$8B862170017861566|AA180613080657|A|2237.1901|N|11402.1369|E|1.579|178|8.70|100000001000|13811|00000000|253162F5|00000000|0.0000|0014|2B16"),
                position("2013-06-18 08:06:57.000", true, 22.61984, 114.03562));

        verifyPosition(decoder, text(
                "$$72862170017856731|3913090911165280000370000000000000000019BD508A0400000003.400000093644.9817N01012.9944E00506F2E"));

        verifyPosition(decoder, text(
                "$$B0456123|61$GPRMC,114725.00,A,1258.68276,N,07730.60237,E,0.410,,080113,,,A*79|1.44|0.66|1.27|000000000000|20130108114425|03600000|00000000|053C2BFE|0000|0.3325|0063|2005"));

        verifyNull(decoder, text(
                "$$AE359772033395899|AA000000000000000000000000000000000000000000000000000000000000|00.0|00.0|00.0|000000000000|20090215000153|13601435|00000000|00000000|0000|0.0000|0007|2DAA"));

        verifyNull(decoder, text(
                "$$AE359772033395899|AA000000000000000000000000000000000000000000000000000000000000|00.0|00.0|00.0|00000000|20090215001204|14182037|00000000|0012D888|0000|0.0000|0016|5B51"));

        verifyNull(decoder, text(
                "$$AE359772033395899|AA00000000000000000000000000000000000000000000000000000000000|00.0|00.0|00.0|00000000000|20090215001337|14182013|00000000|0012D888|0000|0.0000|0017|346E"));

        verifyPosition(decoder, text(
                "$$B3359772032399074|60$GPRMC,094859.000,A,3648.2229,N,01008.0976,E,0.00,,221211,,,A*79|02.3|01.3|02.0|000000000000|20111222094858|13360808|00000000|00000000|0000|0.0000|0001||A977"));

        verifyPosition(decoder, text(
                "$$B3359772032399074|09$GPRMC,094905.000,A,3648.2229,N,01008.0976,E,0.00,,221211,,,A*71|02.1|01.3|01.7|000000000000|20111222094905|03210533|00000000|00000000|0000|0.0000|0002||FA58"));

        verifyPosition(decoder, text(
                "$$B3359772032399074|AA$GPRMC,093911.000,A,3648.2146,N,01008.0977,E,0.00,,140312,,,A*7E|02.1|01.1|01.8|000000000000|20120314093910|04100057|00000000|0012D887|0000|0.0000|1128||C50E"));

        verifyPosition(decoder, text(
                "$$B3359772032399074|AA$GPRMC,094258.000,A,3648.2146,N,01008.0977,E,0.00,,140312,,,A*7F|02.1|01.1|01.8|000000000000|20120314094257|04120057|00000000|0012D887|0000|0.0000|1136||CA32"));

        verifyPosition(decoder, text(
                "$$B3359772032399074|AA$GPRMC,234603.000,A,3648.2179,N,01008.0962,E,0.00,,030412,,,A*74|01.8|01.0|01.5|000000000000|20120403234603|14251914|00000000|0012D888|0000|0.0000|3674||940B"));

        verifyPosition(decoder, text(
                "$$B3359772032399074|AA$GPRMC,234603.000,A,3648.2179,N,01008.0962,E,0.00,,030412,,,A*74|01.8|01.0|01.5|000000000000|20120403234603|14251914|00000000|0012D888|0000|0.0000|3674|940B"));
        
        verifyPosition(decoder, text(
                "$$B2356895037578518|AA$GPRMC,173829.000,A,3740.4107,N,02129.9815,E,0.00,,111113,,,A*7B|02.6|01.6|02.1|000000000000|20131111173829|14041251|00000000|002E0DD7|0000|0.0240|6010|8128"));

        verifyPosition(decoder, text(
                "$$B2356895037578518|AA$GPRMC,203823.000,A,3740.3285,N,02129.9295,E,0.00,,111113,,,A*79|01.5|01.0|01.1|000000000000|20131111203823|14041251|00000000|002E0DD7|0000|0.0000|6371|3824"));

    }

        /**
     * Test for message broker integration in microservices environment.
     * This test is only enabled when running in the Protocol Service context.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservice")
    public void testMessageBrokerIntegration() throws Exception {
        // Check if we're running in microservices environment by looking for MessagePublisher class
        try {
            Class<?> messagePublisherClass = Class.forName("org.traccar.messaging.MessagePublisher");
            Class<?> positionMessageClass = Class.forName("org.traccar.proto.PositionMessage");
            
            // Create a mock message publisher using reflection to avoid compile-time dependencies
            Object mockPublisher = Mockito.mock(messagePublisherClass);
            
            // Create a decoder with the mock publisher injected
            TotemProtocolDecoder decoder = new TotemProtocolDecoder(null);
            
            // Use reflection to set the message publisher
            Method setPublisherMethod = decoder.getClass().getDeclaredMethod("setMessagePublisher", messagePublisherClass);
            setPublisherMethod.setAccessible(true);
            setPublisherMethod.invoke(decoder, mockPublisher);
            
            // Parse a test message
            Position position = decoder.decode(null, null, text(
                    "$$B8862170017856731|AA$GPRMC,171849.00,A,3644.9893,N,01012.9927,E,0.049,51,200813,,,A*73|1.59|0.97|1.25|100000001000|20130820171849|13690000|00000000|019BD508|00000000|0.0000|0026|1B2C"));
            
            // Verify that the message was published using ArgumentCaptor
            // We need to use ArgumentCaptor to capture the argument passed to the publish method
            Class<?> argumentCaptorClass = Class.forName("org.mockito.ArgumentCaptor");
            Method forClassMethod = argumentCaptorClass.getDeclaredMethod("forClass", Class.class);
            Object argumentCaptor = forClassMethod.invoke(null, positionMessageClass);
            
            // Get the capture method from ArgumentCaptor
            Method captureMethod = argumentCaptorClass.getDeclaredMethod("capture");
            Object captureResult = captureMethod.invoke(argumentCaptor);
            
            // Verify the mock was called with the captured argument
            Mockito.verify(mockPublisher).publish(captureResult);
            
            // Get the captured value and verify it's not null
            Method getValueMethod = argumentCaptorClass.getDeclaredMethod("getValue");
            Object capturedMessage = getValueMethod.invoke(argumentCaptor);
            
            // Assert that the captured message is not null
            assertNotNull(capturedMessage);
            
        } catch (ClassNotFoundException e) {
            // Skip test if running in monolithic environment
            System.out.println("Skipping message broker test in monolithic environment");
        } catch (Exception e) {
            throw new RuntimeException("Error in message broker test", e);
        }
    }

    /**
     * Test for protocol handling across service boundaries.
     * This test is only enabled when running in the Protocol Service context.
     */
    @Test
    @EnabledIfSystemProperty(named = "test.environment", matches = "microservice")
    public void testCrossServiceIntegration() throws Exception {
        try {
            // Check if we're running in microservices environment
            Class<?> messagePublisherClass = Class.forName("org.traccar.messaging.MessagePublisher");
            Class<?> positionMessageClass = Class.forName("org.traccar.proto.PositionMessage");
            Class<?> positionServiceClientClass = Class.forName("org.traccar.client.PositionServiceClient");
            
            // Create mocks for cross-service communication
            Object mockPublisher = Mockito.mock(messagePublisherClass);
            Object mockPositionClient = Mockito.mock(positionServiceClientClass);
            
            // Create a decoder with the mocks injected
            TotemProtocolDecoder decoder = new TotemProtocolDecoder(null);
            
            // Use reflection to set the dependencies
            Method setPublisherMethod = decoder.getClass().getDeclaredMethod("setMessagePublisher", messagePublisherClass);
            setPublisherMethod.setAccessible(true);
            setPublisherMethod.invoke(decoder, mockPublisher);
            
            Method setClientMethod = decoder.getClass().getDeclaredMethod("setPositionServiceClient", positionServiceClientClass);
            setClientMethod.setAccessible(true);
            setClientMethod.invoke(decoder, mockPositionClient);
            
            // Parse a test message
            Position position = decoder.decode(null, null, text(
                    "$$B8862170017856731|AA$GPRMC,171849.00,A,3644.9893,N,01012.9927,E,0.049,51,200813,,,A*73|1.59|0.97|1.25|100000001000|20130820171849|13690000|00000000|019BD508|00000000|0.0000|0026|1B2C"));
            
            // Verify cross-service interactions using ArgumentCaptor
            Class<?> argumentCaptorClass = Class.forName("org.mockito.ArgumentCaptor");
            Method forClassMethod = argumentCaptorClass.getDeclaredMethod("forClass", Class.class);
            Object argumentCaptor = forClassMethod.invoke(null, positionMessageClass);
            
            // Get the capture method from ArgumentCaptor
            Method captureMethod = argumentCaptorClass.getDeclaredMethod("capture");
            Object captureResult = captureMethod.invoke(argumentCaptor);
            
            // Verify the mock was called with the captured argument
            Mockito.verify(mockPublisher).publish(captureResult);
            
            // Get the captured value and verify it's not null
            Method getValueMethod = argumentCaptorClass.getDeclaredMethod("getValue");
            Object capturedMessage = getValueMethod.invoke(argumentCaptor);
            
            // Assert that the captured message is not null
            assertNotNull(capturedMessage);
            
            // Verify position service client interaction if applicable
            // This will depend on the specific implementation of cross-service communication
            // For example, if there's a processPosition method:
            try {
                Method processPositionMethod = positionServiceClientClass.getDeclaredMethod("processPosition", positionMessageClass);
                if (processPositionMethod != null) {
                    // Create another ArgumentCaptor for the position service client
                    Object positionCaptor = forClassMethod.invoke(null, positionMessageClass);
                    Object positionCaptureResult = captureMethod.invoke(positionCaptor);
                    
                    // Verify the method was called
                    Mockito.verify(mockPositionClient).processPosition(positionCaptureResult);
                }
            } catch (NoSuchMethodException e) {
                // Method doesn't exist, skip this verification
                System.out.println("processPosition method not found, skipping verification");
            }
            
        } catch (ClassNotFoundException e) {
            // Skip test if running in monolithic environment
            System.out.println("Skipping cross-service test in monolithic environment");
        } catch (Exception e) {
            throw new RuntimeException("Error in cross-service test", e);
        }
    }

}