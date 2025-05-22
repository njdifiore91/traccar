package org.traccar;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.traccar.helper.DataConverter;
import org.traccar.model.CellTower;
import org.traccar.model.Command;
import org.traccar.model.Position;
import org.traccar.model.WifiAccessPoint;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Base test class for protocol testing. Provides utility methods for creating and verifying
 * protocol messages in both monolithic and microservices architectures.
 */
public class ProtocolTest extends BaseTest {

    protected Position position(String time, boolean valid, double lat, double lon) throws ParseException {

        Position position = new Position();

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        position.setTime(dateFormat.parse(time));
        position.setValid(valid);
        position.setLatitude(lat);
        position.setLongitude(lon);

        return position;
    }

    private String concatenateStrings(String... strings) {
        StringBuilder builder = new StringBuilder();
        for (String s : strings) {
            builder.append(s);
        }
        return builder.toString();
    }

    protected ByteBuf concatenateBuffers(ByteBuf... buffers) {
        ByteBuf result = Unpooled.buffer();
        for (ByteBuf buf : buffers) {
            result.writeBytes(buf);
        }
        return result;
    }

    protected ByteBuf binary(String... data) {
        return Unpooled.wrappedBuffer(DataConverter.parseHex(concatenateStrings(data)));
    }

    protected String text(String... data) {
        return concatenateStrings(data);
    }

    protected ByteBuf buffer(String... data) {
        return Unpooled.copiedBuffer(concatenateStrings(data), StandardCharsets.ISO_8859_1);
    }

    protected DefaultFullHttpRequest request(String url) {
        return request(HttpMethod.GET, url);
    }

    protected DefaultFullHttpRequest request(HttpMethod method, String url) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, url);
    }

    protected DefaultFullHttpRequest request(HttpMethod method, String url, ByteBuf data) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, url, data);
    }

    protected DefaultFullHttpRequest request(HttpMethod method, String url, HttpHeaders headers) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, url, Unpooled.buffer(), headers, new DefaultHttpHeaders());
    }

    protected DefaultFullHttpRequest request(HttpMethod method, String url, HttpHeaders headers, ByteBuf data) {
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, method, url, data, headers, new DefaultHttpHeaders());
    }

    protected DefaultFullHttpResponse response(ByteBuf data) {
        return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, data);
    }

    protected void verifyNotNull(BaseProtocolDecoder decoder, Object object) throws Exception {
        assertNotNull(decoder.decode(null, null, object));
    }

    protected void verifyNull(Object object) {
        assertNull(object);
    }

    protected void verifyNull(BaseProtocolDecoder decoder, Object object) throws Exception {
        assertNull(decoder.decode(null, null, object));
    }

    protected void verifyAttribute(BaseProtocolDecoder decoder, Object object, String key, Object expected) throws Exception {
        Object decodedObject = decoder.decode(null, null, object);
        Position position;
        if (decodedObject instanceof Collection) {
            position = (Position) ((Collection<?>) decodedObject).iterator().next();
        } else {
            position = (Position) decodedObject;
        }
        switch (key) {
            case "speed" -> assertEquals(expected, position.getSpeed());
            case "course" -> assertEquals(expected, position.getCourse());
            case "altitude" -> assertEquals(expected, position.getAltitude());
            case "network" -> assertEquals(expected, position.getNetwork());

            default -> assertEquals(expected, position.getAttributes().get(key));
        }
    }

    protected void verifyAttributes(BaseProtocolDecoder decoder, Object object) throws Exception {
        verifyDecodedPosition(decoder.decode(null, null, object), false, true, null);
    }

    protected void verifyPosition(BaseProtocolDecoder decoder, Object object) throws Exception {
        verifyDecodedPosition(decoder.decode(null, null, object), true, false, null);
    }

    protected void verifyPosition(BaseProtocolDecoder decoder, Object object, Position position) throws Exception {
        verifyDecodedPosition(decoder.decode(null, null, object), true, false, position);
    }

    protected void verifyPositions(BaseProtocolDecoder decoder, Object object) throws Exception {
        verifyDecodedList(decoder.decode(null, null, object), true, null);
    }

    protected void verifyPositions(BaseProtocolDecoder decoder, boolean checkLocation, Object object) throws Exception {
        verifyDecodedList(decoder.decode(null, null, object), checkLocation, null);
    }

    protected void verifyPositions(BaseProtocolDecoder decoder, Object object, Position position) throws Exception {
        verifyDecodedList(decoder.decode(null, null, object), true, position);
    }

    /**
     * Verifies asynchronous position processing in microservices architecture.
     * This method allows testing protocol decoders that publish positions to a message broker
     * instead of returning them directly.
     *
     * @param decoder The protocol decoder to test
     * @param object The input object to decode
     * @param messagePublisher The mocked message publisher to verify
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyPositionPublished(BaseProtocolDecoder decoder, Object object, 
                                          Object messagePublisher) throws Exception {
        // Decode the message
        decoder.decode(null, null, object);
        
        // Verify that the message was published to the broker
        verify(messagePublisher, times(1)).publish(anyString(), any(Position.class));
    }

    /**
     * Verifies asynchronous position processing with specific topic and correlation ID.
     * This method is used for testing protocol decoders in a microservices architecture
     * where messages are published to specific topics with correlation IDs for tracing.
     *
     * @param decoder The protocol decoder to test
     * @param object The input object to decode
     * @param messagePublisher The mocked message publisher to verify
     * @param topic The expected topic name
     * @param correlationId The expected correlation ID
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyPositionPublished(BaseProtocolDecoder decoder, Object object, 
                                          Object messagePublisher, String topic, 
                                          String correlationId) throws Exception {
        // Decode the message
        decoder.decode(null, null, object);
        
        // Verify that the message was published to the specified topic with the correlation ID
        verify(messagePublisher, times(1)).publish(eq(topic), any(Position.class), eq(correlationId));
    }

    /**
     * Verifies that a protocol decoder correctly publishes multiple positions to a message broker.
     * Used for testing batch position processing in a microservices architecture.
     *
     * @param decoder The protocol decoder to test
     * @param object The input object to decode
     * @param messagePublisher The mocked message publisher to verify
     * @param expectedCount The expected number of positions to be published
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyPositionsPublished(BaseProtocolDecoder decoder, Object object, 
                                           Object messagePublisher, int expectedCount) throws Exception {
        // Decode the message
        decoder.decode(null, null, object);
        
        // Verify that the expected number of messages were published
        verify(messagePublisher, times(expectedCount)).publish(anyString(), any(Position.class));
    }

    /**
     * Verifies service client interactions in protocol handling.
     * This method is used to test protocol decoders that make calls to other services
     * in a microservices architecture.
     *
     * @param decoder The protocol decoder to test
     * @param object The input object to decode
     * @param serviceClient The mocked service client to verify
     * @param methodName The name of the method expected to be called
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyServiceClientInteraction(BaseProtocolDecoder decoder, Object object, 
                                                Object serviceClient, String methodName) throws Exception {
        // Decode the message
        decoder.decode(null, null, object);
        
        // Verify that the service client method was called
        verify(serviceClient, times(1)).getClass().getMethod(methodName, any()).invoke(serviceClient, any());
    }

    /**
     * Verifies that a protocol decoder correctly registers with service discovery.
     * Used for testing protocol service startup in a microservices architecture.
     *
     * @param serviceDiscovery The mocked service discovery client to verify
     * @param serviceName The expected service name to be registered
     */
    protected void verifyServiceDiscoveryRegistration(Object serviceDiscovery, String serviceName) {
        // Verify that the service was registered with service discovery
        verify(serviceDiscovery, times(1)).register(eq(serviceName), any());
    }

    /**
     * Verifies distributed tracing context propagation in protocol handling.
     * This method is used to test that protocol decoders correctly propagate tracing information
     * when processing messages in a microservices architecture.
     *
     * @param decoder The protocol decoder to test
     * @param object The input object to decode
     * @param tracingContext The mocked tracing context to verify
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyTracingContextPropagation(BaseProtocolDecoder decoder, Object object, 
                                                 Object tracingContext) throws Exception {
        // Decode the message
        decoder.decode(null, null, object);
        
        // Verify that the tracing context was propagated
        verify(tracingContext, times(1)).propagate(any());
    }

    /**
     * Sets up a mock for asynchronous position processing.
     * This method configures a CompletableFuture to be returned by the message publisher
     * to simulate asynchronous processing in a microservices architecture.
     *
     * @param messagePublisher The mocked message publisher to configure
     */
    protected void setupAsyncPositionProcessing(Object messagePublisher) {
        // Configure the mock to return a completed future when publish is called
        when(messagePublisher.getClass().getMethod("publish", String.class, Position.class)
                .invoke(messagePublisher, anyString(), any(Position.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private void verifyDecodedList(Object decodedObject, boolean checkLocation, Position expected) {

        assertNotNull(decodedObject, "list is null");
        assertInstanceOf(List.class, decodedObject, "not a list");
        assertFalse(((List<?>) decodedObject).isEmpty(), "list is empty");

        for (Object item : (List<?>) decodedObject) {
            verifyDecodedPosition(item, checkLocation, false, expected);
        }

    }

    private void verifyDecodedPosition(Object decodedObject, boolean checkLocation, boolean checkAttributes, Position expected) {

        assertNotNull(decodedObject, "position is null");
        assertInstanceOf(Position.class, decodedObject, "not a position");

        Position position = (Position) decodedObject;

        if (checkLocation) {

            if (expected != null) {

                if (expected.getFixTime() != null) {
                    DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                    assertEquals(dateFormat.format(expected.getFixTime()), dateFormat.format(position.getFixTime()), "time");
                }
                assertEquals(expected.getValid(), position.getValid(), "valid");
                assertEquals(expected.getLatitude(), position.getLatitude(), 0.00001, "latitude");
                assertEquals(expected.getLongitude(), position.getLongitude(), 0.00001, "longitude");

            } else {

                assertNotNull(position.getServerTime());
                assertNotNull(position.getFixTime());
                assertTrue(position.getFixTime().after(new Date(915148800000L)), "year > 1999");
                assertTrue(position.getFixTime().getTime() < System.currentTimeMillis() + 25 * 3600000, "time < +25 h");

                assertTrue(position.getLatitude() >= -90, "latitude >= -90");
                assertTrue(position.getLatitude() <= 90, "latitude <= 90");

                assertTrue(position.getLongitude() >= -180, "longitude >= -180");
                assertTrue(position.getLongitude() <= 180, "longitude <= 180");

            }

            assertTrue(position.getAltitude() >= -12262, "altitude >= -12262");
            assertTrue(position.getAltitude() <= 18000, "altitude <= 18000");

            assertTrue(position.getSpeed() >= 0, "speed >= 0");
            assertTrue(position.getSpeed() <= 869, "speed <= 869");

            assertTrue(position.getCourse() >= 0, "course >= 0");
            assertTrue(position.getCourse() <= 360, "course <= 360");

            assertNotNull(position.getProtocol(), "protocol is null");

            assertTrue(position.getDeviceId() > 0, "deviceId > 0");

        }

        Map<String, Object> attributes = position.getAttributes();

        if (checkAttributes) {
            assertFalse(attributes.isEmpty(), "no attributes");
        }

        if (attributes.containsKey(Position.KEY_INDEX)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_INDEX));
        }

        if (attributes.containsKey(Position.KEY_HDOP)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_HDOP));
        }

        if (attributes.containsKey(Position.KEY_VDOP)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_VDOP));
        }

        if (attributes.containsKey(Position.KEY_PDOP)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_PDOP));
        }

        if (attributes.containsKey(Position.KEY_SATELLITES)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_SATELLITES));
        }

        if (attributes.containsKey(Position.KEY_SATELLITES_VISIBLE)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_SATELLITES_VISIBLE));
        }

        if (attributes.containsKey(Position.KEY_RSSI)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_RSSI));
        }

        if (attributes.containsKey(Position.KEY_ODOMETER)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_ODOMETER));
        }

        if (attributes.containsKey(Position.KEY_RPM)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_RPM));
        }

        if (attributes.containsKey(Position.KEY_FUEL_LEVEL)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_FUEL_LEVEL));
        }

        if (attributes.containsKey(Position.KEY_FUEL_USED)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_FUEL_USED));
        }

        if (attributes.containsKey(Position.KEY_POWER)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_POWER));
        }

        if (attributes.containsKey(Position.KEY_BATTERY)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_BATTERY));
        }

        if (attributes.containsKey(Position.KEY_BATTERY_LEVEL)) {
            int batteryLevel = ((Number) attributes.get(Position.KEY_BATTERY_LEVEL)).intValue();
            assertTrue(batteryLevel <= 100 && batteryLevel >= 0);
        }

        if (attributes.containsKey(Position.KEY_CHARGE)) {
            assertInstanceOf(Boolean.class, attributes.get(Position.KEY_CHARGE));
        }

        if (attributes.containsKey(Position.KEY_IGNITION)) {
            assertInstanceOf(Boolean.class, attributes.get(Position.KEY_IGNITION));
        }

        if (attributes.containsKey(Position.KEY_MOTION)) {
            assertInstanceOf(Boolean.class, attributes.get(Position.KEY_MOTION));
        }

        if (attributes.containsKey(Position.KEY_ARCHIVE)) {
            assertInstanceOf(Boolean.class, attributes.get(Position.KEY_ARCHIVE));
        }

        if (attributes.containsKey(Position.KEY_DRIVER_UNIQUE_ID)) {
            assertInstanceOf(String.class, attributes.get(Position.KEY_DRIVER_UNIQUE_ID));
        }

        if (attributes.containsKey(Position.KEY_STEPS)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_STEPS));
        }

        if (attributes.containsKey(Position.KEY_ROAMING)) {
            assertInstanceOf(Boolean.class, attributes.get(Position.KEY_ROAMING));
        }

        if (attributes.containsKey(Position.KEY_HOURS)) {
            assertInstanceOf(Number.class, attributes.get(Position.KEY_HOURS));
        }

        if (attributes.containsKey(Position.KEY_RESULT)) {
            assertInstanceOf(String.class, attributes.get(Position.KEY_RESULT));
        }

        // Verify correlation ID for distributed tracing if present
        if (attributes.containsKey("correlationId")) {
            assertInstanceOf(String.class, attributes.get("correlationId"));
        }

        if (position.getNetwork() != null) {
            if (position.getNetwork().getCellTowers() != null) {
                for (CellTower cellTower : position.getNetwork().getCellTowers()) {
                    checkInteger(cellTower.getMobileCountryCode(), 0, 999);
                    checkInteger(cellTower.getMobileNetworkCode(), 0, 999);
                    checkInteger(cellTower.getLocationAreaCode(), 1, 65535);
                    checkInteger(cellTower.getCellId(), 0, 268435455);
                }
            }

            if (position.getNetwork().getWifiAccessPoints() != null) {
                for (WifiAccessPoint wifiAccessPoint : position.getNetwork().getWifiAccessPoints()) {
                    assertTrue(wifiAccessPoint.getMacAddress().matches("((\\p{XDigit}{2}):){5}(\\p{XDigit}{2})"));
                }
            }
        }

    }

    private void checkInteger(Object value, int min, int max) {
        assertNotNull(value, "value is null");
        assertTrue(value instanceof Integer || value instanceof Long, "not int or long");
        long number = ((Number) value).longValue();
        assertTrue(number >= min, "value too low");
        assertTrue(number <= max, "value too high");
    }

    protected void verifyCommand(
            BaseProtocolEncoder encoder, Command command, ByteBuf expected) {
        verifyFrame(expected, encoder.encodeCommand(command));
    }

    protected void verifyFrame(ByteBuf expected, Object object) {
        assertNotNull(object, "buffer is null");
        assertInstanceOf(ByteBuf.class, object, "not a buffer");
        assertEquals(ByteBufUtil.hexDump(expected), ByteBufUtil.hexDump((ByteBuf) object));
    }

    /**
     * Verifies that a protocol encoder correctly publishes a command to a message broker.
     * Used for testing command handling in a microservices architecture.
     *
     * @param encoder The protocol encoder to test
     * @param command The command to encode
     * @param messagePublisher The mocked message publisher to verify
     */
    protected void verifyCommandPublished(
            BaseProtocolEncoder encoder, Command command, Object messagePublisher) {
        // Encode the command
        encoder.encodeCommand(command);
        
        // Verify that the command was published to the broker
        verify(messagePublisher, times(1)).publish(anyString(), any(Command.class));
    }

    /**
     * Verifies that a protocol encoder correctly publishes a command to a specific topic
     * with a correlation ID for distributed tracing.
     *
     * @param encoder The protocol encoder to test
     * @param command The command to encode
     * @param messagePublisher The mocked message publisher to verify
     * @param topic The expected topic name
     * @param correlationId The expected correlation ID
     */
    protected void verifyCommandPublished(
            BaseProtocolEncoder encoder, Command command, Object messagePublisher, 
            String topic, String correlationId) {
        // Encode the command
        encoder.encodeCommand(command);
        
        // Verify that the command was published to the specified topic with the correlation ID
        verify(messagePublisher, times(1)).publish(eq(topic), any(Command.class), eq(correlationId));
    }

    /**
     * Creates a mock message consumer for testing asynchronous message handling.
     * This method sets up a consumer that can be used to verify message processing
     * in a microservices architecture.
     *
     * @param <T> The type of message to consume
     * @param messageType The class of the message type
     * @return A mocked message consumer
     */
    protected <T> Consumer<T> createMockMessageConsumer(Class<T> messageType) {
        return mock(Consumer.class);
    }

    /**
     * Verifies that a message consumer correctly processes a message.
     * Used for testing message handling in a microservices architecture.
     *
     * @param <T> The type of message being consumed
     * @param consumer The mocked message consumer to verify
     * @param message The message that should have been processed
     */
    protected <T> void verifyMessageConsumed(Consumer<T> consumer, T message) {
        verify(consumer, times(1)).accept(eq(message));
    }

    /**
     * Simulates a message being received from a message broker.
     * This method can be used to test protocol handlers that consume messages
     * from a message broker in a microservices architecture.
     *
     * @param <T> The type of message being received
     * @param messageHandler The message handler to test
     * @param message The message to simulate receiving
     * @param topic The topic the message was received on
     */
    protected <T> void simulateMessageReceived(Object messageHandler, T message, String topic) {
        try {
            messageHandler.getClass().getMethod("onMessage", Object.class, String.class)
                    .invoke(messageHandler, message, topic);
        } catch (Exception e) {
            throw new RuntimeException("Failed to simulate message received", e);
        }
    }
}