package org.traccar;

import com.google.protobuf.InvalidProtocolBufferException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.Assertions;
import org.traccar.config.Config;
import org.traccar.messaging.MessageProducer;
import org.traccar.model.CellTower;
import org.traccar.model.Command;
import org.traccar.model.Network;
import org.traccar.model.Position;
import org.traccar.model.WifiAccessPoint;
import org.traccar.proto.PositionOuterClass;
import org.traccar.session.DeviceSession;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Protocol test utilities
 * Supports testing protocol implementations in the microservices architecture
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

    /**
     * Creates a position object with the specified parameters
     * 
     * @param time Time in ISO-8601 format
     * @param valid Position validity flag
     * @param lat Latitude
     * @param lon Longitude
     * @return Position object
     */
    protected Position createPosition(String time, boolean valid, double lat, double lon) {
        Position position = new Position();
        try {
            position.setTime(Date.from(Instant.parse(time)));
        } catch (Exception e) {
            // Try alternative format
            try {
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
                position.setTime(dateFormat.parse(time));
            } catch (ParseException ex) {
                throw new RuntimeException("Invalid time format", ex);
            }
        }
        position.setValid(valid);
        position.setLatitude(lat);
        position.setLongitude(lon);
        return position;
    }

    /**
     * Creates a position object with the specified parameters and device ID
     * 
     * @param deviceId Device identifier
     * @param time Time in ISO-8601 format
     * @param valid Position validity flag
     * @param lat Latitude
     * @param lon Longitude
     * @return Position object
     */
    protected Position createPosition(long deviceId, String time, boolean valid, double lat, double lon) {
        Position position = createPosition(time, valid, lat, lon);
        position.setDeviceId(deviceId);
        return position;
    }

    /**
     * Creates a Protocol Buffer position message from a Position object
     * 
     * @param position Position object
     * @return Protocol Buffer position message
     */
    protected PositionOuterClass.Position createProtobufPosition(Position position) {
        PositionOuterClass.Position.Builder builder = PositionOuterClass.Position.newBuilder()
                .setDeviceId(position.getDeviceId())
                .setTimestamp(position.getTime().getTime())
                .setValid(position.getValid())
                .setLatitude(position.getLatitude())
                .setLongitude(position.getLongitude());

        if (position.getAltitude() != 0) {
            builder.setAltitude(position.getAltitude());
        }
        if (position.getSpeed() != 0) {
            builder.setSpeed(position.getSpeed());
        }
        if (position.getCourse() != 0) {
            builder.setCourse(position.getCourse());
        }

        // Add attributes if present
        for (Map.Entry<String, Object> entry : position.getAttributes().entrySet()) {
            if (entry.getValue() instanceof Boolean) {
                builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
            } else if (entry.getValue() instanceof Integer) {
                builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
            } else if (entry.getValue() instanceof Long) {
                builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
            } else if (entry.getValue() instanceof Double) {
                builder.putAttributes(entry.getKey(), String.valueOf(entry.getValue()));
            } else if (entry.getValue() instanceof String) {
                builder.putAttributes(entry.getKey(), (String) entry.getValue());
            }
        }

        return builder.build();
    }

    /**
     * Converts a Protocol Buffer position message to a Position object
     * 
     * @param protobufPosition Protocol Buffer position message
     * @return Position object
     */
    protected Position convertFromProtobuf(PositionOuterClass.Position protobufPosition) {
        Position position = new Position();
        position.setDeviceId(protobufPosition.getDeviceId());
        position.setTime(new Date(protobufPosition.getTimestamp()));
        position.setValid(protobufPosition.getValid());
        position.setLatitude(protobufPosition.getLatitude());
        position.setLongitude(protobufPosition.getLongitude());
        
        if (protobufPosition.hasAltitude()) {
            position.setAltitude(protobufPosition.getAltitude());
        }
        if (protobufPosition.hasSpeed()) {
            position.setSpeed(protobufPosition.getSpeed());
        }
        if (protobufPosition.hasCourse()) {
            position.setCourse(protobufPosition.getCourse());
        }
        
        // Convert attributes
        for (Map.Entry<String, String> entry : protobufPosition.getAttributesMap().entrySet()) {
            String value = entry.getValue();
            // Try to convert to appropriate type
            try {
                if (value.equals("true") || value.equals("false")) {
                    position.set(entry.getKey(), Boolean.parseBoolean(value));
                } else if (value.contains(".")) {
                    position.set(entry.getKey(), Double.parseDouble(value));
                } else {
                    position.set(entry.getKey(), Long.parseLong(value));
                }
            } catch (NumberFormatException e) {
                position.set(entry.getKey(), value);
            }
        }
        
        return position;
    }

    /**
     * Creates a network object with cell towers and WiFi access points
     * 
     * @param cellTowers List of cell towers
     * @param wifiAccessPoints List of WiFi access points
     * @return Network object
     */
    protected Network createNetwork(CellTower... cellTowers) {
        Network network = new Network();
        for (CellTower cellTower : cellTowers) {
            network.addCellTower(cellTower);
        }
        return network;
    }

    /**
     * Creates a network object with WiFi access points
     * 
     * @param wifiAccessPoints List of WiFi access points
     * @return Network object
     */
    protected Network createWifiNetwork(WifiAccessPoint... wifiAccessPoints) {
        Network network = new Network();
        for (WifiAccessPoint wifiAccessPoint : wifiAccessPoints) {
            network.addWifiAccessPoint(wifiAccessPoint);
        }
        return network;
    }

    /**
     * Verifies that a decoder correctly processes a binary message
     * 
     * @param decoder Protocol decoder
     * @param binary Binary message data
     * @param expected Expected position object
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyDecodeBinary(BaseProtocolDecoder decoder, byte[] binary, Position expected) throws Exception {
        verifyDecodeBinary(decoder, binary, expected, null);
    }

    /**
     * Verifies that a decoder correctly processes a binary message and publishes to message broker
     * 
     * @param decoder Protocol decoder
     * @param binary Binary message data
     * @param expected Expected position object
     * @param messageProducer Mock message producer to verify
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyDecodeBinary(BaseProtocolDecoder decoder, byte[] binary, Position expected, MessageProducer messageProducer) throws Exception {
        ByteBuf buf = Unpooled.wrappedBuffer(binary);
        Position position = (Position) decoder.decode(null, null, buf);
        Assertions.assertNotNull(position);
        Assertions.assertEquals(expected.getLatitude(), position.getLatitude(), 0.0001);
        Assertions.assertEquals(expected.getLongitude(), position.getLongitude(), 0.0001);
        
        // Verify message broker integration if provided
        if (messageProducer != null) {
            verify(messageProducer, times(1)).sendPosition(any(Position.class));
        }
    }

    /**
     * Verifies that a decoder correctly processes a string message
     * 
     * @param decoder Protocol decoder
     * @param message String message
     * @param expected Expected position object
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyDecodeString(BaseProtocolDecoder decoder, String message, Position expected) throws Exception {
        verifyDecodeString(decoder, message, expected, null);
    }

    /**
     * Verifies that a decoder correctly processes a string message and publishes to message broker
     * 
     * @param decoder Protocol decoder
     * @param message String message
     * @param expected Expected position object
     * @param messageProducer Mock message producer to verify
     * @throws Exception If an error occurs during decoding
     */
    protected void verifyDecodeString(BaseProtocolDecoder decoder, String message, Position expected, MessageProducer messageProducer) throws Exception {
        Position position = (Position) decoder.decode(null, null, message);
        Assertions.assertNotNull(position);
        Assertions.assertEquals(expected.getLatitude(), position.getLatitude(), 0.0001);
        Assertions.assertEquals(expected.getLongitude(), position.getLongitude(), 0.0001);
        
        // Verify message broker integration if provided
        if (messageProducer != null) {
            verify(messageProducer, times(1)).sendPosition(any(Position.class));
        }
    }

    /**
     * Verifies that an encoder correctly encodes a command
     * 
     * @param encoder Protocol encoder
     * @param command Command to encode
     * @param expected Expected encoded message
     * @throws Exception If an error occurs during encoding
     */
    protected void verifyCommand(BaseProtocolEncoder encoder, Command command, String expected) throws Exception {
        Object result = encoder.encodeCommand(command);
        Assertions.assertEquals(expected, result.toString());
    }

    /**
     * Creates a mock device session for testing
     * 
     * @param deviceId Device identifier
     * @param uniqueId Device unique identifier
     * @param protocol Protocol implementation
     * @param channel Mock channel
     * @return Mock device session
     */
    protected DeviceSession createMockDeviceSession(long deviceId, String uniqueId, Protocol protocol, Channel channel) {
        return new DeviceSession(deviceId, uniqueId, null, protocol, channel, null);
    }

    /**
     * Creates an HTTP request for testing HTTP-based protocols
     * 
     * @param method HTTP method
     * @param path Request path
     * @return HTTP request object
     */
    protected DefaultHttpRequest createHttpRequest(String method, String path) {
        return new DefaultHttpRequest(
                HttpVersion.HTTP_1_1, HttpMethod.valueOf(method), path);
    }

    /**
     * Creates a ByteBuf from a hex string
     * 
     * @param hex Hex string
     * @return ByteBuf containing the decoded hex data
     */
    protected ByteBuf getByteBuf(String hex) {
        byte[] data = new byte[hex.length() / 2];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) Integer.parseInt(hex.substring(i * 2, i * 2 + 2), 16);
        }
        return Unpooled.wrappedBuffer(data);
    }

    /**
     * Creates a ByteBuf from a string using UTF-8 encoding
     * 
     * @param string String to encode
     * @return ByteBuf containing the encoded string
     */
    protected ByteBuf getByteBufFromString(String string) {
        return Unpooled.copiedBuffer(string, StandardCharsets.UTF_8);
    }

    /**
     * Configures a mock message producer for testing message broker integration
     * 
     * @param decoder Protocol decoder to inject the message producer into
     * @return Mock message producer
     * @throws Exception If an error occurs during configuration
     */
    protected MessageProducer configureMockMessageProducer(BaseProtocolDecoder decoder) throws Exception {
        MessageProducer messageProducer = mock(MessageProducer.class);
        doNothing().when(messageProducer).sendPosition(any(Position.class));
        doNothing().when(messageProducer).sendPositionProto(any(PositionOuterClass.Position.class));
        
        // Inject the message producer into the decoder
        try {
            var field = decoder.getClass().getDeclaredField("messageProducer");
            field.setAccessible(true);
            field.set(decoder, messageProducer);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("Decoder does not have a messageProducer field", e);
        }
        
        return messageProducer;
    }

    /**
     * Verifies that a position was published to the message broker
     * 
     * @param messageProducer Mock message producer
     * @param position Expected position
     */
    protected void verifyPositionPublished(MessageProducer messageProducer, Position position) {
        verify(messageProducer, times(1)).sendPosition(argThat(p -> 
            p.getDeviceId() == position.getDeviceId() &&
            Math.abs(p.getLatitude() - position.getLatitude()) < 0.0001 &&
            Math.abs(p.getLongitude() - position.getLongitude()) < 0.0001
        ));
    }

    /**
     * Verifies that a protobuf position was published to the message broker
     * 
     * @param messageProducer Mock message producer
     * @param position Expected position
     */
    protected void verifyProtobufPositionPublished(MessageProducer messageProducer, PositionOuterClass.Position position) {
        verify(messageProducer, times(1)).sendPositionProto(argThat(p -> 
            p.getDeviceId() == position.getDeviceId() &&
            Math.abs(p.getLatitude() - position.getLatitude()) < 0.0001 &&
            Math.abs(p.getLongitude() - position.getLongitude()) < 0.0001
        ));
    }

    /**
     * Injects a mock Config object into a decoder
     * 
     * @param decoder Protocol decoder
     * @param configValues Configuration key-value pairs
     * @return The decoder with injected config
     * @throws Exception If an error occurs during injection
     */
    protected BaseProtocolDecoder injectConfig(BaseProtocolDecoder decoder, Map<String, String> configValues) throws Exception {
        Config config = mock(Config.class);
        for (Map.Entry<String, String> entry : configValues.entrySet()) {
            when(config.getString(entry.getKey())).thenReturn(entry.getValue());
        }
        decoder.setConfig(config);
        return decoder;
    }
}