package org.traccar.forward;

import org.junit.jupiter.api.Test;
import org.traccar.ProtocolTest;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.model.Device;
import org.traccar.model.Position;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for the PositionForwarderUrl class in the position-service microservice.
 * This test verifies that the formatRequest method correctly substitutes placeholders
 * in an HTTP forward URL template.
 */
public class PositionForwarderUrlTest extends ProtocolTest {

    @Test
    public void testFormatRequest() throws Exception {
        // Create a configuration with a URL template containing placeholders
        Config config = new Config();
        config.setString(Keys.FORWARD_URL, "http://localhost/?fixTime={fixTime}&gprmc={gprmc}&name={name}");

        // Create a position object with test data
        Position position = position("2016-01-01 01:02:03.000", true, 20, 30);

        // Create a mock device with test data
        var device = mock(Device.class);
        when(device.getId()).thenReturn(1L);
        when(device.getName()).thenReturn("test");
        when(device.getUniqueId()).thenReturn("123456789012345");
        when(device.getStatus()).thenReturn(Device.STATUS_ONLINE);

        // Create a position data object with the position and device
        PositionData positionData = new PositionData();
        positionData.setPosition(position);
        positionData.setDevice(device);

        // Create the forwarder with the configuration
        PositionForwarderUrl forwarder = new PositionForwarderUrl(config, null, null);

        // Verify that the formatRequest method correctly substitutes the placeholders
        assertEquals(
                "http://localhost/?fixTime=1451610123000&gprmc=$GPRMC,010203.000,A,2000.0000,N,03000.0000,E,0.00,0.00,010116,,*05&name=test",
                forwarder.formatRequest(positionData));
    }
}