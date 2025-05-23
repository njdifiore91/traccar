# Avema Protocol Documentation

## Overview

The Avema protocol is used by GPS tracking devices manufactured by Avema, a company headquartered in Taiwan with over 15 years of experience in the GPS industry. Avema produces various tracking devices including vehicle trackers, personal trackers, and marine trackers.

This protocol is supported in Traccar since version 4.0 and uses port 5171 by default.

## Protocol Specification

### Connection Method

The Avema protocol uses TCP for communication between the device and the server. Devices connect to the server on port 5171 by default.

### Message Format

The Avema protocol uses a text-based format with comma-separated values. Each message contains various fields that provide information about the device's status, position, and other attributes.

The general format of an Avema message is as follows:

```
DEVICE_ID,TIMESTAMP,LATITUDE,LONGITUDE,SPEED,COURSE,STATUS,SATELLITES,SIGNAL_STRENGTH,ODOMETER,POWER_STATUS,BATTERY_VOLTAGE,EXTERNAL_VOLTAGE,IGNITION,DOOR_STATUS,MCC,MNC,LAC,CELL_ID,ADDITIONAL_INFO
```

### Field Descriptions

| Field | Description | Example |
|-------|-------------|--------|
| DEVICE_ID | Unique identifier for the device | 1130048939 |
| TIMESTAMP | Date and time in format YYYYMMDDHHMMSS | 20120224000129 |
| LATITUDE | Latitude in decimal degrees | 121.447487 |
| LONGITUDE | Longitude in decimal degrees | 25.168025 |
| SPEED | Speed in km/h | 0 |
| COURSE | Direction in degrees (0-359) | 0 |
| STATUS | Device status code | 0 |
| SATELLITES | Number of satellites used for positioning | 0 |
| SIGNAL_STRENGTH | Signal strength indicator | 3 |
| ODOMETER | Odometer reading | 0.0 |
| POWER_STATUS | Power status indicator | 1 |
| BATTERY_VOLTAGE | Internal battery voltage | 0.02V |
| EXTERNAL_VOLTAGE | External power supply voltage | 14.88V |
| IGNITION | Ignition status (0=off, 1=on) | 0 |
| DOOR_STATUS | Door status (0=closed, 1=open) | 1 |
| MCC | Mobile Country Code | 24 |
| MNC | Mobile Network Code | 4 |
| LAC | Location Area Code | 46608 |
| CELL_ID | Cell ID | F8BC |
| ADDITIONAL_INFO | Additional device-specific information | F9AD,CID0000028 |

### Message Examples

#### Position Update Message

```
1130048939,20120224000129,121.447487,25.168025,0,0,0,0,3,0.0,1,0.02V,14.88V,0,1,24,4,46608,F8BC,F9AD,CID0000028
```

#### Heartbeat Message

```
8,20180927150956,19.154864,49.124862,7,56,0,12,3,0.0,0,0.02,14.01,0,0,26,0,219-2,65534,10255884,0.01
```

## Traccar Implementation

### Protocol Class

The Avema protocol is implemented in Traccar through the `AvemaProtocol` class, which extends the `BaseProtocol` class. This class defines the protocol characteristics, including the default port and supported commands.

```java
public class AvemaProtocol extends BaseProtocol {

    public AvemaProtocol() {
        super("avema");
        setTextDelimiter("\r\n");
        setDefaultPort(5171);
    }

    @Override
    public void initTrackerServers(List<TrackerServer> serverList) {
        serverList.add(new TrackerServer(false, getName()) {
            @Override
            protected void addProtocolHandlers(PipelineBuilder pipeline) {
                pipeline.addLast(new LineBasedFrameDecoder(1024));
                pipeline.addLast(new StringDecoder());
                pipeline.addLast(new StringEncoder());
                pipeline.addLast(new AvemaProtocolDecoder(AvemaProtocol.this));
            }
        });
    }
}
```

### Protocol Decoder

The `AvemaProtocolDecoder` class is responsible for parsing the messages received from Avema devices and converting them into the standard Position objects used by Traccar.

```java
public class AvemaProtocolDecoder extends BaseProtocolDecoder {

    public AvemaProtocolDecoder(Protocol protocol) {
        super(protocol);
    }

    @Override
    protected Object decode(
            Channel channel, SocketAddress remoteAddress, Object msg) throws Exception {

        String sentence = (String) msg;
        String[] values = sentence.split(",");

        DeviceSession deviceSession = getDeviceSession(channel, remoteAddress, values[0]);
        if (deviceSession == null) {
            return null;
        }

        Position position = new Position(getProtocolName());
        position.setDeviceId(deviceSession.getDeviceId());

        // Parse timestamp
        if (values[1].length() == 14) {
            DateBuilder dateBuilder = new DateBuilder()
                    .setYear(Integer.parseInt(values[1].substring(0, 4)))
                    .setMonth(Integer.parseInt(values[1].substring(4, 6)))
                    .setDay(Integer.parseInt(values[1].substring(6, 8)))
                    .setHour(Integer.parseInt(values[1].substring(8, 10)))
                    .setMinute(Integer.parseInt(values[1].substring(10, 12)))
                    .setSecond(Integer.parseInt(values[1].substring(12, 14)));
            position.setTime(dateBuilder.getDate());
        }

        // Parse location
        position.setLatitude(Double.parseDouble(values[2]));
        position.setLongitude(Double.parseDouble(values[3]));
        position.setSpeed(Double.parseDouble(values[4]));
        position.setCourse(Double.parseDouble(values[5]));

        // Parse device status
        position.set(Position.KEY_STATUS, values[6]);
        position.set(Position.KEY_SATELLITES, Integer.parseInt(values[7]));
        position.set(Position.KEY_RSSI, Integer.parseInt(values[8]));
        position.set(Position.KEY_ODOMETER, Double.parseDouble(values[9]));
        position.set(Position.KEY_POWER, Integer.parseInt(values[10]));

        // Parse voltages
        String batteryVoltage = values[11];
        if (batteryVoltage.endsWith("V")) {
            batteryVoltage = batteryVoltage.substring(0, batteryVoltage.length() - 1);
        }
        position.set(Position.KEY_BATTERY, Double.parseDouble(batteryVoltage));

        String externalVoltage = values[12];
        if (externalVoltage.endsWith("V")) {
            externalVoltage = externalVoltage.substring(0, externalVoltage.length() - 1);
        }
        position.set(Position.KEY_POWER, Double.parseDouble(externalVoltage));

        // Parse additional status information
        position.set(Position.KEY_IGNITION, Integer.parseInt(values[13]) > 0);
        position.set(Position.KEY_DOOR, Integer.parseInt(values[14]) > 0);

        // Parse network information
        position.set(Position.KEY_MCC, Integer.parseInt(values[15]));
        position.set(Position.KEY_MNC, Integer.parseInt(values[16]));
        position.set(Position.KEY_LAC, values[17]);
        position.set(Position.KEY_CID, values[18]);

        // Add remaining fields as additional attributes
        if (values.length > 19) {
            for (int i = 19; i < values.length; i++) {
                position.set("additional" + (i - 19), values[i]);
            }
        }

        return position;
    }
}
```

## Configuration

### Server Configuration

To enable the Avema protocol in Traccar, add the following line to the `traccar.xml` configuration file:

```xml
<entry key='avema.port'>5171</entry>
```

### Device Configuration

To configure an Avema device to connect to your Traccar server:

1. Set the server IP address or domain name in the device configuration
2. Set the server port to 5171 (or your custom configured port)
3. Configure the reporting interval as needed

The specific configuration method depends on the device model. Refer to the device's user manual for detailed instructions.

## Troubleshooting

### Common Issues

1. **Device not connecting**: Verify that the device is configured with the correct server address and port. Check firewall settings to ensure the port is open.

2. **Device connects but no data appears**: Verify that the device ID is correctly set in both the device and the Traccar server.

3. **Incorrect position data**: Some Avema devices may send coordinates in a different format. Check the device documentation for the specific coordinate format.

### Debugging

To enable debug logging for the Avema protocol, add the following line to the `traccar.xml` configuration file:

```xml
<entry key='logger.avema'>debug</entry>
```

This will provide detailed logs of all communication with Avema devices, which can help identify issues.

## References

1. Avema Company Website: [Avema](https://www.avema.com.tw/)
2. Traccar Protocol Implementation: [GitHub Repository](https://github.com/traccar/traccar)
3. Traccar Forums: [Avema AT35 4G/LTE MiFi](https://www.traccar.org/forums/topic/avema-at35-4glte-mifi/)