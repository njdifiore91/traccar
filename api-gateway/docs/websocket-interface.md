# WebSocket Interface

## Overview

The Traccar API Gateway provides a WebSocket interface for real-time updates, allowing clients to receive instant notifications about position changes, device status updates, and events. This document describes how to establish WebSocket connections, the message formats used, and best practices for implementing WebSocket clients.

## Connection Establishment

### Connection URL

WebSocket connections are established at the following endpoint:

```
ws://{server-address}/api/socket
```

For secure connections (recommended for production):

```
wss://{server-address}/api/socket
```

### Authentication

WebSocket connections must be authenticated using one of the following methods:

1. **Token-based Authentication**: Append a token parameter to the WebSocket URL
   ```
   wss://{server-address}/api/socket?token={auth-token}
   ```
   
2. **Session-based Authentication**: Establish a session through the REST API login endpoint before connecting to the WebSocket. The WebSocket connection will inherit the session from cookies.

Without valid authentication, the connection will be rejected.

### Connection Lifecycle

1. Client initiates WebSocket connection with authentication
2. API Gateway validates the authentication
3. Upon successful authentication, the connection is established
4. Initial data (latest positions) is sent to the client
5. The connection is registered in the distributed connection registry
6. The API Gateway subscribes to relevant topics in the message broker for this user
7. Real-time updates are forwarded to the client as they occur

## Message Format

All messages exchanged over the WebSocket connection use JSON format. The server sends different types of updates as they occur.

### Server to Client Messages

Messages from the server to client contain collections of objects grouped by type:

```json
{
  "devices": [...],  // Array of device objects (optional)
  "positions": [...],  // Array of position objects (optional)
  "events": [...],  // Array of event objects (optional)
  "logs": [...]  // Array of log records (optional, only if subscribed)
}
```

Each message may contain one or more of these collections, depending on what updates are available.

#### Example: Position Update

```json
{
  "positions": [
    {
      "id": 123456789,
      "deviceId": 1,
      "protocol": "tcp",
      "deviceTime": "2023-04-15T12:30:45.000Z",
      "fixTime": "2023-04-15T12:30:45.000Z",
      "serverTime": "2023-04-15T12:30:47.000Z",
      "outdated": false,
      "valid": true,
      "latitude": 40.7128,
      "longitude": -74.0060,
      "altitude": 10.0,
      "speed": 5.5,
      "course": 45.0,
      "address": "350 5th Ave, New York, NY 10118",
      "accuracy": 5.0,
      "attributes": {
        "batteryLevel": 85,
        "ignition": true
      }
    }
  ]
}
```

#### Example: Device Update

```json
{
  "devices": [
    {
      "id": 1,
      "name": "Vehicle 1",
      "uniqueId": "123456789012345",
      "status": "online",
      "lastUpdate": "2023-04-15T12:30:47.000Z",
      "positionId": 123456789,
      "groupId": 1,
      "phone": "+1234567890",
      "model": "Model X",
      "contact": "driver@example.com",
      "category": "vehicle",
      "disabled": false,
      "attributes": {
        "fuelCapacity": 60.5
      }
    }
  ]
}
```

#### Example: Event Update

```json
{
  "events": [
    {
      "id": 987654321,
      "type": "deviceOnline",
      "serverTime": "2023-04-15T12:30:00.000Z",
      "deviceId": 1,
      "positionId": 123456789,
      "geofenceId": null,
      "maintenanceId": null,
      "attributes": {}
    }
  ]
}
```

#### Example: Log Update (Optional)

```json
{
  "logs": [
    {
      "id": 1,
      "time": "2023-04-15T12:30:00.000Z",
      "deviceId": 1,
      "level": "info",
      "type": "command",
      "message": "Command sent: reboot"
    }
  ]
}
```

### Client to Server Messages

Clients can send configuration messages to the server to control what updates they receive:

```json
{
  "logs": true  // Subscribe to log updates
}
```

By default, clients do not receive log updates. Setting `logs` to `true` enables log message reception.

## Heartbeat Mechanism

The API Gateway implements a heartbeat mechanism to maintain WebSocket connections and detect disconnections:

1. The server sends empty JSON objects (`{}`) as keepalive messages approximately every 55 seconds
2. Clients should respond to these messages to indicate they are still connected
3. If a client doesn't respond to heartbeats, the connection will be marked for cleanup
4. Connections marked for cleanup will be closed during the next cleanup cycle

## Error Handling and Reconnection

### Connection Timeout

WebSocket connections have an idle timeout configured on the server (default: 60 seconds). If no messages are exchanged within this period, the connection will be closed.

### Reconnection Strategy

Clients should implement a reconnection strategy with exponential backoff:

1. Attempt to reconnect immediately after disconnection
2. If reconnection fails, wait for a short period (e.g., 1 second) before retrying
3. For each failed attempt, increase the wait time (e.g., 2, 4, 8 seconds) up to a maximum (e.g., 30 seconds)
4. Continue attempting to reconnect until successful

### Error Codes

WebSocket connections may be closed with the following status codes:

- 1000: Normal closure (clean disconnect)
- 1001: Going away (server shutdown)
- 1008: Policy violation (authentication failure)
- 1011: Server error

Clients should handle these codes appropriately, particularly distinguishing between temporary failures (where reconnection should be attempted) and permanent failures (such as authentication issues).

## Implementation Examples

### JavaScript Example

```javascript
class TraccarWebSocket {
  constructor(serverUrl, authToken) {
    this.serverUrl = serverUrl;
    this.authToken = authToken;
    this.reconnectAttempts = 0;
    this.maxReconnectDelay = 30000; // 30 seconds
    this.listeners = {};
    this.connect();
  }

  connect() {
    const url = this.authToken
      ? `${this.serverUrl}/api/socket?token=${this.authToken}`
      : `${this.serverUrl}/api/socket`;

    this.socket = new WebSocket(url);

    this.socket.onopen = () => {
      console.log('WebSocket connection established');
      this.reconnectAttempts = 0;
      
      // Subscribe to log updates if needed
      this.socket.send(JSON.stringify({ logs: true }));
      
      if (this.listeners.connect) {
        this.listeners.connect();
      }
    };

    this.socket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      
      // Handle different types of updates
      if (data.positions && this.listeners.positions) {
        this.listeners.positions(data.positions);
      }
      
      if (data.devices && this.listeners.devices) {
        this.listeners.devices(data.devices);
      }
      
      if (data.events && this.listeners.events) {
        this.listeners.events(data.events);
      }
      
      if (data.logs && this.listeners.logs) {
        this.listeners.logs(data.logs);
      }
    };

    this.socket.onclose = (event) => {
      console.log(`WebSocket connection closed: ${event.code} ${event.reason}`);
      
      if (this.listeners.disconnect) {
        this.listeners.disconnect(event);
      }
      
      // Don't reconnect on authentication failure
      if (event.code === 1008) {
        console.error('Authentication failed, not reconnecting');
        return;
      }
      
      // Reconnect with exponential backoff
      const delay = Math.min(
        1000 * Math.pow(2, this.reconnectAttempts),
        this.maxReconnectDelay
      );
      
      console.log(`Reconnecting in ${delay}ms...`);
      setTimeout(() => this.connect(), delay);
      this.reconnectAttempts++;
    };

    this.socket.onerror = (error) => {
      console.error('WebSocket error:', error);
      if (this.listeners.error) {
        this.listeners.error(error);
      }
    };
  }

  on(event, callback) {
    this.listeners[event] = callback;
  }

  close() {
    if (this.socket) {
      this.socket.close();
    }
  }
}

// Usage example
const ws = new TraccarWebSocket('wss://traccar.example.com', 'auth-token-here');

ws.on('connect', () => {
  console.log('Connected to Traccar server');
});

ws.on('positions', (positions) => {
  console.log('Received position updates:', positions);
  // Update map with new positions
});

ws.on('devices', (devices) => {
  console.log('Received device updates:', devices);
  // Update device list
});

ws.on('events', (events) => {
  console.log('Received events:', events);
  // Show notifications for events
});
```

## Best Practices

1. **Authentication**: Always use secure WebSocket connections (wss://) in production environments.

2. **Reconnection**: Implement a robust reconnection strategy with exponential backoff.

3. **Error Handling**: Log and handle WebSocket errors appropriately, distinguishing between temporary and permanent failures.

4. **Message Processing**: Process messages asynchronously to avoid blocking the WebSocket connection.

5. **Connection Cleanup**: Properly close WebSocket connections when they are no longer needed to free server resources.

6. **Testing**: Test WebSocket implementations with various network conditions, including slow connections and intermittent failures.

7. **Monitoring**: Monitor WebSocket connections for performance issues and implement appropriate logging for troubleshooting.

## Limitations

1. **Maximum Connections**: The server may limit the number of concurrent WebSocket connections per user or IP address.

2. **Message Size**: Large messages may be rejected or cause performance issues.

3. **Rate Limiting**: Excessive connection attempts may trigger rate limiting.

4. **Browser Support**: Ensure compatibility with target browsers, as WebSocket support varies.

## Troubleshooting

1. **Connection Failures**: Verify authentication credentials and network connectivity.

2. **Missing Updates**: Ensure the user has appropriate permissions for the devices they're monitoring.

3. **Performance Issues**: Consider reducing the number of monitored devices or implementing client-side filtering.

4. **Disconnections**: Check for network issues, proxy configurations, or firewall settings that might interfere with WebSocket connections.

## API Reference

For more details on the data models used in WebSocket messages, refer to the REST API documentation, as the same models are used for both interfaces.