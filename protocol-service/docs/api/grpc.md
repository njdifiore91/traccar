# Protocol Service gRPC API Documentation

## Overview

The Protocol Service exposes a gRPC API that allows other microservices to interact with device protocols and manage device communications. This document describes the gRPC service definitions, message formats, authentication requirements, and provides examples for client implementations.

## Service Definitions

The Protocol Service exposes the following gRPC services:

### ProtocolManagementService

This service provides operations for managing protocols and device communications.

```protobuf
syntax = "proto3";

package org.traccar.protocol.grpc;

import "google/protobuf/empty.proto";
import "common/command.proto";

option java_multiple_files = true;
option java_package = "org.traccar.protocol.grpc";
option java_outer_classname = "ProtocolServiceProto";

service ProtocolManagementService {
  // Get a list of all supported protocols
  rpc GetSupportedProtocols(google.protobuf.Empty) returns (SupportedProtocolsResponse);
  
  // Get details about a specific protocol
  rpc GetProtocolDetails(ProtocolDetailsRequest) returns (ProtocolDetailsResponse);
  
  // Send a command to a device
  rpc SendCommand(CommandRequest) returns (CommandResponse);
  
  // Get protocol statistics
  rpc GetProtocolStatistics(ProtocolStatisticsRequest) returns (ProtocolStatisticsResponse);
  
  // Get active device connections
  rpc GetActiveConnections(ActiveConnectionsRequest) returns (ActiveConnectionsResponse);
}
```

### DeviceCommandService

This service provides operations for sending commands to devices.

```protobuf
syntax = "proto3";

package org.traccar.protocol.grpc;

import "common/command.proto";

option java_multiple_files = true;
option java_package = "org.traccar.protocol.grpc";
option java_outer_classname = "DeviceCommandServiceProto";

service DeviceCommandService {
  // Send a data command to a device
  rpc SendDataCommand(DataCommandRequest) returns (CommandResponse);
  
  // Send a text command to a device
  rpc SendTextCommand(TextCommandRequest) returns (CommandResponse);
  
  // Send a push command to a device
  rpc SendPushCommand(PushCommandRequest) returns (CommandResponse);
  
  // Check if a command type is supported by a protocol
  rpc IsCommandSupported(CommandSupportRequest) returns (CommandSupportResponse);
}
```

## Message Definitions

### Protocol Management Messages

```protobuf
// Request to get details about a specific protocol
message ProtocolDetailsRequest {
  string protocol_name = 1;
}

// Response containing details about a protocol
message ProtocolDetailsResponse {
  string protocol_name = 1;
  repeated string supported_data_commands = 2;
  repeated string supported_text_commands = 3;
  repeated string supported_push_commands = 4;
  repeated ConnectorInfo connectors = 5;
}

// Information about a protocol connector
message ConnectorInfo {
  string type = 1; // "server" or "client"
  string protocol = 2; // "tcp", "udp", etc.
  int32 port = 3;
  bool enabled = 4;
}

// Response containing a list of supported protocols
message SupportedProtocolsResponse {
  repeated ProtocolInfo protocols = 1;
}

// Basic information about a protocol
message ProtocolInfo {
  string name = 1;
  string description = 2;
}

// Request to get protocol statistics
message ProtocolStatisticsRequest {
  string protocol_name = 1; // Optional, if not provided, returns stats for all protocols
}

// Response containing protocol statistics
message ProtocolStatisticsResponse {
  repeated ProtocolStats protocol_stats = 1;
}

// Statistics for a specific protocol
message ProtocolStats {
  string protocol_name = 1;
  int32 active_connections = 2;
  int32 messages_received = 3;
  int32 messages_sent = 4;
  int32 errors = 5;
}

// Request to get active connections
message ActiveConnectionsRequest {
  string protocol_name = 1; // Optional, if not provided, returns connections for all protocols
  int32 limit = 2; // Optional, limit the number of results
  int32 offset = 3; // Optional, offset for pagination
}

// Response containing active connections
message ActiveConnectionsResponse {
  repeated ConnectionInfo connections = 1;
  int32 total_count = 2;
}

// Information about a device connection
message ConnectionInfo {
  string device_id = 1;
  string protocol_name = 2;
  string remote_address = 3;
  int64 connection_time = 4; // Unix timestamp in milliseconds
  int64 last_activity_time = 5; // Unix timestamp in milliseconds
}
```

### Device Command Messages

```protobuf
// Request to send a data command to a device
message DataCommandRequest {
  string device_id = 1;
  org.traccar.proto.Command command = 2;
}

// Request to send a text command to a device
message TextCommandRequest {
  string device_id = 1;
  string destination_address = 2; // Phone number or other address for text commands
  org.traccar.proto.Command command = 3;
}

// Request to send a push command to a device
message PushCommandRequest {
  string device_id = 1;
  org.traccar.proto.Command command = 2;
}

// Response to a command request
message CommandResponse {
  bool success = 1;
  string message = 2; // Error message if success is false
  string result = 3; // Optional result data if success is true
}

// Request to check if a command type is supported by a protocol
message CommandSupportRequest {
  string protocol_name = 1;
  string command_type = 2;
}

// Response indicating if a command type is supported
message CommandSupportResponse {
  bool supported = 1;
}
```

## Authentication and Authorization

The Protocol Service gRPC API requires authentication and authorization for all requests. Two methods are supported:

### Mutual TLS (mTLS)

For service-to-service communication, mutual TLS (mTLS) is the preferred authentication method. Both the client and server authenticate each other using X.509 certificates.

1. Each service must have its own TLS certificate and private key.
2. The certificate must be signed by a trusted Certificate Authority (CA).
3. The client must present its certificate during the TLS handshake.
4. The server validates the client's certificate against the trusted CA.

### JWT Authentication

For scenarios where mTLS is not feasible, JWT (JSON Web Token) authentication is supported.

1. The client must obtain a valid JWT token from the authentication service.
2. The token must be included in the gRPC metadata with the key `authorization` and value `Bearer <token>`.
3. The server validates the token signature, expiration, and claims.

## Client Implementation Examples

### Java Client Example (with mTLS)

```java
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import org.traccar.protocol.grpc.ProtocolManagementServiceGrpc;
import org.traccar.protocol.grpc.ProtocolDetailsRequest;
import org.traccar.protocol.grpc.ProtocolDetailsResponse;

import java.io.File;

public class ProtocolServiceClient {
    public static void main(String[] args) throws Exception {
        // Set up SSL context with mutual TLS
        SslContext sslContext = GrpcSslContexts.forClient()
                .trustManager(new File("/path/to/ca.crt"))
                .keyManager(new File("/path/to/client.crt"), new File("/path/to/client.key"))
                .build();
        
        // Create a secure channel
        ManagedChannel channel = NettyChannelBuilder.forAddress("protocol-service.traccar.svc.cluster.local", 9090)
                .sslContext(sslContext)
                .build();
        
        try {
            // Create a stub
            ProtocolManagementServiceGrpc.ProtocolManagementServiceBlockingStub stub = 
                    ProtocolManagementServiceGrpc.newBlockingStub(channel);
            
            // Make a request
            ProtocolDetailsRequest request = ProtocolDetailsRequest.newBuilder()
                    .setProtocolName("osmand")
                    .build();
            
            // Get the response
            ProtocolDetailsResponse response = stub.getProtocolDetails(request);
            
            // Process the response
            System.out.println("Protocol: " + response.getProtocolName());
            System.out.println("Supported data commands: " + response.getSupportedDataCommandsList());
            System.out.println("Supported text commands: " + response.getSupportedTextCommandsList());
        } finally {
            // Shutdown the channel
            channel.shutdown();
        }
    }
}
```

### Java Client Example (with JWT)

```java
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import io.grpc.ManagedChannelBuilder;
import org.traccar.protocol.grpc.DeviceCommandServiceGrpc;
import org.traccar.protocol.grpc.DataCommandRequest;
import org.traccar.protocol.grpc.CommandResponse;
import org.traccar.proto.Command;

public class DeviceCommandClient {
    public static void main(String[] args) {
        // Create a channel
        ManagedChannel channel = ManagedChannelBuilder.forAddress("protocol-service.traccar.svc.cluster.local", 9090)
                .usePlaintext() // For development only, use TLS in production
                .build();
        
        try {
            // Create a stub
            DeviceCommandServiceGrpc.DeviceCommandServiceBlockingStub stub = 
                    DeviceCommandServiceGrpc.newBlockingStub(channel);
            
            // Add JWT token to metadata
            Metadata metadata = new Metadata();
            Metadata.Key<String> key = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
            metadata.put(key, "Bearer " + getJwtToken());
            stub = MetadataUtils.attachHeaders(stub, metadata);
            
            // Create a command
            Command command = Command.newBuilder()
                    .setType("positionSingle")
                    .build();
            
            // Create a request
            DataCommandRequest request = DataCommandRequest.newBuilder()
                    .setDeviceId("123456")
                    .setCommand(command)
                    .build();
            
            // Send the command
            CommandResponse response = stub.sendDataCommand(request);
            
            // Process the response
            if (response.getSuccess()) {
                System.out.println("Command sent successfully: " + response.getResult());
            } else {
                System.out.println("Command failed: " + response.getMessage());
            }
        } finally {
            // Shutdown the channel
            channel.shutdown();
        }
    }
    
    private static String getJwtToken() {
        // Implementation to get a JWT token from the authentication service
        return "your.jwt.token";
    }
}
```

### Python Client Example

```python
import grpc
import protocol_pb2
import protocol_pb2_grpc
from google.protobuf.empty_pb2 import Empty

# Create credentials for mTLS
credentials = grpc.ssl_channel_credentials(
    root_certificates=open('/path/to/ca.crt', 'rb').read(),
    private_key=open('/path/to/client.key', 'rb').read(),
    certificate_chain=open('/path/to/client.crt', 'rb').read()
)

# Create a secure channel
channel = grpc.secure_channel('protocol-service.traccar.svc.cluster.local:9090', credentials)

# Create a stub
stub = protocol_pb2_grpc.ProtocolManagementServiceStub(channel)

try:
    # Get all supported protocols
    response = stub.GetSupportedProtocols(Empty())
    
    # Print the protocols
    print("Supported protocols:")
    for protocol in response.protocols:
        print(f"- {protocol.name}: {protocol.description}")
    
    # Get details for a specific protocol
    details_request = protocol_pb2.ProtocolDetailsRequest(protocol_name="teltonika")
    details_response = stub.GetProtocolDetails(details_request)
    
    print(f"\nDetails for {details_response.protocol_name}:")
    print(f"Data commands: {details_response.supported_data_commands}")
    print(f"Text commands: {details_response.supported_text_commands}")
    print("Connectors:")
    for connector in details_response.connectors:
        print(f"- {connector.type} ({connector.protocol}) on port {connector.port}")
        
finally:
    # Close the channel
    channel.close()
```

## Versioning and Backward Compatibility

The Protocol Service maintains versioned message schemas to ensure backward compatibility:

1. All Protocol Buffer definitions include version numbers in their package names when major changes are introduced.
2. New fields are added in a backward-compatible way, using optional fields.
3. Deprecated fields are marked with the `deprecated=true` option but are not removed.
4. Breaking changes are introduced in new service methods, keeping the old methods functional.

Clients should check the service version and adapt their behavior accordingly:

```protobuf
// Example of versioned service definition
service ProtocolManagementServiceV2 {
  // New version of the GetProtocolDetails method with additional fields
  rpc GetProtocolDetails(ProtocolDetailsRequestV2) returns (ProtocolDetailsResponseV2);
  
  // Original method is kept for backward compatibility
  rpc GetProtocolDetailsV1(ProtocolDetailsRequest) returns (ProtocolDetailsResponse);
}
```

## Error Handling

The Protocol Service uses standard gRPC error codes to indicate failures:

| Error Code | Description |
|------------|-------------|
| INVALID_ARGUMENT | The request contains invalid parameters |
| NOT_FOUND | The requested resource (protocol, device) was not found |
| PERMISSION_DENIED | The client lacks permission to perform the operation |
| UNAUTHENTICATED | The client failed to authenticate |
| UNAVAILABLE | The service is temporarily unavailable |
| INTERNAL | An internal server error occurred |
| UNIMPLEMENTED | The requested operation is not implemented |

Error responses include detailed error messages to help diagnose issues.

## Rate Limiting

The Protocol Service implements rate limiting to protect against abuse:

1. Each client is limited to a certain number of requests per minute.
2. Rate limits are applied per method and per client.
3. When a rate limit is exceeded, the service returns a RESOURCE_EXHAUSTED error.
4. Clients should implement exponential backoff when encountering rate limit errors.

## Monitoring and Health Checks

The Protocol Service exposes a health check endpoint that can be used to verify the service's status:

```protobuf
service Health {
  rpc Check(HealthCheckRequest) returns (HealthCheckResponse);
  rpc Watch(HealthCheckRequest) returns (stream HealthCheckResponse);
}

message HealthCheckRequest {
  string service = 1;
}

message HealthCheckResponse {
  enum ServingStatus {
    UNKNOWN = 0;
    SERVING = 1;
    NOT_SERVING = 2;
    SERVICE_UNKNOWN = 3;
  }
  ServingStatus status = 1;
}
```

Clients can use this endpoint to check if the service is available before making requests.

## Conclusion

The Protocol Service gRPC API provides a robust and efficient way for other microservices to interact with device protocols and manage device communications. By following the guidelines in this document, clients can securely and reliably communicate with the Protocol Service.