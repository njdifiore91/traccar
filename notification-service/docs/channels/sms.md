# SMS Notification Channel

## Overview

The SMS notification channel enables Traccar to send text message alerts to users' mobile devices. This channel is particularly valuable for time-sensitive notifications that require immediate attention, such as critical device alerts, geofence violations, or system warnings.

The Notification Service implements SMS delivery through the `NotificatorSms` component, which integrates with various SMS gateway providers to ensure reliable message delivery across different regions and carriers.

## SMS Gateway Configuration

The Notification Service supports multiple SMS gateway providers through a pluggable architecture. Configuration is managed through the system settings.

### Supported Gateway Types

| Gateway Type | Description | Configuration Keys |
|-------------|-------------|--------------------|
| HTTP API | RESTful API integration with providers like Twilio, ClickSend, Plivo | `sms.http.url`, `sms.http.authorization` |
| SMPP | Direct SMSC connection using Short Message Peer-to-Peer protocol | `sms.smpp.host`, `sms.smpp.port`, `sms.smpp.systemId`, `sms.smpp.password` |
| AWS SNS | Amazon Simple Notification Service integration | `sms.aws.accessKey`, `sms.aws.secretKey`, `sms.aws.region` |
| GSM Modem | Direct connection to GSM modem hardware | `sms.modem.port`, `sms.modem.baudRate` |

### Basic Configuration Example

```properties
# Enable SMS notifications
sms.enable = true

# Select gateway type (http, smpp, aws, modem)
sms.provider = http

# HTTP API configuration (for providers like Twilio)
sms.http.url = https://api.example.com/sms/send
sms.http.authorization = Bearer your-api-key
sms.http.template = {"to":"${phone}","body":"${message}"}
```

### Advanced Configuration Options

| Setting | Description | Default |
|---------|-------------|--------|
| `sms.enable` | Enable/disable SMS notifications | `false` |
| `sms.provider` | SMS gateway provider type | `http` |
| `sms.format` | Message format template | `short` |
| `sms.limitCharacters` | Maximum characters per message | `160` |
| `sms.splitLongMessages` | Split messages exceeding character limit | `true` |
| `sms.retryCount` | Number of delivery retry attempts | `3` |
| `sms.retryDelay` | Delay between retry attempts (seconds) | `60` |

## Message Formatting and Character Limits

SMS messages are subject to character limits and encoding considerations:

### Character Limits

- Standard SMS messages are limited to 160 characters using GSM-7 encoding
- Messages using Unicode characters (e.g., non-Latin alphabets) are limited to 70 characters
- Messages exceeding these limits may be split into multiple segments or truncated based on configuration

### Message Templates

The Notification Service uses the `short` template format for SMS messages to ensure concise content that fits within character limits. Templates can be customized in the notification templates configuration.

Example template for a geofence event:

```
Device: ${device.name} has ${event.type} geofence ${geofence.name} at ${event.serverTime}
```

### Message Encoding

The system automatically handles message encoding based on content:

- Messages with only standard GSM characters use GSM-7 encoding (160 chars)
- Messages with special characters use UCS-2 encoding (70 chars)
- The `sms.forceUnicode` setting can force UCS-2 encoding for all messages

## Integration with SMS Providers

The Notification Service integrates with SMS providers through the `SmsManager` interface. The implementation details vary based on the configured provider.

### HTTP API Integration

For HTTP-based SMS gateways (most common):

1. The system formats the notification message using the configured template
2. The message is sent to the SMS gateway using an HTTP POST request
3. The gateway delivers the message to the recipient's mobile device
4. Delivery status is tracked and logged for monitoring

### Message Broker Integration

In the microservices architecture, SMS notifications flow through the message broker:

1. Events are published to the notification topic by other services
2. The Notification Service consumes these events
3. For SMS notifications, messages are published to the `sms-out` topic
4. The SMS Handler consumes from this topic and processes delivery
5. Delivery status and errors are tracked through the system

## Troubleshooting SMS Delivery Issues

Common issues with SMS delivery and their solutions:

### Delivery Failures

| Issue | Possible Causes | Solutions |
|-------|----------------|----------|
| Message not sent | Invalid configuration, service unavailable | Verify SMS gateway settings, check service status |
| Invalid phone number | Missing country code, incorrect format | Ensure phone numbers include country code (e.g., +1 for US) |
| Carrier blocking | Spam filtering, content restrictions | Modify message content, contact carrier |
| Rate limiting | Exceeding provider's message quota | Implement throttling, upgrade service plan |

### Debugging Steps

1. **Check logs**: Review notification service logs for error messages
   ```
   grep "SMS" /path/to/notification-service.log
   ```

2. **Verify configuration**: Ensure SMS gateway settings are correct
   ```
   SELECT * FROM tc_server WHERE id = 'sms.provider';
   ```

3. **Test gateway connection**: Use the API testing tool to verify connectivity
   ```
   curl -X POST https://api.example.com/sms/test -H "Authorization: Bearer your-api-key"
   ```

4. **Monitor message broker**: Check if messages are being published to the SMS topic
   ```
   kafka-console-consumer --bootstrap-server localhost:9092 --topic sms-out
   ```

### Common Error Codes

| Error Code | Description | Action |
|------------|-------------|--------|
| `30001` | Queue overflow | Reduce message volume or increase capacity |
| `30002` | Account suspended | Contact provider to resolve account issues |
| `30003` | Unreachable destination | Verify recipient's phone number and carrier |
| `30004` | Message blocked | Review content for restricted terms |
| `30005` | Unknown error | Check provider's documentation for specific code |

## Best Practices

- **Keep messages concise**: Shorter messages have higher delivery rates and lower costs
- **Include opt-out instructions**: Comply with regulations by providing a way to unsubscribe
- **Monitor delivery rates**: Track success rates to identify and address issues promptly
- **Implement fallback channels**: Configure alternative notification methods for critical alerts
- **Test internationally**: Verify delivery to different countries and carriers if operating globally
- **Respect quiet hours**: Configure time-based rules to avoid sending non-critical SMS during night hours

## Regulatory Considerations

SMS messaging is subject to various regulations worldwide:

- **TCPA** (USA): Requires explicit consent before sending marketing messages
- **GDPR** (EU): Requires consent and provides right to opt-out
- **CASL** (Canada): Requires identification and unsubscribe mechanism
- **DNC Registry**: Many countries maintain Do-Not-Call lists that apply to SMS

Ensure your SMS notification implementation complies with local regulations in the regions where you operate.

## Related Documentation

- [Notification Service Overview](../README.md)
- [Email Notification Channel](./email.md)
- [Push Notification Channel](./push.md)
- [Notification Templates](../templates/README.md)