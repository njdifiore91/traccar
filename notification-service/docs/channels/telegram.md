# Telegram Notification Channel

## Overview

The Telegram notification channel enables Traccar to send notifications to users via the [Telegram Bot API](https://core.telegram.org/bots/api). This channel supports both text messages and location sharing, making it ideal for real-time tracking alerts and status updates.

## Configuration

### Bot Setup

1. Create a Telegram bot by messaging [@BotFather](https://t.me/botfather) on Telegram
2. Follow the instructions to create a new bot
3. Once created, BotFather will provide a bot token (e.g., `123456789:ABCDefGhIJKlmNoPQRsTUVwxyZ`)
4. Configure this token in your Traccar configuration:

```properties
notificator.telegram.key=123456789:ABCDefGhIJKlmNoPQRsTUVwxyZ
```

### Chat ID Configuration

Telegram notifications require a chat ID to determine where messages should be sent. There are two ways to configure this:

#### System-Wide Default Chat ID

Set a default chat ID that will be used when no user-specific chat ID is configured:

```properties
notificator.telegram.chatId=123456789
```

#### User-Specific Chat ID

For personalized notifications, each user can have their own chat ID configured in their user attributes:

1. In the user profile, add a custom attribute named `telegramChatId`
2. Set the value to the user's Telegram chat ID

### Finding Your Chat ID

1. Start a conversation with your bot
2. Send any message to the bot
3. Access the following URL (replace with your bot token):
   ```
   https://api.telegram.org/bot<YOUR_BOT_TOKEN>/getUpdates
   ```
4. Look for the `"chat":{"id":123456789}` value in the response

### Location Sharing

The Telegram channel can optionally send device location as a Telegram location message after the text notification. To enable this feature:

```properties
notificator.telegram.sendLocation=true
```

## Message Formatting

Telegram notifications support HTML formatting for text messages. The following HTML tags are supported:

- `<b>bold</b>`
- `<i>italic</i>`
- `<u>underline</u>`
- `<s>strikethrough</s>`
- `<a href="URL">link</a>`
- `<code>monospace</code>`
- `<pre>pre-formatted</pre>`

Example template with formatting:

```
<b>Device:</b> {device.name}
<b>Event:</b> {event.type}
<b>Time:</b> {event.serverTime}
<b>Location:</b> {position.address}
```

## Location Messages

When location sharing is enabled, the notification service will send:

1. A text message with the notification content
2. A location message containing:
   - Latitude and longitude
   - Course (bearing)
   - Accuracy (if available from the device)

This appears in Telegram as an interactive map that users can tap to view in their preferred mapping application.

## Implementation Details

The Telegram notification channel uses the Telegram Bot API's `sendMessage` and `sendLocation` endpoints:

- `https://api.telegram.org/bot<TOKEN>/sendMessage` - For text notifications
- `https://api.telegram.org/bot<TOKEN>/sendLocation` - For location sharing

The implementation includes circuit breaker patterns to prevent cascading failures if the Telegram API is temporarily unavailable.

## Troubleshooting

### Common Issues

#### Notifications Not Being Delivered

1. **Bot Token Issues**
   - Verify your bot token is correct
   - Ensure the bot hasn't been deleted or blocked
   - Try regenerating the bot token via BotFather

2. **Chat ID Problems**
   - Confirm the chat ID is correct
   - Make sure the user has started a conversation with the bot
   - Check that the bot hasn't been blocked by the user

3. **Network Connectivity**
   - Ensure the Notification Service can reach api.telegram.org
   - Check for any firewall rules blocking outbound HTTPS connections

#### Error Messages

| Error | Possible Cause | Solution |
|-------|---------------|----------|
| "Unauthorized" | Invalid bot token | Verify and update your bot token |
| "Bad Request: chat not found" | Incorrect chat ID | Confirm the chat ID is correct |
| "Forbidden: bot was blocked by the user" | User blocked the bot | User must unblock the bot |
| "Too Many Requests" | Rate limiting by Telegram | Implement backoff strategy or reduce notification frequency |

### Debugging

To enable debug logging for Telegram notifications:

```properties
logger.org.traccar.notificators.NotificatorTelegram=DEBUG
```

This will log detailed information about notification attempts, including request and response details.

## Limitations

- Telegram messages have a maximum length of 4096 characters
- Rate limits apply to the Telegram Bot API (approximately 30 messages per second)
- Users must initiate conversation with the bot before it can send them messages
- Location messages require valid latitude and longitude values

## Security Considerations

- Bot tokens should be treated as sensitive credentials and stored securely
- Chat IDs are unique identifiers that should be protected to prevent unauthorized message delivery
- Consider using a dedicated bot for your Traccar instance rather than sharing with other applications