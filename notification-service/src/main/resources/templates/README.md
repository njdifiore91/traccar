# Notification Templates

This directory contains Velocity templates used for formatting notifications across different channels.

## Template Naming Convention

Templates follow a specific naming convention:

- `{name}.vm` - Default template (version 1)
- `{name}-v{version}.vm` - Versioned template (e.g., `email-v2.vm`)
- `{name}-fallback.vm` - Fallback template used when the primary template cannot be loaded

## Template Versioning

Templates are versioned to maintain backward compatibility. When a template is significantly changed,
a new version should be created rather than modifying the existing template. This ensures that
existing notifications continue to work as expected.

## Available Templates

### Email Templates

- `email.vm` - Default email template
- `email-html.vm` - HTML email template

### SMS Templates

- `sms.vm` - Default SMS template

### Push Notification Templates

- `push.vm` - Default push notification template

### Web Notification Templates

- `web.vm` - Default web notification template

## Template Context

Templates have access to the following context variables:

- `event` - The event that triggered the notification
- `position` - The position associated with the event
- `device` - The device associated with the event
- `user` - The user receiving the notification
- `geocoder` - Geocoding information for the position
- `speedUnit` - The user's preferred speed unit
- `distanceUnit` - The user's preferred distance unit
- `timezone` - The user's timezone

## Example Template

```velocity
#if($event.type == "deviceOffline")
  Device $device.name is offline since $dateTool.format("yyyy-MM-dd HH:mm:ss", $event.serverTime, $timezone)
#elseif($event.type == "deviceOnline")
  Device $device.name is online again
#elseif($event.type == "deviceOverspeed")
  Device $device.name exceeded the speed limit: $position.speed $speedUnit
#else
  $event.type event detected on device $device.name
#end
```