/*
 * Copyright 2019 - 2023 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.config;

public final class Keys {

    /**
     * Connection timeout value in seconds. Because sometimes there is no way to detect lost TCP connection old
     * connections stay in open state. On most systems there is a limit on number of open connection, so this leads to
     * problems with establishing new connections when number of devices is high or devices data connections are
     * unstable.
     */
    public static final ConfigKey SERVER_TIMEOUT = new ConfigKey(
            "server.timeout", Integer.class);

    /**
     * Minimum timeout value in seconds to wait for position before closing connection. If server timeout is less than
     * this value, this value is used instead.
     */
    public static final ConfigKey SERVER_SESSION_TIMEOUT = new ConfigKey(
            "server.sessionTimeout", Integer.class);

    /**
     * Save device IP addresses information. Disabled by default because it is a potential privacy issue.
     */
    public static final ConfigKey PROCESSING_REMOTE_ADDRESS_ENABLE = new ConfigKey(
            "processing.remoteAddressEnable", Boolean.class);

    /**
     * Enable engine hours calculation. It uses ignition value to determine engine state.
     */
    public static final ConfigKey PROCESSING_ENGINE_HOURS_ENABLE = new ConfigKey(
            "processing.engineHoursEnable", Boolean.class);

    /**
     * Enable copying of missing attributes from last position to current position. Might be useful if device doesn't
     * send some values in every message.
     */
    public static final ConfigKey PROCESSING_COPY_ATTRIBUTES_ENABLE = new ConfigKey(
            "processing.copyAttributesEnable", Boolean.class);

    /**
     * Enable computed attributes processing.
     */
    public static final ConfigKey PROCESSING_COMPUTED_ATTRIBUTES_ENABLE = new ConfigKey(
            "processing.computedAttributesEnable", Boolean.class);

    /**
     * Enable computed attributes processing.
     */
    public static final ConfigKey PROCESSING_COMPUTED_ATTRIBUTES_DEVICE_ATTRIBUTES = new ConfigKey(
            "processing.computedAttributesDeviceAttributes", Boolean.class);

    /**
     * Boolean flag to enable or disable reverse geocoder.
     */
    public static final ConfigKey GEOCODER_ENABLE = new ConfigKey(
            "geocoder.enable", Boolean.class);

    /**
     * Reverse geocoder type. Check reverse geocoding documentation for more info. By default (if not specified)
     * server uses Google API.
     */
    public static final ConfigKey GEOCODER_TYPE = new ConfigKey(
            "geocoder.type", String.class);

    /**
     * Geocoder server URL. Applicable only to Nominatim and Gisgraphy providers.
     */
    public static final ConfigKey GEOCODER_URL = new ConfigKey(
            "geocoder.url", String.class);

    /**
     * App id for use with Here provider.
     */
    public static final ConfigKey GEOCODER_ID = new ConfigKey(
            "geocoder.id", String.class);

    /**
     * Provider API key. Most providers require API keys.
     */
    public static final ConfigKey GEOCODER_KEY = new ConfigKey(
            "geocoder.key", String.class);

    /**
     * Language parameter for providers that support localization.
     */
    public static final ConfigKey GEOCODER_LANGUAGE = new ConfigKey(
            "geocoder.language", String.class);

    /**
     * Address format string. Default value is %h %r, %t, %s, %c. See AddressFormat for more info.
     */
    public static final ConfigKey GEOCODER_FORMAT = new ConfigKey(
            "geocoder.format", String.class);

    /**
     * Cache size for geocoding results.
     */
    public static final ConfigKey GEOCODER_CACHE_SIZE = new ConfigKey(
            "geocoder.cacheSize", Integer.class);

    /**
     * Disable automatic reverse geocoding requests for all positions.
     */
    public static final ConfigKey GEOCODER_IGNORE_POSITIONS = new ConfigKey(
            "geocoder.ignorePositions", Boolean.class);

    /**
     * Boolean flag to apply reverse geocoding to invalid positions.
     */
    public static final ConfigKey GEOCODER_PROCESS_INVALID_POSITIONS = new ConfigKey(
            "geocoder.processInvalidPositions", Boolean.class);

    /**
     * Optional parameter to specify minimum distance for new reverse geocoding request. If distance is less than
     * specified value (in meters), then Traccar will reuse last known address.
     */
    public static final ConfigKey GEOCODER_REUSE_DISTANCE = new ConfigKey(
            "geocoder.reuseDistance", Integer.class);

    /**
     * Boolean flag to enable LBS location resolution. Some devices send cell towers information and WiFi point when GPS
     * location is not available. Traccar can determine coordinates based on that information using third party
     * services. Default value is false.
     */
    public static final ConfigKey GEOLOCATION_ENABLE = new ConfigKey(
            "geolocation.enable", Boolean.class);

    /**
     * Provider to use for LBS location. Available options: google, mozilla and opencellid. By default opencellid is
     * used. You have to supply a key that you get from corresponding provider. For more information see LBS geolocation
     * documentation.
     */
    public static final ConfigKey GEOLOCATION_TYPE = new ConfigKey(
            "geolocation.type", String.class);

    /**
     * Geolocation provider API URL address. Not required for most providers.
     */
    public static final ConfigKey GEOLOCATION_URL = new ConfigKey(
            "geolocation.url", String.class);

    /**
     * Provider API key. OpenCellID service requires API key.
     */
    public static final ConfigKey GEOLOCATION_KEY = new ConfigKey(
            "geolocation.key", String.class);

    /**
     * Boolean flag to apply geolocation to invalid positions.
     */
    public static final ConfigKey GEOLOCATION_PROCESS_INVALID_POSITIONS = new ConfigKey(
            "geolocation.processInvalidPositions", Boolean.class);

    /**
     * Override latitude sign / hemisphere. Useful in cases where value is incorrect because of device bug. Value can be
     * N for North or S for South.
     */
    public static final ConfigKey LOCATION_LATITUDE_HEMISPHERE = new ConfigKey(
            "location.latitudeHemisphere", Boolean.class);

    /**
     * Override longitude sign / hemisphere. Useful in cases where value is incorrect because of device bug. Value can
     * be E for East or W for West.
     */
    public static final ConfigKey LOCATION_LONGITUDE_HEMISPHERE = new ConfigKey(
            "location.longitudeHemisphere", Boolean.class);

    /**
     * Enable speed limit API to get speed limit values depending on location. Default value is false.
     */
    public static final ConfigKey SPEED_LIMIT_ENABLE = new ConfigKey(
            "speedLimit.enable", Boolean.class);

    /**
     * Speed limit provider. Here or Overpass servers can be used. By default Here is selected. In case of Overpass make
     * sure you have server URL configured.
     */
    public static final ConfigKey SPEED_LIMIT_TYPE = new ConfigKey(
            "speedLimit.type", String.class);

    /**
     * Provider API URL address. Not required for Here provider.
     */
    public static final ConfigKey SPEED_LIMIT_URL = new ConfigKey(
            "speedLimit.url", String.class);

    /**
     * Provider API key. Here provider requires API key.
     */
    public static final ConfigKey SPEED_LIMIT_KEY = new ConfigKey(
            "speedLimit.key", String.class);

    /**
     * Web interface implementation. By default Traccar uses modern and responsive Bootstrap web interface. If you need
     * legacy interface, you can change value to "old". Old interface is now deprecated.
     */
    public static final ConfigKey WEB_TYPE = new ConfigKey(
            "web.type", String.class);

    /**
     * A flag that controls whether to open a browser window on the server startup. Default value is false.
     */
    public static final ConfigKey WEB_CONSOLE = new ConfigKey(
            "web.console", Boolean.class);

    /**
     * Web interface port. By default Traccar uses port 8082. You can change it to any port you want. You can also
     * configure server to use SSL.
     */
    public static final ConfigKey WEB_PORT = new ConfigKey(
            "web.port", Integer.class);

    /**
     * Web server address. By default server binds to all available network interfaces. You can specify specific IP
     * address to restrict that.
     */
    public static final ConfigKey WEB_ADDRESS = new ConfigKey(
            "web.address", String.class);

    /**
     * Path to web app directory. By default traccar uses a relative path "web".
     */
    public static final ConfigKey WEB_PATH = new ConfigKey(
            "web.path", String.class);

    /**
     * WebSocket connection timeout in milliseconds. Default timeout is 60 seconds.
     */
    public static final ConfigKey WEB_TIMEOUT = new ConfigKey(
            "web.timeout", Integer.class);

    /**
     * Authentication sessions timeout in seconds. By default sessions are valid for one hour.
     */
    public static final ConfigKey WEB_SESSION_TIMEOUT = new ConfigKey(
            "web.sessionTimeout", Integer.class);

    /**
     * Enable database access restrictions. By default anyone can read data from the database. If you enable this option
     * then devices, users and other entities will be visible only to users who have access to them.
     */
    public static final ConfigKey WEB_RESTRICT_ACCESS = new ConfigKey(
            "web.restrictAccess", Boolean.class);

    /**
     * Disable HTTP caching. By default HTTP responses are cached. When developing it might be useful to disable
     * caching.
     */
    public static final ConfigKey WEB_DISABLE_CACHE = new ConfigKey(
            "web.disableCache", Boolean.class);

    /**
     * Disable configuration modifications. By default users are allowed to make changes to configuration via UI or REST
     * API.
     */
    public static final ConfigKey WEB_DISABLE_CONFIG = new ConfigKey(
            "web.disableConfig", Boolean.class);

    /**
     * Disable links in the UI. By default users are allowed to see links in the UI.
     */
    public static final ConfigKey WEB_DISABLE_LINKS = new ConfigKey(
            "web.disableLinks", Boolean.class);

    /**
     * Disable device deletion. By default users are allowed to delete devices.
     */
    public static final ConfigKey WEB_DISABLE_DEVICE_DELETE = new ConfigKey(
            "web.disableDeviceDelete", Boolean.class);

    /**
     * Disable registration page. By default users are allowed to create new accounts.
     */
    public static final ConfigKey WEB_DISABLE_REGISTRATION = new ConfigKey(
            "web.disableRegistration", Boolean.class);

    /**
     * Disable login page. By default users are allowed to login.
     */
    public static final ConfigKey WEB_DISABLE_LOGIN = new ConfigKey(
            "web.disableLogin", Boolean.class);

    /**
     * Disable password reset. By default users are allowed to reset passwords.
     */
    public static final ConfigKey WEB_DISABLE_RESET_PASSWORD = new ConfigKey(
            "web.disableResetPassword", Boolean.class);

    /**
     * Default user password hash method. Available options: plain text, sha1, sha256, md5.
     */
    public static final ConfigKey WEB_PASSWORD_HASH = new ConfigKey(
            "web.passwordHash", String.class);

    /**
     * Salt used to generate password hashes. For security reasons it's recommended to change this value before first
     * launching the app.
     */
    public static final ConfigKey WEB_SALT = new ConfigKey(
            "web.salt", String.class);

    /**
     * Application name to display at the login page and in the browser title bar.
     */
    public static final ConfigKey WEB_NAME = new ConfigKey(
            "web.name", String.class);

    /**
     * Default user interface language. Currently available languages: ar, az, bg, bn, cs, de, dk, en, es, fa, fi, fr,
     * he, hi, hr, hu, id, it, ja, ka, kk, ko, lo, lt, lv, ml, mn, ms, nb, ne, nl, nn, pl, pt, pt_BR, ro, ru, si, sk,
     * sl, sq, sr, sv, ta, th, tr, uk, uz, vi, zh, zh_TW.
     */
    public static final ConfigKey WEB_LANGUAGE = new ConfigKey(
            "web.language", String.class);

    /**
     * Enable events filtering by device group. By default events from all devices are displayed.
     */
    public static final ConfigKey WEB_FILTER_EVENTS = new ConfigKey(
            "web.filterEvents", Boolean.class);

    /**
     * Enable web service API documentation. By default documentation is available at /api/swagger-ui.html path.
     */
    public static final ConfigKey WEB_SERVICE_API = new ConfigKey(
            "web.serviceApi", Boolean.class);

    /**
     * Enable CORS headers for web services. By default cross-origin requests are not allowed.
     */
    public static final ConfigKey WEB_CORS_ENABLE = new ConfigKey(
            "web.corsEnable", Boolean.class);

    /**
     * List of origin URLs for CORS headers.
     */
    public static final ConfigKey WEB_CORS_ORIGINS = new ConfigKey(
            "web.corsOrigins", String.class);

    /**
     * List of methods for CORS headers.
     */
    public static final ConfigKey WEB_CORS_METHODS = new ConfigKey(
            "web.corsMethods", String.class);

    /**
     * List of headers for CORS headers.
     */
    public static final ConfigKey WEB_CORS_HEADERS = new ConfigKey(
            "web.corsHeaders", String.class);

    /**
     * List of credentials for CORS headers.
     */
    public static final ConfigKey WEB_CORS_CREDENTIALS = new ConfigKey(
            "web.corsCredentials", Boolean.class);

    /**
     * List of exposed headers for CORS headers.
     */
    public static final ConfigKey WEB_CORS_EXPOSED_HEADERS = new ConfigKey(
            "web.corsExposedHeaders", String.class);

    /**
     * Boolean flag to enable forwarded header support for reverse proxies.
     */
    public static final ConfigKey WEB_FORWARDED_ENABLE = new ConfigKey(
            "web.forwardedEnable", Boolean.class);

    /**
     * List of proxies to trust for forwarded headers.
     */
    public static final ConfigKey WEB_FORWARDED_PROXIES = new ConfigKey(
            "web.forwardedProxies", String.class);

    /**
     * Boolean flag to enable WebSocket compression.
     */
    public static final ConfigKey WEB_WEBSOCKET_COMPRESSION = new ConfigKey(
            "web.websocketCompression", Boolean.class);

    /**
     * Boolean flag to enable health check endpoint.
     */
    public static final ConfigKey WEB_HEALTH_CHECK = new ConfigKey(
            "web.healthCheck", Boolean.class);

    /**
     * Boolean flag to enable health check logging.
     */
    public static final ConfigKey HEALTH_CHECK_LOGGING_ENABLED = new ConfigKey(
            "health.logging.enabled", Boolean.class);

    /**
     * Health check interval in seconds.
     */
    public static final ConfigKey HEALTH_CHECK_INTERVAL = new ConfigKey(
            "health.interval", Integer.class);

    /**
     * Protocol name. Used to apply protocol specific settings.
     */
    public static final ConfigKey PROTOCOL_NAME = new ConfigKey(
            "protocol.name", String.class);

    /**
     * Connection timeout in seconds. Default timeout is 0 (disabled). It's recommended to use timeout to avoid
     * protocol-related issues.
     */
    public static final ConfigKey PROTOCOL_TIMEOUT = new ConfigKey(
            "protocol.timeout", Integer.class);

    /**
     * Boolean flag to enable/disable protocol detection.
     */
    public static final ConfigKey PROTOCOL_DETECT = new ConfigKey(
            "protocol.detect", Boolean.class);

    /**
     * Protocol specific message length. If not specified then standard algorithm is used.
     */
    public static final ConfigKey PROTOCOL_MESSAGE_LENGTH = new ConfigKey(
            "protocol.messageLength", Integer.class);

    /**
     * Server option to enable or disable TCP keep-alive.
     */
    public static final ConfigKey PROTOCOL_KEEP_ALIVE_ENABLE = new ConfigKey(
            "protocol.keepAliveEnable", Boolean.class);

    /**
     * Server option to configure TCP keep-alive idle timeout.
     */
    public static final ConfigKey PROTOCOL_KEEP_ALIVE_IDLE = new ConfigKey(
            "protocol.keepAliveIdle", Integer.class);

    /**
     * Server option to configure TCP keep-alive packet interval.
     */
    public static final ConfigKey PROTOCOL_KEEP_ALIVE_INTERVAL = new ConfigKey(
            "protocol.keepAliveInterval", Integer.class);

    /**
     * Server option to configure TCP keep-alive retry count.
     */
    public static final ConfigKey PROTOCOL_KEEP_ALIVE_COUNT = new ConfigKey(
            "protocol.keepAliveCount", Integer.class);

    /**
     * Boolean flag to enable or disable SSL support for incoming connections.
     */
    public static final ConfigKey PROTOCOL_SSL = new ConfigKey(
            "protocol.ssl", Boolean.class);

    /**
     * Boolean flag to enable or disable SSL certificate validation for outgoing connections.
     */
    public static final ConfigKey PROTOCOL_SSL_TRUST_ALL = new ConfigKey(
            "protocol.sslTrustAll", Boolean.class);

    /**
     * Protocol data handling tasks executor service threads count.
     */
    public static final ConfigKey PROTOCOL_DATA_HANDLING_THREADS_COUNT = new ConfigKey(
            "protocol.dataHandlingThreadsCount", Integer.class);

    /**
     * Server listening port. By default Traccar uses 5000-5150 range of ports for all protocols. You can narrow down
     * port range or even assign dedicated port for specific protocol.
     */
    public static final ConfigKey PROTOCOL_PORT = new ConfigKey(
            "protocol.port", Integer.class);

    /**
     * Server listening address. By default Traccar binds to all network interfaces. You can specify specific interface
     * to bind.
     */
    public static final ConfigKey PROTOCOL_ADDRESS = new ConfigKey(
            "protocol.address", String.class);

    /**
     * Boolean flag to enable or disable protocol logging to database.
     */
    public static final ConfigKey PROTOCOL_DATABASE = new ConfigKey(
            "protocol.database", Boolean.class);

    /**
     * Version of the application. Specified in the Maven build plugin.
     */
    public static final ConfigKey VERSION = new ConfigKey(
            "version", String.class);

    /**
     * Service discovery type. Available options: consul, kubernetes, none.
     */
    public static final ConfigKey SERVICE_DISCOVERY_TYPE = new ConfigKey(
            "service.discovery.type", String.class);

    /**
     * Consul service discovery URL.
     */
    public static final ConfigKey SERVICE_DISCOVERY_CONSUL_URL = new ConfigKey(
            "service.discovery.consul.url", String.class);

    /**
     * Kubernetes service discovery namespace.
     */
    public static final ConfigKey SERVICE_DISCOVERY_K8S_NAMESPACE = new ConfigKey(
            "service.discovery.kubernetes.namespace", String.class);

    /**
     * Kubernetes service discovery API URL.
     */
    public static final ConfigKey SERVICE_DISCOVERY_K8S_API_URL = new ConfigKey(
            "service.discovery.kubernetes.api.url", String.class);

    /**
     * Health check endpoint path.
     */
    public static final ConfigKey SERVICE_DISCOVERY_HEALTH_PATH = new ConfigKey(
            "service.discovery.health.path", String.class);

    /**
     * Health check interval in seconds.
     */
    public static final ConfigKey SERVICE_DISCOVERY_HEALTH_INTERVAL = new ConfigKey(
            "service.discovery.health.interval", Integer.class);

    private Keys() {
    }

}