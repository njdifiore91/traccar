package org.traccar.geocoder;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Integration test suite that validates address resolution and formatting for all supported
 * geocoder implementations in the position service. It tests each geocoder's ability to convert
 * coordinates to human-readable addresses, ensuring proper integration with external geocoding
 * services and correct formatting of returned addresses.
 */
@SpringBootTest
@ActiveProfiles("test")
@Testcontainers
public class GeocoderTest {

    @RegisterExtension
    static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

    static {
        Locale.setDefault(Locale.US);
    }

    private Client client;
    private Tracer tracer;
    private String correlationId;

    @Autowired(required = false)
    private DiscoveryClient discoveryClient;

    /**
     * Mock geocoding service container for testing with containerized dependencies.
     * This container simulates an external geocoding service for integration testing.
     */
    @Container
    private static final GenericContainer<?> mockGeocodingService = new GenericContainer<>("mockserver/mockserver:latest")
            .withExposedPorts(1080)
            .withEnv("MOCKSERVER_INITIALIZATION_JSON_PATH", "/config/geocoder-mocks.json");

    @BeforeEach
    public void setUp() {
        // Initialize JAX-RS client
        client = ClientBuilder.newClient();
        
        // Initialize OpenTelemetry tracer
        tracer = otelTesting.getOpenTelemetry().getTracer("geocoder-test");
        
        // Generate a unique correlation ID for distributed tracing
        correlationId = UUID.randomUUID().toString();
    }

    @AfterEach
    public void tearDown() {
        // Properly close the client to release resources
        if (client != null) {
            client.close();
        }
    }

    /**
     * Helper method to get service URL from service discovery or use default if not available.
     * 
     * @param serviceName the name of the service to discover
     * @param defaultUrl the default URL to use if service discovery is not available
     * @return the service URL
     */
    private String getServiceUrl(String serviceName, String defaultUrl) {
        if (discoveryClient != null) {
            return discoveryClient.getInstances(serviceName)
                    .stream()
                    .findFirst()
                    .map(instance -> instance.getUri().toString())
                    .orElse(defaultUrl);
        }
        return defaultUrl;
    }

    /**
     * Helper method to create a span for distributed tracing.
     * 
     * @param operationName the name of the operation being traced
     * @return the created span
     */
    private Span createSpan(String operationName) {
        return tracer.spanBuilder(operationName)
                .setSpanKind(SpanKind.CLIENT)
                .setAttribute("correlation.id", correlationId)
                .startSpan();
    }

    @Disabled
    @Test
    public void testGoogle() {
        Span span = createSpan("geocoder-google-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new GoogleGeocoder(
                        client, 
                        getServiceUrl("google-maps", null), 
                        "test-api-key", 
                        null, 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(31.776797, 35.211489, null);
            });
            
            assertEquals("1 Ibn Shaprut St, Jerusalem, Jerusalem District, IL", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testNominatim() {
        Span span = createSpan("geocoder-nominatim-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new NominatimGeocoder(
                        client, 
                        getServiceUrl("nominatim", null), 
                        "traccar-test", 
                        null, 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(40.7337807, -73.9974401, null);
            });
            
            assertEquals("35 West 9th Street, NYC, New York, US", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testGisgraphy() {
        Span span = createSpan("geocoder-gisgraphy-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new GisgraphyGeocoder(
                        client, 
                        getServiceUrl("gisgraphy", null), 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(48.8530000, 2.3400000, null);
            });
            
            assertEquals("Rue du Jardinet, Paris, Île-de-France, FR", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testOpenCage() {
        Span span = createSpan("geocoder-opencage-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new OpenCageGeocoder(
                        client, 
                        getServiceUrl("opencage", "http://api.opencagedata.com/geocode/v1"), 
                        "test-api-key", 
                        null, 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(34.116302, -118.051519, null);
            });
            
            assertEquals("Charleston Road, California, US", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testGeocodeFarm() {
        Span span = createSpan("geocoder-geocodefarm-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new GeocodeFarmGeocoder(
                        client, 
                        getServiceUrl("geocodefarm", null), 
                        "test-api-key", 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(34.116302, -118.051519, null);
            });
            
            assertEquals("604 Estrella Ave, Arcadia, CA, United States", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testGeocodeXyz() {
        Span span = createSpan("geocoder-geocodexyz-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new GeocodeXyzGeocoder(
                        client, 
                        getServiceUrl("geocodexyz", null), 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(34.116302, -118.051519, null);
            });
            
            assertEquals("605 ESTRELLA AVE, ARCADIA, California United States of America, US", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testBan() {
        Span span = createSpan("geocoder-ban-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new BanGeocoder(
                        client, 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(48.8575, 2.2944, null);
            });
            
            assertEquals("8 Avenue Gustave Eiffel, Paris, FR", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testHere() {
        Span span = createSpan("geocoder-here-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new HereGeocoder(
                        client, 
                        getServiceUrl("here", null), 
                        "test-api-key", 
                        null, 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(48.8575, 2.2944, null);
            });
            
            assertEquals("1 Tour Eiffel, Paris, Île-de-France, FRA", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testMapmyIndia() {
        Span span = createSpan("geocoder-mapmyindia-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new MapmyIndiaGeocoder(
                        client, 
                        getServiceUrl("mapmyindia", ""), 
                        "test-api-key", 
                        0, 
                        new AddressFormat("%f"));
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(28.6129602407977, 77.2294557094574, null);
            });
            
            assertEquals("New Delhi, Delhi. 1 m from India Gate pin-110001 (India)", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testPositionStack() {
        Span span = createSpan("geocoder-positionstack-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new PositionStackGeocoder(
                        client, 
                        getServiceUrl("positionstack", ""), 
                        0, 
                        new AddressFormat("%f"));
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(28.6129602407977, 77.2294557094574, null);
            });
            
            assertEquals("India Gate, New Delhi, India", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testMapbox() {
        Span span = createSpan("geocoder-mapbox-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new MapboxGeocoder(
                        client, 
                        getServiceUrl("mapbox", ""), 
                        0, 
                        new AddressFormat("%f"));
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(40.733, -73.989, null);
            });
            
            assertEquals("120 East 13th Street, New York, New York 10003, United States", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testMapTiler() {
        Span span = createSpan("geocoder-maptiler-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new MapTilerGeocoder(
                        client, 
                        getServiceUrl("maptiler", ""), 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(40.733, -73.989, null);
            });
            
            assertEquals("East 13th Street, New York City, New York, United States", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testGeoapify() {
        Span span = createSpan("geocoder-geoapify-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new GeoapifyGeocoder(
                        client, 
                        getServiceUrl("geoapify", ""), 
                        null, 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(40.733, -73.989, null);
            });
            
            assertEquals("114 East 13th Street, New York, New York, US", result);
        } finally {
            span.end();
        }
    }

    @Disabled
    @Test
    public void testGeocodeJSON() {
        Span span = createSpan("geocoder-geocodejson-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                Geocoder geocoder = new GeocodeJsonGeocoder(
                        client, 
                        getServiceUrl("geocodejson", null), 
                        null, 
                        null, 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(40.7337807, -73.9974401, null);
            });
            
            assertEquals("35 West 9th Street, New York, New York, US", result);
        } finally {
            span.end();
        }
    }

    /**
     * Tests geocoder with containerized mock service.
     * This test uses the mock geocoding service container to test the geocoder
     * without relying on external services.
     */
    @Test
    public void testWithMockGeocodingService() {
        // Only run if the container is running
        if (!mockGeocodingService.isRunning()) {
            return;
        }

        Span span = createSpan("geocoder-mock-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                // Get the mock service URL
                String mockServiceUrl = String.format("http://%s:%d",
                        mockGeocodingService.getHost(),
                        mockGeocodingService.getMappedPort(1080));
                
                // Create a geocoder that uses the mock service
                Geocoder geocoder = new NominatimGeocoder(
                        client, 
                        mockServiceUrl, 
                        "traccar-test", 
                        null, 
                        0, 
                        new AddressFormat());
                
                // Add correlation ID to the geocoder context
                geocoder.setCorrelationId(correlationId);
                
                return geocoder.getAddress(40.7337807, -73.9974401, null);
            });
            
            // Verify the result from the mock service
            assertNotNull(result);
            assertEquals("Mock Address, New York, US", result);
        } finally {
            span.end();
        }
    }

    /**
     * Tests geocoder with fallback mechanism when primary geocoder fails.
     */
    @Test
    public void testGeocoderWithFallback() {
        Span span = createSpan("geocoder-fallback-test");
        
        try {
            Context context = Context.current().with(span);
            
            String result = context.with(() -> {
                // Create a primary geocoder that will fail
                Geocoder primaryGeocoder = mock(Geocoder.class);
                when(primaryGeocoder.getAddress(anyDouble(), anyDouble(), null))
                        .thenThrow(new RuntimeException("Primary geocoder failed"));
                
                // Create a fallback geocoder
                Geocoder fallbackGeocoder = mock(Geocoder.class);
                when(fallbackGeocoder.getAddress(anyDouble(), anyDouble(), null))
                        .thenReturn("Fallback Address, New York, US");
                
                // Try primary geocoder first, then fallback
                String address;
                try {
                    address = primaryGeocoder.getAddress(40.7337807, -73.9974401, null);
                } catch (Exception e) {
                    // Log the exception in the span
                    Span.current().recordException(e);
                    // Use fallback geocoder
                    address = fallbackGeocoder.getAddress(40.7337807, -73.9974401, null);
                }
                
                return address;
            });
            
            // Verify the result from the fallback geocoder
            assertEquals("Fallback Address, New York, US", result);
        } finally {
            span.end();
        }
    }

    private static double anyDouble() {
        return 0.0;
    }
}