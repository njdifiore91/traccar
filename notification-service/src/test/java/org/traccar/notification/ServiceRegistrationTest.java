package org.traccar.notification;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.core.env.Environment;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the ServiceRegistration class.
 */
public class ServiceRegistrationTest {

    @Mock
    private Environment environment;

    @Mock
    private ServiceRegistration.ServiceDiscoveryClient discoveryClient;

    @Mock
    private ApplicationReadyEvent applicationReadyEvent;

    @Mock
    private ContextClosedEvent contextClosedEvent;

    private ServiceRegistration serviceRegistration;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(environment.getProperty(eq("service.registration.host"))).thenReturn("test-host");
        when(environment.getProperty(eq("info.app.version"), anyString())).thenReturn("1.0.0");
        when(environment.getActiveProfiles()).thenReturn(new String[]{"test"});

        serviceRegistration = new ServiceRegistration(environment, discoveryClient);

        // Set properties using reflection
        ReflectionTestUtils.setField(serviceRegistration, "serviceName", "test-service");
        ReflectionTestUtils.setField(serviceRegistration, "serverPort", 8080);
        ReflectionTestUtils.setField(serviceRegistration, "managementPort", 8081);
        ReflectionTestUtils.setField(serviceRegistration, "registrationEnabled", true);
        ReflectionTestUtils.setField(serviceRegistration, "serviceTags", "test,notification");
        ReflectionTestUtils.setField(serviceRegistration, "healthCheckPath", "/health");
        ReflectionTestUtils.setField(serviceRegistration, "healthCheckInterval", "10s");
    }

    @Test
    public void testRegisterService() {
        // When
        serviceRegistration.onApplicationEvent(applicationReadyEvent);

        // Then
        ArgumentCaptor<String> serviceIdCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> serviceNameCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> hostCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> portCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<String> healthCheckPathCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> healthCheckIntervalCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String[]> tagsCaptor = ArgumentCaptor.forClass(String[].class);
        ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);

        verify(discoveryClient).register(
                serviceIdCaptor.capture(),
                serviceNameCaptor.capture(),
                hostCaptor.capture(),
                portCaptor.capture(),
                healthCheckPathCaptor.capture(),
                healthCheckIntervalCaptor.capture(),
                tagsCaptor.capture(),
                metadataCaptor.capture());

        assertEquals("test-service", serviceNameCaptor.getValue());
        assertEquals("test-host", hostCaptor.getValue());
        assertEquals(Integer.valueOf(8080), portCaptor.getValue());
        assertEquals("/health", healthCheckPathCaptor.getValue());
        assertEquals("10s", healthCheckIntervalCaptor.getValue());

        String[] tags = tagsCaptor.getValue();
        assertEquals(2, tags.length);
        assertEquals("test", tags[0]);
        assertEquals("notification", tags[1]);

        Map<String, String> metadata = metadataCaptor.getValue();
        assertEquals("1.0.0", metadata.get("version"));
        assertEquals("test", metadata.get("environment"));
        assertEquals("8081", metadata.get("managementPort"));
        assertEquals("/health", metadata.get("healthCheckPath"));
    }

    @Test
    public void testRenewRegistration() {
        // Given
        ReflectionTestUtils.setField(serviceRegistration, "registered", true);

        // When
        serviceRegistration.renewRegistration();

        // Then
        ArgumentCaptor<String> serviceIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(discoveryClient).renew(serviceIdCaptor.capture());
    }

    @Test
    public void testDeregisterService() {
        // Given
        ReflectionTestUtils.setField(serviceRegistration, "registered", true);

        // When
        serviceRegistration.onApplicationEvent(contextClosedEvent);

        // Then
        ArgumentCaptor<String> serviceIdCaptor = ArgumentCaptor.forClass(String.class);
        verify(discoveryClient).deregister(serviceIdCaptor.capture());
    }

    @Test
    public void testRegistrationDisabled() {
        // Given
        ReflectionTestUtils.setField(serviceRegistration, "registrationEnabled", false);

        // When
        serviceRegistration.onApplicationEvent(applicationReadyEvent);

        // Then
        verify(discoveryClient, never()).register(
                anyString(), anyString(), anyString(), anyInt(),
                anyString(), anyString(), any(String[].class), anyMap());
    }

    @Test
    public void testRenewalWhenNotRegistered() {
        // Given
        ReflectionTestUtils.setField(serviceRegistration, "registered", false);

        // When
        serviceRegistration.renewRegistration();

        // Then
        verify(discoveryClient, never()).renew(anyString());
    }

    @Test
    public void testDeregistrationWhenNotRegistered() {
        // Given
        ReflectionTestUtils.setField(serviceRegistration, "registered", false);

        // When
        serviceRegistration.deregisterService();

        // Then
        verify(discoveryClient, never()).deregister(anyString());
    }

    @Test
    public void testBuildServiceMetadata() {
        // When
        serviceRegistration.onApplicationEvent(applicationReadyEvent);

        // Then
        ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(discoveryClient).register(
                anyString(), anyString(), anyString(), anyInt(),
                anyString(), anyString(), any(String[].class), metadataCaptor.capture());

        Map<String, String> metadata = metadataCaptor.getValue();
        assertEquals("1.0.0", metadata.get("version"));
        assertEquals("test", metadata.get("environment"));
        assertEquals("8081", metadata.get("managementPort"));
        assertEquals("/health", metadata.get("healthCheckPath"));
    }

    @Test
    public void testAdditionalMetadata() {
        // Given
        when(environment.getProperty(eq("service.registration.metadata"), anyString()))
                .thenReturn("key1=value1,key2=value2");

        // When
        serviceRegistration.onApplicationEvent(applicationReadyEvent);

        // Then
        ArgumentCaptor<Map<String, String>> metadataCaptor = ArgumentCaptor.forClass(Map.class);
        verify(discoveryClient).register(
                anyString(), anyString(), anyString(), anyInt(),
                anyString(), anyString(), any(String[].class), metadataCaptor.capture());

        Map<String, String> metadata = metadataCaptor.getValue();
        assertEquals("value1", metadata.get("key1"));
        assertEquals("value2", metadata.get("key2"));
    }
}