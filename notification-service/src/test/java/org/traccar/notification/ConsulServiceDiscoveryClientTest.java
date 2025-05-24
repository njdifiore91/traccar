package org.traccar.notification;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.agent.model.NewService;
import com.ecwid.consul.v1.agent.model.Service;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the ConsulServiceDiscoveryClient class.
 */
public class ConsulServiceDiscoveryClientTest {

    @Mock
    private ConsulClient consulClient;

    @Mock
    private Response<Map<String, Service>> servicesResponse;

    private ConsulServiceDiscoveryClient discoveryClient;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        discoveryClient = new ConsulServiceDiscoveryClient(consulClient);
        ReflectionTestUtils.setField(discoveryClient, "consulHost", "localhost");
        ReflectionTestUtils.setField(discoveryClient, "consulPort", 8500);

        Map<String, Service> services = new HashMap<>();
        Service service = new Service();
        service.setId("test-service-id");
        service.setService("test-service");
        services.put("test-service-id", service);

        when(consulClient.getAgentServices()).thenReturn(servicesResponse);
        when(servicesResponse.getValue()).thenReturn(services);
    }

    @Test
    public void testRegister() {
        // Given
        String serviceId = "test-service-id";
        String serviceName = "test-service";
        String host = "test-host";
        int port = 8080;
        String healthCheckPath = "/health";
        String healthCheckInterval = "10s";
        String[] tags = {"tag1", "tag2"};
        Map<String, String> metadata = new HashMap<>();
        metadata.put("version", "1.0.0");
        metadata.put("environment", "test");

        // When
        discoveryClient.register(serviceId, serviceName, host, port, healthCheckPath, healthCheckInterval, tags, metadata);

        // Then
        ArgumentCaptor<NewService> serviceCaptor = ArgumentCaptor.forClass(NewService.class);
        verify(consulClient).agentServiceRegister(serviceCaptor.capture());

        NewService capturedService = serviceCaptor.getValue();
        assertEquals(serviceId, capturedService.getId());
        assertEquals(serviceName, capturedService.getName());
        assertEquals(host, capturedService.getAddress());
        assertEquals(port, capturedService.getPort());
        assertEquals(2, capturedService.getTags().size());
        assertEquals("tag1", capturedService.getTags().get(0));
        assertEquals("tag2", capturedService.getTags().get(1));
        assertEquals(metadata, capturedService.getMeta());

        NewService.Check check = capturedService.getCheck();
        assertNotNull(check);
        assertEquals("http://test-host:8080/health", check.getHttp());
        assertEquals(healthCheckInterval, check.getInterval());
        assertEquals("90m", check.getDeregisterCriticalServiceAfter());
    }

    @Test
    public void testRegisterWithSecureFlag() {
        // Given
        String serviceId = "test-service-id";
        String serviceName = "test-service";
        String host = "test-host";
        int port = 8443;
        String healthCheckPath = "/health";
        String healthCheckInterval = "10s";
        String[] tags = {"tag1", "tag2"};
        Map<String, String> metadata = new HashMap<>();
        metadata.put("version", "1.0.0");
        metadata.put("environment", "test");
        metadata.put("secure", "true");

        // When
        discoveryClient.register(serviceId, serviceName, host, port, healthCheckPath, healthCheckInterval, tags, metadata);

        // Then
        ArgumentCaptor<NewService> serviceCaptor = ArgumentCaptor.forClass(NewService.class);
        verify(consulClient).agentServiceRegister(serviceCaptor.capture());

        NewService capturedService = serviceCaptor.getValue();
        NewService.Check check = capturedService.getCheck();
        assertNotNull(check);
        assertEquals("https://test-host:8443/health", check.getHttp());
    }

    @Test
    public void testRenew() {
        // Given
        String serviceId = "test-service-id";

        // When
        discoveryClient.renew(serviceId);

        // Then
        verify(consulClient).getAgentServices();
    }

    @Test
    public void testDeregister() {
        // Given
        String serviceId = "test-service-id";

        // When
        discoveryClient.deregister(serviceId);

        // Then
        verify(consulClient).agentServiceDeregister(serviceId);
    }
}