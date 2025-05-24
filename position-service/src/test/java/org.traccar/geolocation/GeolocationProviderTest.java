package org.traccar.geolocation;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.traccar.common.test.BaseServiceTest;
import org.traccar.model.CellTower;
import org.traccar.model.Network;
import org.traccar.service.discovery.ServiceDiscovery;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@Testcontainers
@ExtendWith(MockitoExtension.class)
public class GeolocationProviderTest extends BaseServiceTest {

    @Mock
    private Client client;

    @Mock
    private WebTarget webTarget;

    @Mock
    private Invocation.Builder builder;

    @Mock
    private Response response;

    @Mock
    private ServiceDiscovery serviceDiscovery;

    private MeterRegistry meterRegistry;
    private CircuitBreakerRegistry circuitBreakerRegistry;
    private CircuitBreaker circuitBreaker;
    private GeolocationProvider provider;

    @Container
    private static final GenericContainer<?> wiremockContainer = new GenericContainer<>(DockerImageName.parse("wiremock/wiremock:2.35.0"))
            .withExposedPorts(8080)
            .waitingFor(Wait.forHttp("/__admin/mappings").forStatusCode(200))
            .withCommand("--global-response-templating")
            .withEnv("WIREMOCK_OPTIONS", "--verbose");

    @BeforeEach
    void setUp() {
        // Setup Micrometer registry for metrics collection
        meterRegistry = new SimpleMeterRegistry();
        
        // Setup Circuit Breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMillis(1000))
                .permittedNumberOfCallsInHalfOpenState(2)
                .slidingWindowSize(10)
                .build();
        
        circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig);
        circuitBreaker = circuitBreakerRegistry.circuitBreaker("geolocation");
        
        // Setup service discovery mock
        when(serviceDiscovery.getServiceUrl(anyString())).thenReturn("http://localhost:" + wiremockContainer.getMappedPort(8080));
        
        // Setup HTTP client mocks
        when(client.target(anyString())).thenReturn(webTarget);
        when(webTarget.request(MediaType.APPLICATION_JSON)).thenReturn(builder);
        when(builder.post(any())).thenReturn(response);
        
        // Create provider with mocked dependencies
        provider = new GoogleGeolocationProvider(client, "test-api-key", circuitBreaker, meterRegistry, serviceDiscovery);
    }

    @AfterEach
    void tearDown() {
        // Reset circuit breaker state
        circuitBreaker.reset();
    }

    @Test
    void testSuccessfulGeolocation() throws Exception {
        // Setup mock response
        when(response.getStatus()).thenReturn(200);
        when(response.readEntity(String.class)).thenReturn("{\"location\": {\"lat\": 60.07254, \"lng\": 30.30996}, \"accuracy\": 10.0}");
        
        // Create test network data
        Network network = new Network(CellTower.from(208, 1, 2, 1234567));
        
        // Use CountDownLatch to wait for async callback instead of Thread.sleep(Long.MAX_VALUE)
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Boolean> testResult = new CompletableFuture<>();
        
        // Call the provider
        provider.getLocation(network, new GeolocationProvider.LocationProviderCallback() {
            @Override
            public void onSuccess(double latitude, double longitude, double accuracy) {
                try {
                    assertEquals(60.07254, latitude, 0.00001);
                    assertEquals(30.30996, longitude, 0.00001);
                    assertEquals(10.0, accuracy, 0.00001);
                    testResult.complete(true);
                } catch (AssertionError e) {
                    testResult.completeExceptionally(e);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Throwable e) {
                testResult.completeExceptionally(e);
                latch.countDown();
            }
        });
        
        // Wait for callback with timeout
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback was not called within timeout");
        assertTrue(testResult.get(), "Test assertions failed");
        
        // Verify metrics were recorded
        assertNotNull(meterRegistry.find("geolocation.requests").counter());
        assertNotNull(meterRegistry.find("geolocation.success").counter());
        
        // Verify service discovery was used
        verify(serviceDiscovery).getServiceUrl(anyString());
    }

    @Test
    void testGeolocationFailure() throws Exception {
        // Setup mock response for failure
        when(response.getStatus()).thenReturn(400);
        when(response.readEntity(String.class)).thenReturn("{\"error\": {\"code\": 400, \"message\": \"Bad request\"}}");
        
        // Create test network data
        Network network = new Network(CellTower.from(208, 1, 2, 1234567));
        
        // Use CountDownLatch to wait for async callback
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Boolean> testResult = new CompletableFuture<>();
        
        // Call the provider
        provider.getLocation(network, new GeolocationProvider.LocationProviderCallback() {
            @Override
            public void onSuccess(double latitude, double longitude, double accuracy) {
                testResult.completeExceptionally(new AssertionError("Expected failure but got success"));
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable e) {
                try {
                    assertNotNull(e);
                    assertTrue(e instanceof GeolocationException);
                    testResult.complete(true);
                } catch (AssertionError assertionError) {
                    testResult.completeExceptionally(assertionError);
                } finally {
                    latch.countDown();
                }
            }
        });
        
        // Wait for callback with timeout
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Callback was not called within timeout");
        assertTrue(testResult.get(), "Test assertions failed");
        
        // Verify metrics were recorded
        assertNotNull(meterRegistry.find("geolocation.requests").counter());
        assertNotNull(meterRegistry.find("geolocation.errors").counter());
    }

    @Test
    void testCircuitBreakerTrip() throws Exception {
        // Setup mock response for failure
        when(response.getStatus()).thenReturn(500);
        when(response.readEntity(String.class)).thenReturn("{\"error\": {\"code\": 500, \"message\": \"Internal server error\"}}");
        
        Network network = new Network(CellTower.from(208, 1, 2, 1234567));
        CountDownLatch latch = new CountDownLatch(1);
        
        // Force circuit breaker to open by making multiple failing calls
        for (int i = 0; i < 10; i++) {
            provider.getLocation(network, new GeolocationProvider.LocationProviderCallback() {
                @Override
                public void onSuccess(double latitude, double longitude, double accuracy) {
                    // Not expected
                }

                @Override
                public void onFailure(Throwable e) {
                    // Expected
                    if (i == 9) {
                        latch.countDown();
                    }
                }
            });
        }
        
        // Wait for all calls to complete
        assertTrue(latch.await(5, TimeUnit.SECONDS), "Callbacks were not called within timeout");
        
        // Verify circuit breaker is open
        assertEquals(CircuitBreaker.State.OPEN, circuitBreaker.getState());
        
        // Verify circuit breaker metrics
        assertNotNull(meterRegistry.find("resilience4j.circuitbreaker.state").gauge());
        assertNotNull(meterRegistry.find("resilience4j.circuitbreaker.calls").counter());
        assertNotNull(meterRegistry.find("resilience4j.circuitbreaker.failure.rate").gauge());
    }

    @Test
    void testServiceDiscoveryIntegration() {
        // Setup mock response
        when(response.getStatus()).thenReturn(200);
        when(response.readEntity(String.class)).thenReturn("{\"location\": {\"lat\": 60.07254, \"lng\": 30.30996}, \"accuracy\": 10.0}");
        
        // Create test network data
        Network network = new Network(CellTower.from(208, 1, 2, 1234567));
        
        // Call the provider
        provider.getLocation(network, new GeolocationProvider.LocationProviderCallback() {
            @Override
            public void onSuccess(double latitude, double longitude, double accuracy) {
                // Success expected
            }

            @Override
            public void onFailure(Throwable e) {
                fail("Should not fail: " + e.getMessage());
            }
        });
        
        // Verify service discovery was used to get the service URL
        verify(serviceDiscovery).getServiceUrl("geolocation-service");
    }
}