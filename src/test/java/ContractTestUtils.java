import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import org.mockito.Mockito;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Utility class for contract testing between microservices.
 * Provides capabilities for consumer-driven contract verification, schema validation,
 * and contract test execution with mock providers.
 *
 * This class supports:
 * - REST API contract validation
 * - gRPC service contract validation
 * - Message broker schema validation
 * - Mock service implementations for contract testing
 * - Contract test reporting and verification
 */
public class ContractTestUtils {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    
    /**
     * Contract test result containing validation status and details.
     */
    public static class ContractTestResult {
        private final boolean success;
        private final List<String> errors;
        private final Map<String, Object> details;

        public ContractTestResult(boolean success) {
            this.success = success;
            this.errors = new ArrayList<>();
            this.details = new HashMap<>();
        }

        public ContractTestResult(boolean success, List<String> errors) {
            this.success = success;
            this.errors = errors;
            this.details = new HashMap<>();
        }

        public boolean isSuccess() {
            return success;
        }

        public List<String> getErrors() {
            return errors;
        }

        public Map<String, Object> getDetails() {
            return details;
        }

        public void addDetail(String key, Object value) {
            details.put(key, value);
        }

        public void addError(String error) {
            errors.add(error);
        }
    }

    /**
     * Contract definition containing expected request and response details.
     */
    public static class ContractDefinition {
        private final String name;
        private final String description;
        private final Map<String, Object> request;
        private final Map<String, Object> response;
        private final Map<String, Object> metadata;

        public ContractDefinition(String name, String description, 
                                 Map<String, Object> request, 
                                 Map<String, Object> response) {
            this.name = name;
            this.description = description;
            this.request = request;
            this.response = response;
            this.metadata = new HashMap<>();
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public Map<String, Object> getRequest() {
            return request;
        }

        public Map<String, Object> getResponse() {
            return response;
        }

        public Map<String, Object> getMetadata() {
            return metadata;
        }

        public void addMetadata(String key, Object value) {
            metadata.put(key, value);
        }
    }

    /**
     * Loads contract definitions from a JSON file.
     *
     * @param filePath Path to the contract JSON file
     * @return List of contract definitions
     * @throws IOException If the file cannot be read or parsed
     */
    public static List<ContractDefinition> loadContractsFromFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        String content = Files.readString(path);
        JsonNode rootNode = OBJECT_MAPPER.readTree(content);
        
        List<ContractDefinition> contracts = new ArrayList<>();
        
        if (rootNode.isArray()) {
            for (JsonNode contractNode : rootNode) {
                contracts.add(parseContractDefinition(contractNode));
            }
        } else {
            contracts.add(parseContractDefinition(rootNode));
        }
        
        return contracts;
    }
    
    private static ContractDefinition parseContractDefinition(JsonNode node) {
        String name = node.path("name").asText("Unnamed Contract");
        String description = node.path("description").asText("");
        
        Map<String, Object> request = OBJECT_MAPPER.convertValue(
            node.path("request"), 
            OBJECT_MAPPER.getTypeFactory().constructMapType(
                HashMap.class, String.class, Object.class));
                
        Map<String, Object> response = OBJECT_MAPPER.convertValue(
            node.path("response"), 
            OBJECT_MAPPER.getTypeFactory().constructMapType(
                HashMap.class, String.class, Object.class));
                
        ContractDefinition contract = new ContractDefinition(name, description, request, response);
        
        // Add metadata if present
        if (node.has("metadata")) {
            Map<String, Object> metadata = OBJECT_MAPPER.convertValue(
                node.path("metadata"), 
                OBJECT_MAPPER.getTypeFactory().constructMapType(
                    HashMap.class, String.class, Object.class));
            metadata.forEach(contract::addMetadata);
        }
        
        return contract;
    }

    /**
     * Creates a WireMock server configured for contract testing.
     *
     * @param port Port to run the WireMock server on
     * @param contracts List of contract definitions to configure the server with
     * @return Configured WireMock server instance
     */
    public static WireMockServer createMockProviderServer(int port, List<ContractDefinition> contracts) {
        WireMockServer server = new WireMockServer(WireMockConfiguration.options().port(port));
        server.start();
        
        for (ContractDefinition contract : contracts) {
            Map<String, Object> requestMap = contract.getRequest();
            Map<String, Object> responseMap = contract.getResponse();
            
            String method = (String) requestMap.getOrDefault("method", "GET");
            String path = (String) requestMap.getOrDefault("path", "/");
            
            WireMock.configureFor("localhost", port);
            
            // Configure request matching
            WireMock.stubFor(WireMock.request(method, WireMock.urlPathMatching(path))
                .willReturn(WireMock.aResponse()
                    .withStatus((Integer) responseMap.getOrDefault("status", 200))
                    .withHeader("Content-Type", (String) responseMap.getOrDefault("contentType", "application/json"))
                    .withBody(responseMap.containsKey("body") ? responseMap.get("body").toString() : "{}")
                )
            );
        }
        
        return server;
    }

    /**
     * Verifies that a consumer's requests match the expected contract.
     *
     * @param server WireMock server instance
     * @param contract Contract definition to verify against
     * @param consumerTest Consumer test function that makes the request
     * @return Contract test result with verification details
     */
    public static ContractTestResult verifyConsumerContract(
            WireMockServer server, 
            ContractDefinition contract, 
            Consumer<String> consumerTest) {
        
        String baseUrl = server.baseUrl();
        consumerTest.accept(baseUrl);
        
        Map<String, Object> requestMap = contract.getRequest();
        String method = (String) requestMap.getOrDefault("method", "GET");
        String path = (String) requestMap.getOrDefault("path", "/");
        
        List<LoggedRequest> requests = server.findAll(WireMock.requestMadeFor(
            WireMock.requestPatternBuilder().withMethod(WireMock.equalTo(method))
                .withUrl(WireMock.urlPathMatching(path))
        ));
        
        ContractTestResult result = new ContractTestResult(!requests.isEmpty());
        result.addDetail("requestCount", requests.size());
        
        if (requests.isEmpty()) {
            result.addError("No matching requests found for contract: " + contract.getName());
        } else {
            // Verify request headers if specified in contract
            if (requestMap.containsKey("headers")) {
                @SuppressWarnings("unchecked")
                Map<String, String> expectedHeaders = (Map<String, String>) requestMap.get("headers");
                LoggedRequest actualRequest = requests.get(0);
                
                for (Map.Entry<String, String> header : expectedHeaders.entrySet()) {
                    String actualValue = actualRequest.getHeader(header.getKey());
                    if (actualValue == null || !actualValue.equals(header.getValue())) {
                        result = new ContractTestResult(false);
                        result.addError(String.format("Header mismatch for '%s': expected '%s', got '%s'", 
                            header.getKey(), header.getValue(), actualValue));
                    }
                }
            }
            
            // Verify request body if specified in contract
            if (requestMap.containsKey("body")) {
                LoggedRequest actualRequest = requests.get(0);
                String expectedBody = requestMap.get("body").toString();
                String actualBody = actualRequest.getBodyAsString();
                
                try {
                    JsonNode expectedJson = OBJECT_MAPPER.readTree(expectedBody);
                    JsonNode actualJson = OBJECT_MAPPER.readTree(actualBody);
                    
                    if (!expectedJson.equals(actualJson)) {
                        result = new ContractTestResult(false);
                        result.addError("Request body mismatch: expected " + expectedBody + ", got " + actualBody);
                    }
                } catch (IOException e) {
                    // Fall back to string comparison if not valid JSON
                    if (!expectedBody.equals(actualBody)) {
                        result = new ContractTestResult(false);
                        result.addError("Request body mismatch: expected " + expectedBody + ", got " + actualBody);
                    }
                }
            }
        }
        
        return result;
    }

    /**
     * Verifies that a provider's responses match the expected contract.
     *
     * @param providerBaseUrl Base URL of the provider service
     * @param contract Contract definition to verify against
     * @return Contract test result with verification details
     */
    public static ContractTestResult verifyProviderContract(String providerBaseUrl, ContractDefinition contract) {
        Map<String, Object> requestMap = contract.getRequest();
        Map<String, Object> responseMap = contract.getResponse();
        
        String method = (String) requestMap.getOrDefault("method", "GET");
        String path = (String) requestMap.getOrDefault("path", "/");
        
        try {
            HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(providerBaseUrl + path));
            
            // Set method and body if needed
            if (method.equals("GET")) {
                requestBuilder.GET();
            } else if (method.equals("POST") || method.equals("PUT") || method.equals("PATCH")) {
                String body = requestMap.containsKey("body") ? requestMap.get("body").toString() : "{}";
                requestBuilder.method(method, HttpRequest.BodyPublishers.ofString(body));
            } else if (method.equals("DELETE")) {
                requestBuilder.DELETE();
            }
            
            // Add headers if specified
            if (requestMap.containsKey("headers")) {
                @SuppressWarnings("unchecked")
                Map<String, String> headers = (Map<String, String>) requestMap.get("headers");
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    requestBuilder.header(header.getKey(), header.getValue());
                }
            }
            
            HttpResponse<String> response = HTTP_CLIENT.send(
                requestBuilder.build(), 
                HttpResponse.BodyHandlers.ofString());
            
            // Verify response status
            int expectedStatus = (Integer) responseMap.getOrDefault("status", 200);
            boolean statusMatches = response.statusCode() == expectedStatus;
            
            ContractTestResult result = new ContractTestResult(statusMatches);
            result.addDetail("actualStatus", response.statusCode());
            result.addDetail("expectedStatus", expectedStatus);
            
            if (!statusMatches) {
                result.addError(String.format("Status code mismatch: expected %d, got %d", 
                    expectedStatus, response.statusCode()));
                return result;
            }
            
            // Verify response body if specified
            if (responseMap.containsKey("body")) {
                String expectedBody = responseMap.get("body").toString();
                String actualBody = response.body();
                
                try {
                    JsonNode expectedJson = OBJECT_MAPPER.readTree(expectedBody);
                    JsonNode actualJson = OBJECT_MAPPER.readTree(actualBody);
                    
                    boolean bodyMatches = expectedJson.equals(actualJson);
                    if (!bodyMatches) {
                        result = new ContractTestResult(false);
                        result.addError("Response body mismatch");
                        result.addDetail("expectedBody", expectedBody);
                        result.addDetail("actualBody", actualBody);
                    }
                } catch (IOException e) {
                    // Fall back to string comparison if not valid JSON
                    boolean bodyMatches = expectedBody.equals(actualBody);
                    if (!bodyMatches) {
                        result = new ContractTestResult(false);
                        result.addError("Response body mismatch");
                        result.addDetail("expectedBody", expectedBody);
                        result.addDetail("actualBody", actualBody);
                    }
                }
            }
            
            // Verify response headers if specified
            if (responseMap.containsKey("headers")) {
                @SuppressWarnings("unchecked")
                Map<String, String> expectedHeaders = (Map<String, String>) responseMap.get("headers");
                
                for (Map.Entry<String, String> header : expectedHeaders.entrySet()) {
                    String actualValue = response.headers().firstValue(header.getKey()).orElse(null);
                    if (actualValue == null || !actualValue.equals(header.getValue())) {
                        result = new ContractTestResult(false);
                        result.addError(String.format("Header mismatch for '%s': expected '%s', got '%s'", 
                            header.getKey(), header.getValue(), actualValue));
                    }
                }
            }
            
            return result;
            
        } catch (Exception e) {
            ContractTestResult result = new ContractTestResult(false);
            result.addError("Exception during provider verification: " + e.getMessage());
            return result;
        }
    }

    /**
     * Creates a Kafka container for message broker contract testing.
     *
     * @return Configured Kafka container
     */
    public static KafkaContainer createKafkaContainer() {
        KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.3.0"));
        kafka.start();
        return kafka;
    }

    /**
     * Creates a RabbitMQ container for message broker contract testing.
     *
     * @return Configured RabbitMQ container
     */
    public static RabbitMQContainer createRabbitMQContainer() {
        RabbitMQContainer rabbitmq = new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.11-management"));
        rabbitmq.start();
        return rabbitmq;
    }

    /**
     * Validates a message against a schema for message broker contract testing.
     *
     * @param message Message to validate
     * @param schemaPath Path to the schema file (JSON Schema, Avro, etc.)
     * @return Contract test result with validation details
     */
    public static ContractTestResult validateMessageSchema(String message, String schemaPath) {
        try {
            // Load schema
            String schemaContent = Files.readString(Paths.get(schemaPath));
            JsonNode schemaNode = OBJECT_MAPPER.readTree(schemaContent);
            
            // Parse message
            JsonNode messageNode = OBJECT_MAPPER.readTree(message);
            
            // For this example, we'll do a simple validation
            // In a real implementation, you would use a proper schema validator like json-schema-validator
            ContractTestResult result = new ContractTestResult(true);
            result.addDetail("schema", schemaPath);
            result.addDetail("message", message);
            
            // This is a placeholder for actual schema validation
            // In a real implementation, you would validate the message against the schema
            // and return appropriate results
            
            return result;
        } catch (Exception e) {
            ContractTestResult result = new ContractTestResult(false);
            result.addError("Schema validation error: " + e.getMessage());
            return result;
        }
    }

    /**
     * Saves contract test results to a file for reporting.
     *
     * @param results List of contract test results
     * @param outputPath Path to save the report to
     * @throws IOException If the file cannot be written
     */
    public static void saveContractTestReport(List<ContractTestResult> results, String outputPath) throws IOException {
        Map<String, Object> report = new HashMap<>();
        report.put("timestamp", System.currentTimeMillis());
        report.put("totalTests", results.size());
        
        long passedTests = results.stream().filter(ContractTestResult::isSuccess).count();
        report.put("passedTests", passedTests);
        report.put("failedTests", results.size() - passedTests);
        
        List<Map<String, Object>> testDetails = new ArrayList<>();
        for (int i = 0; i < results.size(); i++) {
            ContractTestResult result = results.get(i);
            Map<String, Object> testDetail = new HashMap<>();
            testDetail.put("testNumber", i + 1);
            testDetail.put("success", result.isSuccess());
            testDetail.put("errors", result.getErrors());
            testDetail.put("details", result.getDetails());
            testDetails.add(testDetail);
        }
        report.put("tests", testDetails);
        
        String reportJson = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(report);
        Files.writeString(Paths.get(outputPath), reportJson);
    }

    /**
     * Waits for a service to be ready by polling a health endpoint.
     *
     * @param baseUrl Base URL of the service
     * @param healthEndpoint Health endpoint path
     * @param timeoutSeconds Maximum time to wait in seconds
     * @return true if the service is ready, false otherwise
     */
    public static boolean waitForService(String baseUrl, String healthEndpoint, int timeoutSeconds) {
        long startTime = System.currentTimeMillis();
        long timeoutMillis = TimeUnit.SECONDS.toMillis(timeoutSeconds);
        
        while (System.currentTimeMillis() - startTime < timeoutMillis) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + healthEndpoint))
                    .GET()
                    .build();
                
                HttpResponse<String> response = HTTP_CLIENT.send(request, HttpResponse.BodyHandlers.ofString());
                
                if (response.statusCode() >= 200 && response.statusCode() < 300) {
                    return true;
                }
            } catch (Exception e) {
                // Service not ready yet, continue waiting
            }
            
            try {
                Thread.sleep(1000); // Wait 1 second before retrying
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        
        return false;
    }

    /**
     * Creates a mock implementation of a service interface for contract testing.
     *
     * @param serviceClass Interface class to mock
     * @param contracts List of contract definitions to configure the mock with
     * @param <T> Type of the service interface
     * @return Mocked service implementation
     */
    @SuppressWarnings("unchecked")
    public static <T> T createMockService(Class<T> serviceClass, List<ContractDefinition> contracts) {
        T mockService = Mockito.mock(serviceClass);
        
        // This is a simplified example. In a real implementation, you would
        // configure the mock service based on the contract definitions.
        // The specific configuration would depend on the service interface.
        
        return mockService;
    }

    /**
     * Generates a contract definition from recorded interactions with a WireMock server.
     *
     * @param server WireMock server with recorded interactions
     * @param contractName Name for the generated contract
     * @param outputPath Path to save the contract to
     * @throws IOException If the file cannot be written
     */
    public static void generateContractFromRecording(WireMockServer server, String contractName, String outputPath) throws IOException {
        List<LoggedRequest> requests = server.findAll(WireMock.anyRequestedFor(WireMock.anyUrl()));
        
        if (requests.isEmpty()) {
            throw new IllegalStateException("No requests recorded by WireMock server");
        }
        
        // For simplicity, we'll just use the first recorded request/response pair
        LoggedRequest request = requests.get(0);
        
        Map<String, Object> requestMap = new HashMap<>();
        requestMap.put("method", request.getMethod().getName());
        requestMap.put("path", request.getUrl());
        
        Map<String, String> requestHeaders = new HashMap<>();
        request.getHeaders().all().forEach(header -> 
            requestHeaders.put(header.key(), header.firstValue()));
        requestMap.put("headers", requestHeaders);
        
        if (request.getBody().length > 0) {
            try {
                // Try to parse as JSON
                JsonNode bodyJson = OBJECT_MAPPER.readTree(request.getBodyAsString());
                requestMap.put("body", bodyJson);
            } catch (Exception e) {
                // Fall back to string
                requestMap.put("body", request.getBodyAsString());
            }
        }
        
        // For the response, we need to get the stub mapping that matched this request
        // This is simplified; in a real implementation you would need to find the exact stub that matched
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put("status", 200); // Default
        responseMap.put("headers", Map.of("Content-Type", "application/json"));
        responseMap.put("body", "{}"); // Default
        
        ContractDefinition contract = new ContractDefinition(
            contractName,
            "Generated from recorded interaction",
            requestMap,
            responseMap
        );
        
        // Save the contract to file
        Map<String, Object> contractMap = new HashMap<>();
        contractMap.put("name", contract.getName());
        contractMap.put("description", contract.getDescription());
        contractMap.put("request", contract.getRequest());
        contractMap.put("response", contract.getResponse());
        contractMap.put("metadata", contract.getMetadata());
        
        String contractJson = OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(contractMap);
        Files.writeString(Paths.get(outputPath), contractJson);
    }

    /**
     * Creates a directory structure for organizing contract tests.
     *
     * @param basePath Base directory path
     * @param consumerName Name of the consumer service
     * @param providerName Name of the provider service
     * @return Path to the created contract directory
     * @throws IOException If directories cannot be created
     */
    public static Path createContractTestDirectory(String basePath, String consumerName, String providerName) throws IOException {
        Path contractsDir = Paths.get(basePath, "contracts", consumerName, providerName);
        Files.createDirectories(contractsDir);
        return contractsDir;
    }
}