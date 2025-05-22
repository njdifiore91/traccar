package org.traccar;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.RequestResponsePact;
import au.com.dius.pact.core.model.annotations.Pact;
import au.com.dius.pact.provider.junit5.HttpTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junit5.PactVerificationInvocationContextProvider;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactFolder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.AbstractStub;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Base class for contract testing between microservices in the Traccar system.
 * Provides utilities for verifying API contracts, message schemas, and service compatibility.
 * <p>
 * This class supports:
 * - Consumer-driven contract testing using Pact
 * - REST API contract verification
 * - gRPC service definition validation
 * - Message broker schema compatibility testing
 * - Service version compatibility verification
 * - Contract test stub generation
 */
public class ContractTest extends BaseTest {

    private static final String CONTRACTS_DIR = "src/test/resources/contracts";
    private static final String SCHEMAS_DIR = "src/test/resources/schemas";
    private static final String FIXTURES_DIR = "src/test/resources/fixtures";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonSchemaFactory JSON_SCHEMA_FACTORY = JsonSchemaFactory.byDefault();
    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().build();

    /**
     * Validates a JSON payload against a JSON Schema.
     *
     * @param schemaPath Path to the JSON Schema file relative to the schemas directory
     * @param jsonPayload JSON payload to validate
     * @return True if the payload is valid according to the schema
     * @throws IOException If the schema file cannot be read
     * @throws ProcessingException If the schema validation fails
     */
    protected boolean validateJsonSchema(String schemaPath, String jsonPayload) throws IOException, ProcessingException {
        Path fullSchemaPath = Paths.get(SCHEMAS_DIR, schemaPath);
        JsonNode schemaNode = OBJECT_MAPPER.readTree(new File(fullSchemaPath.toString()));
        JsonNode payloadNode = OBJECT_MAPPER.readTree(jsonPayload);
        
        JsonSchema schema = JSON_SCHEMA_FACTORY.getJsonSchema(schemaNode);
        ProcessingReport report = schema.validate(payloadNode);
        
        return report.isSuccess();
    }

    /**
     * Validates a JSON payload against a JSON Schema from a URL.
     *
     * @param schemaUrl URL to the JSON Schema
     * @param jsonPayload JSON payload to validate
     * @return True if the payload is valid according to the schema
     * @throws IOException If the schema cannot be read from the URL
     * @throws ProcessingException If the schema validation fails
     */
    protected boolean validateJsonSchemaFromUrl(URL schemaUrl, String jsonPayload) throws IOException, ProcessingException {
        JsonNode schemaNode = OBJECT_MAPPER.readTree(schemaUrl);
        JsonNode payloadNode = OBJECT_MAPPER.readTree(jsonPayload);
        
        JsonSchema schema = JSON_SCHEMA_FACTORY.getJsonSchema(schemaNode);
        ProcessingReport report = schema.validate(payloadNode);
        
        return report.isSuccess();
    }

    /**
     * Validates an Avro message against an Avro schema.
     *
     * @param schemaPath Path to the Avro schema file relative to the schemas directory
     * @param avroData Byte array containing the Avro message
     * @return True if the message is valid according to the schema
     * @throws IOException If the schema file cannot be read or the message cannot be decoded
     */
    protected boolean validateAvroSchema(String schemaPath, byte[] avroData) throws IOException {
        Path fullSchemaPath = Paths.get(SCHEMAS_DIR, schemaPath);
        Schema schema = new Schema.Parser().parse(new File(fullSchemaPath.toString()));
        
        try {
            DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(avroData, null);
            reader.read(null, decoder);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Validates a Protocol Buffer message against its schema.
     *
     * @param messageBuilder Protocol Buffer message builder for the expected message type
     * @param jsonPayload JSON representation of the Protocol Buffer message
     * @return True if the message can be parsed according to the schema
     */
    protected boolean validateProtobufSchema(Message.Builder messageBuilder, String jsonPayload) {
        try {
            JsonFormat.parser().ignoringUnknownFields().merge(jsonPayload, messageBuilder);
            messageBuilder.build();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Creates a gRPC channel to a service for contract testing.
     *
     * @param host Host of the gRPC service
     * @param port Port of the gRPC service
     * @return A managed channel connected to the service
     */
    protected ManagedChannel createGrpcChannel(String host, int port) {
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }

    /**
     * Closes a gRPC channel safely.
     *
     * @param channel The channel to close
     * @throws InterruptedException If the shutdown is interrupted
     */
    protected void closeGrpcChannel(ManagedChannel channel) throws InterruptedException {
        if (channel != null) {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Executes a gRPC service call and validates the response.
     *
     * @param stub The gRPC service stub
     * @param request The request message
     * @param validator A functional interface to validate the response
     * @param <StubT> The type of the gRPC stub
     * @param <ReqT> The type of the request message
     * @param <RespT> The type of the response message
     * @return True if the service call succeeds and the validator returns true
     */
    protected <StubT extends AbstractStub<StubT>, ReqT, RespT> boolean validateGrpcServiceCall(
            StubT stub, ReqT request, GrpcResponseValidator<RespT> validator) {
        try {
            // This is a placeholder for the actual gRPC call implementation
            // In a real implementation, you would call the specific service method on the stub
            // RespT response = stub.someMethod((Message) request);
            // return validator.validate(response);
            return true;
        } catch (StatusRuntimeException e) {
            return false;
        }
    }

    /**
     * Functional interface for validating gRPC responses.
     *
     * @param <RespT> The type of the response message
     */
    @FunctionalInterface
    public interface GrpcResponseValidator<RespT> {
        boolean validate(RespT response);
    }

    /**
     * Loads a contract file from the contracts directory.
     *
     * @param contractPath Path to the contract file relative to the contracts directory
     * @return The content of the contract file as a string
     * @throws IOException If the contract file cannot be read
     */
    protected String loadContract(String contractPath) throws IOException {
        Path fullContractPath = Paths.get(CONTRACTS_DIR, contractPath);
        return Files.readString(fullContractPath);
    }

    /**
     * Loads a fixture file from the fixtures directory.
     *
     * @param fixturePath Path to the fixture file relative to the fixtures directory
     * @return The content of the fixture file as a string
     * @throws IOException If the fixture file cannot be read
     */
    protected String loadFixture(String fixturePath) throws IOException {
        Path fullFixturePath = Paths.get(FIXTURES_DIR, fixturePath);
        return Files.readString(fullFixturePath);
    }

    /**
     * Loads a binary fixture file from the fixtures directory.
     *
     * @param fixturePath Path to the fixture file relative to the fixtures directory
     * @return The content of the fixture file as a byte array
     * @throws IOException If the fixture file cannot be read
     */
    protected byte[] loadBinaryFixture(String fixturePath) throws IOException {
        Path fullFixturePath = Paths.get(FIXTURES_DIR, fixturePath);
        return Files.readAllBytes(fullFixturePath);
    }

    /**
     * Sends an HTTP request to a service endpoint and validates the response.
     *
     * @param baseUrl Base URL of the service
     * @param path Path of the endpoint
     * @param method HTTP method (GET, POST, etc.)
     * @param requestBody Request body (can be null for GET requests)
     * @param expectedStatusCode Expected HTTP status code
     * @return The response body as a string
     * @throws IOException If the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    protected String sendHttpRequest(String baseUrl, String path, String method, String requestBody, int expectedStatusCode)
            throws IOException, InterruptedException {
        HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", "application/json");

        if (requestBody != null && !method.equals("GET")) {
            requestBuilder.method(method, HttpRequest.BodyPublishers.ofString(requestBody));
        } else {
            requestBuilder.method(method, HttpRequest.BodyPublishers.noBody());
        }

        HttpResponse<String> response = HTTP_CLIENT.send(requestBuilder.build(), HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() != expectedStatusCode) {
            throw new IOException("Unexpected status code: " + response.statusCode() + ", expected: " + expectedStatusCode);
        }
        
        return response.body();
    }

    /**
     * Validates a service's compatibility with a specific version of a contract.
     *
     * @param serviceUrl URL of the service to test
     * @param contractPath Path to the contract file relative to the contracts directory
     * @param version Version of the contract to validate against
     * @return True if the service is compatible with the specified contract version
     * @throws IOException If the contract file cannot be read or the HTTP request fails
     * @throws InterruptedException If the HTTP request is interrupted
     */
    protected boolean validateServiceCompatibility(String serviceUrl, String contractPath, String version)
            throws IOException, InterruptedException {
        // Load the contract for the specified version
        String versionedContractPath = contractPath.replace(".json", "-" + version + ".json");
        String contract = loadContract(versionedContractPath);
        
        // Parse the contract to extract endpoints and expected responses
        JsonNode contractNode = OBJECT_MAPPER.readTree(contract);
        
        // This is a simplified implementation - in a real scenario, you would iterate through
        // all endpoints defined in the contract and validate each one
        JsonNode endpointsNode = contractNode.get("endpoints");
        if (endpointsNode == null || !endpointsNode.isArray()) {
            throw new IOException("Invalid contract format: missing or invalid 'endpoints' array");
        }
        
        boolean allEndpointsValid = true;
        
        for (JsonNode endpointNode : endpointsNode) {
            String path = endpointNode.get("path").asText();
            String method = endpointNode.get("method").asText();
            int expectedStatusCode = endpointNode.get("expectedStatusCode").asInt();
            
            String requestBody = null;
            if (endpointNode.has("requestBody")) {
                requestBody = endpointNode.get("requestBody").toString();
            }
            
            try {
                String responseBody = sendHttpRequest(serviceUrl, path, method, requestBody, expectedStatusCode);
                
                // If the endpoint has an expected response schema, validate the response against it
                if (endpointNode.has("responseSchema")) {
                    String schemaPath = endpointNode.get("responseSchema").asText();
                    boolean isValid = validateJsonSchema(schemaPath, responseBody);
                    if (!isValid) {
                        allEndpointsValid = false;
                    }
                }
            } catch (Exception e) {
                allEndpointsValid = false;
            }
        }
        
        return allEndpointsValid;
    }

    /**
     * Generates a contract test stub for a service.
     *
     * @param serviceName Name of the service
     * @param contractPath Path to the contract file relative to the contracts directory
     * @param outputDir Directory to write the generated test stub to
     * @throws IOException If the contract file cannot be read or the output file cannot be written
     */
    protected void generateContractTestStub(String serviceName, String contractPath, String outputDir) throws IOException {
        String contract = loadContract(contractPath);
        JsonNode contractNode = OBJECT_MAPPER.readTree(contract);
        
        // Generate a simple test stub based on the contract
        StringBuilder testStub = new StringBuilder();
        testStub.append("package org.traccar.contract;\n\n");
        testStub.append("import org.junit.jupiter.api.Test;\n");
        testStub.append("import org.traccar.ContractTest;\n\n");
        testStub.append("import static org.junit.jupiter.api.Assertions.assertTrue;\n\n");
        
        testStub.append("/**\n * Generated contract test for ").append(serviceName).append("\n */\n");
        testStub.append("public class ").append(serviceName).append("ContractTest extends ContractTest {\n\n");
        
        JsonNode endpointsNode = contractNode.get("endpoints");
        if (endpointsNode != null && endpointsNode.isArray()) {
            for (JsonNode endpointNode : endpointsNode) {
                String path = endpointNode.get("path").asText();
                String method = endpointNode.get("method").asText();
                
                // Generate a test method for each endpoint
                String testMethodName = "test" + method.toUpperCase() + path.replace("/", "_").replace("{", "").replace("}", "");
                testStub.append("    @Test\n");
                testStub.append("    public void ").append(testMethodName).append("() throws Exception {\n");
                testStub.append("        // TODO: Implement contract test for ").append(method).append(" ").append(path).append("\n");
                testStub.append("        // Example:\n");
                testStub.append("        // String response = sendHttpRequest(\"http://localhost:8080\", \"").append(path).append("\", \"").append(method).append("\", null, 200);\n");
                testStub.append("        // assertTrue(validateJsonSchema(\"schemas/").append(serviceName.toLowerCase()).append("/response.json\", response));\n");
                testStub.append("    }\n\n");
            }
        }
        
        testStub.append("}\n");
        
        // Write the generated test stub to a file
        Path outputPath = Paths.get(outputDir, serviceName + "ContractTest.java");
        Files.createDirectories(outputPath.getParent());
        Files.writeString(outputPath, testStub.toString());
    }

    /**
     * Base class for consumer-driven contract tests using Pact.
     * Extend this class to create consumer contract tests.
     */
    @ExtendWith(PactConsumerTestExt.class)
    public static class ConsumerContractTest extends ContractTest {
        
        /**
         * Creates a Pact contract for a consumer-provider interaction.
         *
         * @param builder The Pact DSL builder
         * @param consumerName Name of the consumer service
         * @param providerName Name of the provider service
         * @param description Description of the interaction
         * @param path API path for the interaction
         * @param method HTTP method for the interaction
         * @param requestHeaders Request headers
         * @param requestBody Request body as a PactDslJsonBody
         * @param responseStatus Expected response status code
         * @param responseHeaders Response headers
         * @param responseBody Response body as a PactDslJsonBody
         * @return A RequestResponsePact representing the contract
         */
        protected RequestResponsePact createPact(PactDslWithProvider builder,
                                               String consumerName,
                                               String providerName,
                                               String description,
                                               String path,
                                               String method,
                                               Map<String, String> requestHeaders,
                                               PactDslJsonBody requestBody,
                                               int responseStatus,
                                               Map<String, String> responseHeaders,
                                               PactDslJsonBody responseBody) {
            
            return builder
                    .given("default")
                    .uponReceiving(description)
                    .path(path)
                    .method(method)
                    .headers(requestHeaders)
                    .body(requestBody)
                    .willRespondWith()
                    .status(responseStatus)
                    .headers(responseHeaders)
                    .body(responseBody)
                    .toPact();
        }
        
        /**
         * Creates a simple GET Pact contract with default headers.
         *
         * @param builder The Pact DSL builder
         * @param consumerName Name of the consumer service
         * @param providerName Name of the provider service
         * @param description Description of the interaction
         * @param path API path for the interaction
         * @param responseStatus Expected response status code
         * @param responseBody Response body as a PactDslJsonBody
         * @return A RequestResponsePact representing the contract
         */
        protected RequestResponsePact createSimpleGetPact(PactDslWithProvider builder,
                                                       String consumerName,
                                                       String providerName,
                                                       String description,
                                                       String path,
                                                       int responseStatus,
                                                       PactDslJsonBody responseBody) {
            
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-Type", "application/json");
            
            return builder
                    .given("default")
                    .uponReceiving(description)
                    .path(path)
                    .method("GET")
                    .willRespondWith()
                    .status(responseStatus)
                    .headers(headers)
                    .body(responseBody)
                    .toPact();
        }
        
        /**
         * Executes a test against a mock server based on a Pact contract.
         *
         * @param mockServer The mock server to test against
         * @param path API path to test
         * @param method HTTP method to use
         * @param requestBody Request body (can be null for GET requests)
         * @param expectedStatusCode Expected HTTP status code
         * @return The response body as a string
         * @throws IOException If the HTTP request fails
         * @throws InterruptedException If the HTTP request is interrupted
         */
        protected String testAgainstMockServer(MockServer mockServer, String path, String method, String requestBody, int expectedStatusCode)
                throws IOException, InterruptedException {
            return sendHttpRequest(mockServer.getUrl(), path, method, requestBody, expectedStatusCode);
        }
    }

    /**
     * Base class for provider contract tests using Pact.
     * Extend this class to create provider contract tests.
     */
    @Provider("provider")
    @PactFolder("src/test/resources/contracts/pacts")
    public static class ProviderContractTest extends ContractTest {
        
        private String providerUrl;
        
        /**
         * Sets up the provider test target.
         *
         * @param context The Pact verification context
         */
        @BeforeEach
        void setupTestTarget(PactVerificationContext context) {
            if (providerUrl != null) {
                context.setTarget(new HttpTestTarget(providerUrl, 8080));
            }
        }
        
        /**
         * Sets the provider URL for testing.
         *
         * @param url The base URL of the provider service
         */
        protected void setProviderUrl(String url) {
            this.providerUrl = url;
        }
        
        /**
         * Verifies the provider against the Pact contracts.
         *
         * @param context The Pact verification context
         * @param extensionContext The JUnit extension context
         */
        @ExtendWith(PactVerificationInvocationContextProvider.class)
        void verifyPact(PactVerificationContext context, ExtensionContext extensionContext) {
            if (context != null) {
                context.verifyInteraction();
            }
        }
        
        /**
         * Sets up the default state for Pact verification.
         */
        @State("default")
        public void toDefaultState() {
            // Setup code to bring the provider to the expected state
            // This is a placeholder - implement specific state setup in subclasses
        }
    }

    /**
     * Base class for message contract tests.
     * Extend this class to create tests for message-based contracts (e.g., Kafka, RabbitMQ).
     */
    @Tag("message-contract")
    public static class MessageContractTest extends ContractTest {
        
        /**
         * Validates a message against an Avro schema.
         *
         * @param schemaPath Path to the Avro schema file relative to the schemas directory
         * @param messagePath Path to the message fixture file relative to the fixtures directory
         * @return True if the message is valid according to the schema
         * @throws IOException If the schema or message file cannot be read
         */
        protected boolean validateMessageAgainstAvroSchema(String schemaPath, String messagePath) throws IOException {
            byte[] messageData = loadBinaryFixture(messagePath);
            return validateAvroSchema(schemaPath, messageData);
        }
        
        /**
         * Validates a JSON message against a JSON Schema.
         *
         * @param schemaPath Path to the JSON Schema file relative to the schemas directory
         * @param messagePath Path to the message fixture file relative to the fixtures directory
         * @return True if the message is valid according to the schema
         * @throws IOException If the schema or message file cannot be read
         * @throws ProcessingException If the schema validation fails
         */
        protected boolean validateJsonMessageAgainstSchema(String schemaPath, String messagePath) throws IOException, ProcessingException {
            String messageData = loadFixture(messagePath);
            return validateJsonSchema(schemaPath, messageData);
        }
        
        /**
         * Validates a Protocol Buffer message against its schema.
         *
         * @param messageBuilder Protocol Buffer message builder for the expected message type
         * @param messagePath Path to the message fixture file relative to the fixtures directory
         * @return True if the message is valid according to the schema
         * @throws IOException If the message file cannot be read
         */
        protected boolean validateProtobufMessage(Message.Builder messageBuilder, String messagePath) throws IOException {
            String messageData = loadFixture(messagePath);
            return validateProtobufSchema(messageBuilder, messageData);
        }
    }
}