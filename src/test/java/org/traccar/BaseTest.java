package org.traccar;

import org.junit.jupiter.api.BeforeEach;
import org.traccar.config.Config;
import org.traccar.config.Keys;

import java.util.Properties;

/**
 * Base class for tests.
 * This class has been updated to support both monolithic and microservices testing environments.
 */
public class BaseTest {

    protected Properties properties = new Properties();
    protected Config config = new Config(properties);

    @BeforeEach
    public void beforeTest() {
        properties.setProperty(Keys.DATABASE_DRIVER.getKey(), "org.h2.Driver");
        properties.setProperty(Keys.DATABASE_URL.getKey(), "jdbc:h2:mem:");
        properties.setProperty(Keys.DATABASE_USER.getKey(), "sa");
        properties.setProperty(Keys.DATABASE_PASSWORD.getKey(), "");
    }

    /**
     * Injects a protocol decoder with dependencies.
     * This method is used for testing protocol decoders in a monolithic environment.
     *
     * @param decoder the protocol decoder to inject
     * @param <T>     the type of the protocol decoder
     * @return the injected protocol decoder
     */
    protected <T> T inject(T object) {
        return object;
    }

    /**
     * Determines if the test is running in a microservices environment.
     *
     * @return true if running in a microservices environment, false otherwise
     */
    protected boolean isMicroservicesEnvironment() {
        return "microservices".equals(System.getProperty("test.environment"));
    }

    /**
     * Determines if the test is running in an integration testing environment.
     *
     * @return true if running in an integration testing environment, false otherwise
     */
    protected boolean isIntegrationTestingEnvironment() {
        return "true".equals(System.getProperty("test.integration"));
    }
}