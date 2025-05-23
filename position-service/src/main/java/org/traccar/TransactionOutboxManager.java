package org.traccar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Implements the transaction outbox pattern for reliable message publishing.
 * <p>
 * The transaction outbox pattern ensures that messages are published to the broker
 * even in the event of service failures by storing them in a database table as part
 * of the same transaction that updates business data. A separate process then reads
 * from this table and publishes the messages to the broker.
 * <p>
 * This implementation provides:
 * - Atomic database operations with message storage
 * - Scheduled processing of outbox messages
 * - Retry logic for failed publications
 * - Message ordering based on creation time
 */
@Component
public class TransactionOutboxManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionOutboxManager.class);

    private final DataSource dataSource;
    private final MessagePublisher messagePublisher;

    /**
     * Constructs a new TransactionOutboxManager.
     *
     * @param dataSource       the data source to use for database operations
     * @param messagePublisher the publisher to use for sending messages to the broker
     */
    @Autowired
    public TransactionOutboxManager(DataSource dataSource, MessagePublisher messagePublisher) {
        this.dataSource = dataSource;
        this.messagePublisher = messagePublisher;
        initializeOutboxTable();
    }

    /**
     * Initializes the outbox table if it doesn't exist.
     */
    private void initializeOutboxTable() {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(
                "CREATE TABLE IF NOT EXISTS outbox (" +
                "id VARCHAR(36) PRIMARY KEY, " +
                "topic VARCHAR(255) NOT NULL, " +
                "key VARCHAR(255), " +
                "payload TEXT NOT NULL, " +
                "status VARCHAR(20) NOT NULL, " +
                "created_at TIMESTAMP NOT NULL, " +
                "published_at TIMESTAMP, " +
                "retry_count INT DEFAULT 0, " +
                "last_error TEXT, " +
                "version INT DEFAULT 0" +
                ")");
            LOGGER.info("Outbox table initialized");
        } catch (SQLException e) {
            LOGGER.error("Failed to initialize outbox table", e);
            throw new RuntimeException("Failed to initialize outbox table", e);
        }
    }

    /**
     * Stores a message in the outbox table as part of the current transaction.
     * This method should be called within a transactional context.
     *
     * @param topic   the topic to publish the message to
     * @param key     the key for the message (can be null)
     * @param payload the message payload
     * @return the ID of the stored message
     */
    @Transactional
    public String storeMessage(String topic, String key, String payload) {
        String id = UUID.randomUUID().toString();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                 "INSERT INTO outbox (id, topic, key, payload, status, created_at) " +
                 "VALUES (?, ?, ?, ?, ?, ?)")) {
            statement.setString(1, id);
            statement.setString(2, topic);
            statement.setString(3, key);
            statement.setString(4, payload);
            statement.setString(5, "PENDING");
            statement.setTimestamp(6, Timestamp.from(Instant.now()));
            statement.executeUpdate();
            LOGGER.debug("Stored message in outbox: id={}, topic={}", id, topic);
            return id;
        } catch (SQLException e) {
            LOGGER.error("Failed to store message in outbox", e);
            throw new RuntimeException("Failed to store message in outbox", e);
        }
    }

    /**
     * Processes pending messages in the outbox table.
     * This method is scheduled to run at a fixed rate.
     */
    @Scheduled(fixedDelayString = "${outbox.processing.interval:5000}")
    public void processOutbox() {
        LOGGER.debug("Processing outbox messages");
        List<OutboxMessage> messages = fetchPendingMessages();
        if (messages.isEmpty()) {
            LOGGER.debug("No pending messages found in outbox");
            return;
        }

        LOGGER.info("Found {} pending messages in outbox", messages.size());
        for (OutboxMessage message : messages) {
            processMessage(message);
        }
    }

    /**
     * Fetches pending messages from the outbox table.
     *
     * @return a list of pending outbox messages
     */
    private List<OutboxMessage> fetchPendingMessages() {
        List<OutboxMessage> messages = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                 "SELECT id, topic, key, payload, retry_count, version " +
                 "FROM outbox " +
                 "WHERE status = 'PENDING' " +
                 "ORDER BY created_at ASC " +
                 "LIMIT 100")) {
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    OutboxMessage message = new OutboxMessage(
                        resultSet.getString("id"),
                        resultSet.getString("topic"),
                        resultSet.getString("key"),
                        resultSet.getString("payload"),
                        resultSet.getInt("retry_count"),
                        resultSet.getInt("version")
                    );
                    messages.add(message);
                }
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to fetch pending messages from outbox", e);
        }
        return messages;
    }

    /**
     * Processes a single outbox message.
     *
     * @param message the message to process
     */
    private void processMessage(OutboxMessage message) {
        try {
            LOGGER.debug("Processing outbox message: id={}, topic={}", message.id(), message.topic());
            messagePublisher.publish(message.topic(), message.key(), message.payload());
            markMessageAsPublished(message.id(), message.version());
            LOGGER.info("Successfully published message from outbox: id={}, topic={}", message.id(), message.topic());
        } catch (Exception e) {
            LOGGER.warn("Failed to publish message from outbox: id={}, topic={}, error={}", 
                message.id(), message.topic(), e.getMessage());
            incrementRetryCount(message.id(), message.version(), e.getMessage());
        }
    }

    /**
     * Marks a message as published in the outbox table.
     *
     * @param id      the ID of the message
     * @param version the current version of the message
     */
    private void markMessageAsPublished(String id, int version) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                 "UPDATE outbox " +
                 "SET status = 'PUBLISHED', published_at = ?, version = version + 1 " +
                 "WHERE id = ? AND version = ?")) {
            statement.setTimestamp(1, Timestamp.from(Instant.now()));
            statement.setString(2, id);
            statement.setInt(3, version);
            int updated = statement.executeUpdate();
            if (updated == 0) {
                LOGGER.warn("Failed to mark message as published due to version mismatch: id={}", id);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to mark message as published: id={}", id, e);
        }
    }

    /**
     * Increments the retry count for a message in the outbox table.
     *
     * @param id        the ID of the message
     * @param version   the current version of the message
     * @param errorMessage the error message from the failed publication attempt
     */
    private void incrementRetryCount(String id, int version, String errorMessage) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                 "UPDATE outbox " +
                 "SET retry_count = retry_count + 1, last_error = ?, version = version + 1 " +
                 "WHERE id = ? AND version = ?")) {
            statement.setString(1, errorMessage);
            statement.setString(2, id);
            statement.setInt(3, version);
            int updated = statement.executeUpdate();
            if (updated == 0) {
                LOGGER.warn("Failed to increment retry count due to version mismatch: id={}", id);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to increment retry count: id={}", id, e);
        }
    }

    /**
     * Purges published messages from the outbox table that are older than the specified retention period.
     * This method is scheduled to run at a fixed rate.
     */
    @Scheduled(cron = "${outbox.purge.cron:0 0 * * * *}")
    public void purgePublishedMessages() {
        LOGGER.debug("Purging published messages from outbox");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                 "DELETE FROM outbox " +
                 "WHERE status = 'PUBLISHED' " +
                 "AND published_at < ?")) {
            // Default retention period: 7 days
            Timestamp retentionThreshold = Timestamp.from(Instant.now().minusSeconds(7 * 24 * 60 * 60));
            statement.setTimestamp(1, retentionThreshold);
            int deleted = statement.executeUpdate();
            if (deleted > 0) {
                LOGGER.info("Purged {} published messages from outbox", deleted);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to purge published messages from outbox", e);
        }
    }

    /**
     * Represents a message in the outbox table.
     */
    private record OutboxMessage(String id, String topic, String key, String payload, int retryCount, int version) {}

    /**
     * Interface for publishing messages to a broker.
     * This should be implemented by a class that knows how to interact with the specific message broker being used.
     */
    public interface MessagePublisher {
        /**
         * Publishes a message to the specified topic.
         *
         * @param topic   the topic to publish to
         * @param key     the key for the message (can be null)
         * @param payload the message payload
         * @throws Exception if the publication fails
         */
        void publish(String topic, String key, String payload) throws Exception;
    }
}