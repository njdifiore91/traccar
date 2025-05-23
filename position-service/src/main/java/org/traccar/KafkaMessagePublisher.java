package org.traccar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

/**
 * Implementation of the MessagePublisher interface that publishes messages to Kafka.
 * <p>
 * This class is used by the TransactionOutboxManager to publish messages from the outbox
 * to Kafka topics. It handles the actual interaction with the Kafka broker.
 */
@Component
public class KafkaMessagePublisher implements TransactionOutboxManager.MessagePublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaMessagePublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Constructs a new KafkaMessagePublisher.
     *
     * @param kafkaTemplate the Kafka template to use for publishing messages
     */
    @Autowired
    public KafkaMessagePublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a message to the specified Kafka topic.
     * <p>
     * This method sends the message to Kafka and waits for the result to ensure
     * the message was successfully delivered. If the delivery fails, an exception
     * is thrown which will be caught by the TransactionOutboxManager for retry handling.
     *
     * @param topic   the Kafka topic to publish to
     * @param key     the key for the message (can be null)
     * @param payload the message payload
     * @throws Exception if the publication fails
     */
    @Override
    public void publish(String topic, String key, String payload) throws Exception {
        try {
            ListenableFuture<SendResult<String, String>> future;
            if (key != null) {
                future = kafkaTemplate.send(topic, key, payload);
            } else {
                future = kafkaTemplate.send(topic, payload);
            }

            // Add a callback to log the result
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
                @Override
                public void onSuccess(SendResult<String, String> result) {
                    LOGGER.debug("Message sent successfully to topic {}: {}", topic, result.getRecordMetadata());
                }

                @Override
                public void onFailure(Throwable ex) {
                    LOGGER.error("Failed to send message to topic {}", topic, ex);
                }
            });

            // Wait for the result to ensure the message was delivered
            future.get();
        } catch (Exception e) {
            LOGGER.error("Error publishing message to Kafka topic {}", topic, e);
            throw e;
        }
    }
}