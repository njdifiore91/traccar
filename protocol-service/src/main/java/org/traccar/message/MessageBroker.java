package org.traccar.message;

import io.netty.buffer.ByteBuf;
import org.traccar.model.Position;

/**
 * Interface for message broker integration in microservices architecture.
 * This interface defines methods for publishing decoded messages to a message broker
 * for consumption by other services.
 */
public interface MessageBroker {

    /**
     * Publishes a raw message to the message broker.
     * 
     * @param topic The topic to publish to
     * @param message The raw message as a ByteBuf
     * @return true if the message was successfully published, false otherwise
     */
    boolean publishRawMessage(String topic, ByteBuf message);
    
    /**
     * Publishes a decoded position to the message broker.
     * 
     * @param topic The topic to publish to
     * @param position The decoded position
     * @return true if the position was successfully published, false otherwise
     */
    boolean publishPosition(String topic, Position position);
    
    /**
     * Subscribes to a topic for receiving messages.
     * 
     * @param topic The topic to subscribe to
     * @param handler The handler to process received messages
     * @return true if the subscription was successful, false otherwise
     */
    boolean subscribe(String topic, MessageHandler handler);
    
    /**
     * Unsubscribes from a topic.
     * 
     * @param topic The topic to unsubscribe from
     * @return true if the unsubscription was successful, false otherwise
     */
    boolean unsubscribe(String topic);
    
    /**
     * Closes the connection to the message broker.
     */
    void close();
}