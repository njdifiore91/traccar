package org.traccar.message;

/**
 * Interface for handling messages received from the message broker.
 */
public interface MessageHandler {

    /**
     * Handles a message received from the message broker.
     * 
     * @param topic The topic the message was received from
     * @param message The message payload
     */
    void onMessage(String topic, byte[] message);
    
    /**
     * Handles an error that occurred while processing a message.
     * 
     * @param topic The topic the message was received from
     * @param error The error that occurred
     */
    void onError(String topic, Throwable error);
}