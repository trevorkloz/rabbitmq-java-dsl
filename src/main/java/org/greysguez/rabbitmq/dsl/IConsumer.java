package org.greysguez.rabbitmq.dsl;

/**
 * Consumes messages from queue and gives back an acknowledgement.
 */
public interface IConsumer {
	
	/**
	 * Handles messages of the consumer
	 * @param message the message itself
	 */
	Acknowledgement handle(String message);
}
