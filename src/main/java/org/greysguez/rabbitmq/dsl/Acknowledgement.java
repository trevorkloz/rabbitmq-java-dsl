package org.greysguez.rabbitmq.dsl;


public enum Acknowledgement {

	/**
	 * When message needs no acknowledgement i.e. when operating on a auto-ack
	 * consumer.
	 */
	NONE,

	/** Positive Acknowledgement */
	ACK,

	/** Negative Acknowledgement */
	NACK
}