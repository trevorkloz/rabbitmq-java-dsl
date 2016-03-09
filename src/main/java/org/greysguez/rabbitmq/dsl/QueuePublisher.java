package org.greysguez.rabbitmq.dsl;

import java.io.IOException;

import org.greysguez.rabbitmq.dsl.QueueBuilder.Base;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Rabbit MQ Connector to publish messages to the exchange of a queue.
 */
public class QueuePublisher implements AutoCloseable {

	private Channel channel;
	private String amqpUrl;
	private Connection connection;
    
	private String exchangeName;
	private String routingKey;

	private BasicProperties messageProperties;

	/* package */ QueuePublisher() {
		// Will be called through fluent API
    }
    
	/* package */ void declare(Base base,
			String exchangeName, String exchangeType, String routingKey, boolean exchangeDurable){
    	
		this.exchangeName = exchangeName;
		this.routingKey = routingKey;
		
		this.messageProperties = base.properties == null ? 
				new AMQP.BasicProperties.Builder().build() : base.properties;
				
		this.amqpUrl = amqpUrl(base.user, base.password, base.host, base.port);

        ConnectionFactory factory = factory();
        
        this.connection = connection(factory);
        this.channel = channel(connection);
        
        try {
        	this.channel.exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
        } catch (Exception e) {
            throw new QueueException("Could not declare exchange:", e);
        }
    }

    public void publish(String message) {
        try {
            channel.basicPublish(exchangeName, routingKey, messageProperties, message.getBytes());
        } catch (Exception e) {
            throw new QueueException("Could not publish message:", e);
        }
    }
    
    public void publish(String exchange, String routingKey, String message) {
        try {
            channel.basicPublish(exchangeName, routingKey, messageProperties, message.getBytes());
        } catch (Exception e) {
            throw new QueueException("Could not publish message:", e);
        }
    }
    
    public void close() {
        try {
            channel.close();
            connection.close();
        } catch (Exception e) {
            throw new QueueException("Could not close connection:", e);
        }
    }

    protected String amqpUrl(String user, String pass, String host, String port) {
        return String.format("amqp://%s:%s@%s:%s", user, pass, host, port);
    }

    protected Channel channel(Connection connection) {
        try {
            return connection.createChannel();
        } catch (IOException e) {
            throw new QueueException("Could not create channel:", e);
        }
    }

    protected Connection connection(ConnectionFactory factory) {
        try {
            return factory.newConnection();
        } catch (Exception e) {
            throw new QueueException("Could not create channel:", e);
        }
    }

    protected ConnectionFactory factory() {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(amqpUrl);
            return factory;
        } catch (Exception ex) {
            String message = String.format("Failed to initialize ConnectionFactory with %s.", amqpUrl);
            throw new QueueException(message, ex);
        }
    }
}
