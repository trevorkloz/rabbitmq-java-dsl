package org.greysguez.rabbitmq.dsl;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

import org.greysguez.rabbitmq.dsl.QueueBuilder.Base;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class QueueConsumer {

	private static final Logger logger = LoggerFactory.getLogger(Connection.class);
    
	private Channel channel;
	private QueueingConsumer consumer;
	private Connection connection;

	private String queueName;
	private boolean autoAck = true;
    private boolean queueAutoDelete = false;
    private boolean queueDurable = false;
    private boolean queueExclusive = false;
	private long queueMaxLength;
	private long queueMaxLengthBytes;
	private long queueMsgTTL;

	private String amqpUrl;

	/* package */ QueueConsumer() {
    }
    
	/* package */ void declare(Base base,
			String queueName, String exchangeName, String exchangeType, String[] bindingKeys, boolean exchangeDurable,
			long queueMaxLength, long queueMaxLengthBytes, long queueMsgTTL, boolean autoAck,
			boolean queueAutoDelete, boolean queueDurable, boolean queueExclusive){
    	
		this.autoAck = autoAck;
		
		this.queueName = queueName;
		this.queueMaxLength = queueMaxLength;
		this.queueMaxLengthBytes = queueMaxLengthBytes;
		this.queueMsgTTL = queueMsgTTL;
		
		this.amqpUrl = amqpUrl(base.user, base.password, base.host, base.port);

        ConnectionFactory factory = factory();
        
        this.connection = connection(factory);
        this.channel = channel(connection);
        
        try {
        	this.channel.exchangeDeclare(exchangeName, exchangeType, exchangeDurable);
        } catch (Exception e) {
            throw new QueueException("Could not declare exchange:", e);
        }
        
    	declareQueue(channel);
    	this.consumer = consumer(channel, queueName);
    	
    	for (String key : bindingKeys) {
    		try {
    			channel.queueBind(queueName, exchangeName, key);
    		} catch (IOException e) {
    			throw new QueueException("Could not bind queue to exchange:", e);
    		}
		}
    }
    
    public void deliveryLoop(IConsumer handler) {
        while (true) {
            final QueueingConsumer.Delivery delivery = delivery(consumer);
            if (delivery != null) {
                try {
                    Acknowledgement handle = handler.handle(message(delivery));
                    if(handle == Acknowledgement.ACK)
                    	ack(channel, delivery);
                    else if(handle == Acknowledgement.NACK)
                    	nack(channel, delivery);
                } catch (Exception ex) {
                    nack(channel, delivery);
                }
            }
        }
    }
    
    private String message(QueueingConsumer.Delivery delivery) {
        try {
            return new String(delivery.getBody(), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new QueueException("Failed to parse message:", e);
        }
    }

    private void nack(Channel channel, QueueingConsumer.Delivery delivery) {
        try {
            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            channel.basicNack(deliveryTag, false, false);
            logger.warn(String.format("Rejected message: tag: %d body: %s ", deliveryTag, new String(delivery.getBody())));
        } catch (IOException e) {
            throw new QueueException("Failed to nack delivery:", e);
        }
    }

    private void ack(Channel channel, QueueingConsumer.Delivery delivery) {
        try {
            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        } catch (IOException e) {
            throw new QueueException("Failed to ack delivery:", e);
        }
    }

    protected QueueingConsumer.Delivery delivery(QueueingConsumer consumer) {
        try {
            return consumer.nextDelivery();
        } catch (InterruptedException e) {
            throw new QueueException("Consumer interrupted:", e);
        }
    }

    private QueueingConsumer consumer(Channel channel, String queueName) {
        try {
            QueueingConsumer consumer = new QueueingConsumer(channel);
            channel.basicConsume(queueName, autoAck, consumer);
            return consumer;
        } catch (IOException e) {
            throw new QueueException("Could not create consumer:", e);
        }
    }

    private void declareQueue(Channel channel) {
        try {
        	
        	HashMap<String, Object> arguments = new HashMap<String, Object>();
        	if(queueMaxLength > -1){
        		arguments.put("x-max-length", queueMaxLength);
        	}
        	if(queueMaxLengthBytes > -1){
        		arguments.put("x-max-length-bytes", queueMaxLengthBytes);
        	}
        	if(queueMsgTTL > -1){
        		arguments.put("x-message-ttl", queueMsgTTL);
        	}
        	
            channel.queueDeclare(
            		queueName, 
            		queueDurable, 
            		queueExclusive, 
            		queueAutoDelete, arguments);
            
        } catch (IOException e) {
            throw new QueueException("Could not declare queue:", e);
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
