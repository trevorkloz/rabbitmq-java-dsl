package org.greysguez.rabbitmq.dsl;

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Flow API to build publisher and consumer
 */
public class QueueBuilder {

	public class ConsumerBuilder {

		private Consumer consumer;

		public ConsumerBuilder(Consumer consumer) {
			this.consumer = consumer;
		}
		
		public QueueConsumer build(){
			QueueConsumer rmqc = new QueueConsumer();
			rmqc.declare(
					consumer.base, 
					consumer.queueName, 
					consumer.exchangeName, 
					consumer.exchangeType, 
					consumer.bindingKeys, 
					consumer.durable,
					consumer.queueMaxLength,
					consumer.queueMaxLengthBytes,
					consumer.queueMsgTTL,
					consumer.autoAck
			);
			return rmqc;
		}
	}
	
	public class PublisherBuilder {

		private Publisher publisher;

		public PublisherBuilder(Publisher publisher) {
			this.publisher = publisher;
		}
		
		public QueuePublisher build(){
			QueuePublisher rmqp = new QueuePublisher();
			rmqp.declare(
				publisher.base, 
				publisher.exchangeName, 
				publisher.exchangeType, 
				publisher.routingKey, publisher.durable);
			
			return rmqp;
		}
	}
	
	public class Consumer {

		protected boolean durable = false;
		protected String queueName = "queue";
		protected long queueMaxLength = -1; // not set
		protected long queueMaxLengthBytes = -1; // not set
		protected long queueMsgTTL = -1; // not set
		protected boolean autoAck = true;
		protected Base base;
		protected String[] bindingKeys;
		protected String exchangeName = "#";
		protected String exchangeType = "topic";
		
		public Consumer(String queueName, Base base) {
			this.queueName = queueName;
			this.base = base;
		}

		public Consumer queueMaxLength(long queueMaxLength){
			this.queueMaxLength = queueMaxLength;
			return this;
		}
		
		public Consumer queueMaxLengthBytes(long queueMaxLengthBytes){
			this.queueMaxLengthBytes = queueMaxLengthBytes;
			return this;
		}

		public Consumer queueMsgTTL(long queueMsgTTL){
			this.queueMsgTTL = queueMsgTTL;
			return this;
		}
		
		public Consumer autoAck(boolean autoAck){
			this.autoAck = autoAck;
			return this;
		}
		
		public Consumer fromTopic(String... bindingKeys){
			this.bindingKeys = bindingKeys;
			this.exchangeType = "topic";
			return this;
		}
		
		public Consumer directlyFrom(String... bindingKeys){
			this.bindingKeys = bindingKeys;
			this.exchangeType = "direct";
			return this;
		}
		
		public Consumer fanout(){
			this.bindingKeys = new String[]{""};
			this.exchangeType = "fanout";
			return this;
		}
		
		public ConsumerBuilder fromDurableExchange(String exchangeName){
			this.exchangeName = exchangeName;
			this.durable = true;
			return new ConsumerBuilder(this);
		}
		
		public ConsumerBuilder fromExchange(String exchangeName){
			this.exchangeName = exchangeName;
			this.durable = false;
			return new ConsumerBuilder(this);
		}
	}
	
	public class Publisher {
		
		protected boolean durable = false;
		protected String exchangeName = "#";
		protected String exchangeType = "topic";
		protected String routingKey = "#";
		protected Base base;
		
		public Publisher(String exchangeName, boolean durable, Base base) {
			this.base = base;
			this.durable = durable;
			this.exchangeName = exchangeName;
		}

		public PublisherBuilder onTopic(String routingKey){
			this.exchangeType = "topic";
			this.routingKey = routingKey;
			return new PublisherBuilder(this);
		}
		
		public PublisherBuilder directlyTo(String routingKey){
			this.exchangeType = "direct";
			this.routingKey = routingKey;
			return new PublisherBuilder(this);
		}
		
		public PublisherBuilder fanout(){
			this.exchangeType = "fanout";
			return new PublisherBuilder(this);
		}
	}
	
	public class Base {
		
		protected String user;
		protected String password;
		protected String host;
		protected String port;
		protected BasicProperties properties;

		public Base(String user, String password, String host, String port) {
			this.user = user;
			this.password = password;
			this.host = host;
			this.port = port;
		}

		public Publisher publishToExchange(String exchange){
			return new Publisher(exchange, false, this);
		}
		
		public Publisher publishToDurableExchange(String exchange){
			return new Publisher(exchange, true, this);
		}
		
		public Consumer consumeFromQueue(String queueName){
			return new Consumer(queueName, this);
		}
		
		public Base withProperties(BasicProperties properties){
			this.properties = properties;
			return this;
		}
	}
	
	public Base connectTo(String user, String password, String host, String port){
		return new Base(user, password, host, port);
	} 

}