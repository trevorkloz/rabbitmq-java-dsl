package org.greysguez.rabbitmq.dsl;

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * Flow API to build publisher and consumer
 */
public class QueueBuilder {

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
	
	public class Queue {

		protected boolean queueAutoDelete = false;
		protected boolean queueDurable = false;
		protected boolean queueExclusive = false;
		
		protected String queueName = "queue";
		protected long queueMaxLength = -1; // not set
		protected long queueMaxLengthBytes = -1; // not set
		protected long queueMsgTTL = -1; // not set
		
		protected Consumer consumer;
	
		public Queue(Consumer consumer, String queueName) {
			this.consumer = consumer;
			this.queueName = queueName;
		}
		
		public Queue queueMaxLength(long queueMaxLength){
			this.queueMaxLength = queueMaxLength;
			return this;
		}
		
		public Queue queueMaxLengthBytes(long queueMaxLengthBytes){
			this.queueMaxLengthBytes = queueMaxLengthBytes;
			return this;
		}

		public Queue queueMsgTTL(long queueMsgTTL){
			this.queueMsgTTL = queueMsgTTL;
			return this;
		}
		
		public Queue queueDurable(){
			this.queueDurable = true;
			return this;
		}

		public Queue queueExclusive(){
			this.queueExclusive = true;
			return this;
		}
		
		public Queue queueAutoDelete(){
			this.queueAutoDelete = true;
			return this;
		}

		public Exchange topicExchange(String exchangeName, String... bindingKeys){
			return new Exchange(this, exchangeName, "topic", bindingKeys);
		}
		
		public Exchange fanoutExchange(String exchangeName){
			return new Exchange(this, exchangeName, "fanout", new String[]{""});
		}
		
		public Exchange directExchange(String exchangeName, String... bindingKeys){
			return new Exchange(this, exchangeName, "direct", bindingKeys);
		}
	}
	
	public class Exchange {
		
		protected String[] bindingKeys;
		protected String exchangeName = "#";
		protected String exchangeType = "topic";
		protected boolean durable = false;
		private Queue queue;
		
		public Exchange(Queue queue, String exchangeName, String exchangeType, String[] bindingKeys) {
			this.queue = queue;
			this.exchangeName = exchangeName;
			this.exchangeType = exchangeType;
			this.bindingKeys = bindingKeys;
		}
		
		public Exchange durable(){
			this.durable = true;
			return this;
		}
		
		public QueueConsumer build(){
			QueueConsumer rmqc = new QueueConsumer();
			rmqc.declare(
					queue.consumer.base, 
					queue.queueName, 
					this.exchangeName, 
					this.exchangeType, 
					this.bindingKeys, 
					this.durable,
					queue.queueMaxLength,
					queue.queueMaxLengthBytes,
					queue.queueMsgTTL,
					queue.consumer.autoAck,
					queue.queueAutoDelete,
					queue.queueDurable,
					queue.queueExclusive);
			return rmqc;
		}
	}
	
	public class Consumer {

		protected boolean autoAck = true;
		protected Base base;
		
		public Consumer(Base base) {
			this.base = base;
		}
		
		public Consumer autoAck(boolean autoAck){
			this.autoAck = autoAck;
			return this;
		}
		
		public Queue fromQueue(String queueName){
			return new Queue(this, queueName);
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
		
		public Consumer consume(){
			return new Consumer(this);
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