# rabbitmq-java-dsl
DSL / Fluent API in Java to connect to RabbitMQ

#### Publisher

To build a **publisher**:

```java
QueuePublisher publisher = new QueueBuilder().connectTo("user", "pass", "localhost", "5672")
			.publishToExchange("data-exchange").onTopic("topic-name").build();
```			

Then you can use this publisher to **send a message**:

```java
Message message = new Message("TYPE");
message.setPayload("Some Text, JSON or XML to send...");
publisher.publish(message.toJson());
```	

#### Consumer

Build a **consumer**:

```java
QueueConsumer consumer = new QueueBuilder().connectTo("user", "pass", "localhost", "5672")
			.consumeFromQueue("data-queue")
			.fromTopic("topic-name")
			.autoAck(true)
			.fromExchange("data-exchange").build();
```	

Then somewhere in your code you can do something like this to **receive messages**:

```java
public class XYZClass {
  
  // ...snip
  
  Thread thread = new Thread(new Runnable() {
  	public void run() {
  		XYZClass.this.consumer.deliveryLoop(new IConsumer() {
  			public Acknowledgement handle(String message) {
  				return XYZClass.this.handle(message);
  			}
  		});
  	}
  });
  thread.start();

  private Acknowledgement handle(String message) {
		Message msg = Message.fromJson(message);
		if(msg.getType().matches("TYPE")){
			 // ... do some fancy magic ...
		}
		return Acknowledgement.NONE;
	}
  // ...snip
}
```

Have fun, or contribute...
