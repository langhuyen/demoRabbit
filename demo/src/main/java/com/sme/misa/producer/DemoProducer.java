package com.sme.misa.producer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import org.springframework.context.annotation.Configuration;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;


@Configuration
public class DemoProducer {
	
	private static Connection connection;
	private static Channel channel;
	public DemoProducer() throws IOException, TimeoutException {
		//Tạo kết nối
		ConnectionFactory conFactory=new ConnectionFactory();
		conFactory.setHost("localhost");
		conFactory.setUsername("guest");
		conFactory.setPassword("guest");
		conFactory.setPort(5672);
		try {
			//Khai báo các 
			connection=conFactory.newConnection();
			channel=  connection.createChannel();
			//Khai bao exchange voi ten voi loai exchange
			/*
			 * Co 4 loai exchange : direct,fanout,topic,header
			 */
			channel.exchangeDeclare("exchange.name", "direct");
			//Khai báo queue
			/*
			 * với đối só tên queue ,durable,exclusive,autoDelete,argument
			 *
			 */
			channel.queueDeclare("queue.name", true, false, false, null);
			
			/*
			 * Bind queue tới routing key
			 * 
			 */
			
			channel.queueBind("queue.name", "exchange.name", "rotingkey");
			
			boolean autoAck=false;
			/*
			 * Tạo consumer bằng câu lệnh channel.basicConsume()
			 * 
			 */
			channel.basicConsume("queue.name", autoAck, "idConsume", new DefaultConsumer(channel) {

				 @Override
		         public void handleDelivery(String consumerTag,
		                                    Envelope envelope,
		                                    AMQP.BasicProperties properties,
		                                    byte[] body)
		             throws IOException
		         {	
		             String routingKey = envelope.getRoutingKey();
		             String contentType = properties.getContentType();
		             long deliveryTag = envelope.getDeliveryTag();
		             String typeMessage=properties.getType();
		             // (process the message components here ...)
		             //lấy data
		             String messagePayload=new String(body,"UTF-8");
		             
		             /*
		              * Gửi lại ack sau khi xử lsy tin nhắn
		              */
		             channel.basicAck(deliveryTag, true);
		         }
				
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void produceMsg(String msg){
	    try {
	    	/*
	    	 * Có the dung dung cac ham 
	    	 * 
	    	 * case 1:mandatory flat quá tải.Nếu quá tải thì không cho public ,và set message với trạng thái persistent
	    	 * channel.basicPublish(exchangeName, routingKey, mandatory,
                     MessageProperties.PERSISTENT_TEXT_PLAIN,
                     messageBodyBytes);
	    	 * case 2:Chuyển tin nhắn với set các thuộc tính cho message
	    	 * channel.basicPublish(exchangeName, routingKey,
             new AMQP.BasicProperties.Builder()
               .contentType("text/plain")
               .deliveryMode(2)
               .priority(1)
               .userId("bob")
               .build(),
               messageBodyBytes);
	    	 * Map<String, Object> headers = new HashMap<String, Object>();
								headers.put("latitude",  51.5252949);
								headers.put("longitude", -0.0905493);
								
								channel.basicPublish(exchangeName, routingKey,
								             new AMQP.BasicProperties.Builder()
								               .headers(headers)
								               .build(),
								               messageBodyBytes);
			 channel.basicPublish(exchangeName, routingKey,
             new AMQP.BasicProperties.Builder()
               .expiration("60000")
               .build(),
               messageBodyBytes);
	    	 */
	    	
	    	
	    	/*
	    	 * Set message có thuộc tính type thuộc loại SanPham.Created
	    	 */
	    	BasicProperties props=new AMQP.BasicProperties.Builder().deliveryMode(2).type("SanPham.Created").build();
			channel.basicPublish("exchange.name", "rotingkey", true, props, msg.getBytes("UTF-8"));
		} catch (IOException e) {
			
			e.printStackTrace();
		}		    	   			 
	}	
	
}
