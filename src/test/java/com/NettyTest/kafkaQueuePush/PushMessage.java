package com.NettyTest.kafkaQueuePush;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.protobuf.DynamicMessage;

public class PushMessage {
	public static void pushMessage(DynamicMessage message) {
		Properties producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", "localhost:9092");
		producerProperties.put("serializer.class",
				org.apache.kafka.common.serialization.StringSerializer.class.getName());
		producerProperties.put("key.serializer",
				org.apache.kafka.common.serialization.StringSerializer.class.getName());
		producerProperties.put("value.serializer",
				org.apache.kafka.common.serialization.StringSerializer.class.getName());
		producerProperties.put("request.required.acks", "0");
		KafkaProducer<String, Object> kafkawriter = new KafkaProducer<String, Object>(producerProperties);
		ProducerRecord<String, Object> constructedMessage = new ProducerRecord<String, Object>("test", "ImKey",
				message);
		// pushing the message to kafkaQueue.
		kafkawriter.send(constructedMessage);
	}
}
