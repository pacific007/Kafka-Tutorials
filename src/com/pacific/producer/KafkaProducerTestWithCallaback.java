package com.pacific.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerTestWithCallaback {

	public static void main(String[] args) {
		/**
		 * Topic name created in kafka broker - if not created already API will create
		 * by itself
		 */
		String topicName = "MYFIRSTTOPIC";

		String key = "key-callback-1";
		String value = "value-1";

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9095");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> message = new ProducerRecord<String, String>(topicName, key, value);

		/**
		 * Producer will call the callback method passed as second parameter
		 */
		producer.send(message, new ProducerCallback());

		producer.close();

	}

}

class ProducerCallback implements Callback {

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exp) {
		if (exp == null) {
			System.out.println("Message sent to broker at partition " + metadata.partition());
			System.out.println("Callback Producer completed");
		} else {
			System.out.println(exp.getMessage());
		}

	}

}