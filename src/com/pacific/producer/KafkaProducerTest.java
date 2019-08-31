package com.pacific.producer;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaProducerTest {

	public static void main(String[] args) {

		/**
		 * Topic name created in kafka broker - if not created already API will create
		 * by itself
		 */
		String topicName = "MYFIRSTTOPIC";

		String key = "key-1";
		String value = "value-1";

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9095"); //mandatory
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //mandatory
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //mandatory

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		ProducerRecord<String, String> message = new ProducerRecord<String, String>(topicName, key, value);

		/**
		 * send() method will return Future Object which holds RecordMetaData like -
		 * offset, partition
		 */
		Future<RecordMetadata> response = producer.send(message);
		try {
			RecordMetadata metaData = response.get(); // Its blocking the thread till response returned from Kafka
														// Broker

			System.out.println("Message sent to broker at partition " + metaData.partition());
			System.out.println("Synchronous Producer completed");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		producer.close();

	}

}
