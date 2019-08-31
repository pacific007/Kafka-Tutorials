package com.pacific.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerCustomPartition {

	/**
	 * It will send all message with key LOG to the partition 0 else wise 1
	 * partition
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		/**
		 * Topic name created in kafka broker - if not created already API will create
		 * by itself
		 */
		String topicName = "MYFIRSTTOPIC";

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9095");
		props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty("partitioner.class", "com.pacific.partitioner.CustomPartitioner");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<String, String>(topicName, "LOG", "Message from log " + i));
		}

		for (int i = 0; i < 10; i++) {
			producer.send(new ProducerRecord<String, String>(topicName, "CLICK" + i, "Message from click " + i));
		}

		producer.close();

	}

}
