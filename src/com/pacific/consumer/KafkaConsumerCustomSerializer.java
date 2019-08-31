package com.pacific.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerCustomSerializer {

	public static void main(String[] args) {
		String topic = "MYFIRSTTOPIC";
		String consumerGroupName = "Consumers Group";
		Properties props = new Properties();

		props.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9095");
		props.setProperty("group.id", consumerGroupName);
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.setProperty("value.deserializer", "com.pacific.consumer.EmployeeDeserializer");

		KafkaConsumer<String, Employee> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		while (true) {
			ConsumerRecords<String, Employee> records = consumer.poll(100);
			records.forEach(record -> System.out.println(record.value().getFirstName()));

		}

	}

}
