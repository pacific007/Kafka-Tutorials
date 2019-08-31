/**
 * 
 */
package com.pacific.consumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * @author prashant
 *
 */
public class KafkaConsumerTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String topic = "MYFIRSTTOPIC";
		String consumerGroupName = "Consumers Group";
		Properties props = new Properties();

		props.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9095"); //mandatory
		props.setProperty("group.id", consumerGroupName); //mandatory - if not passed than org.apache.kafka.common.errors.InvalidGroupIdException: 
		props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //mandatory
		props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"); //mandatory

		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		while (true) {
			@SuppressWarnings("deprecation")
			ConsumerRecords<String, String> records = consumer.poll(100);
			records.forEach(record -> System.out.println(record.value()));

		}

	}

}
