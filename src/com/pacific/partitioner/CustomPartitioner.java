package com.pacific.partitioner;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

public class CustomPartitioner implements Partitioner {

	/**
	 * in this method you will receive all configurations passed as properties
	 */
	@Override
	public void configure(Map<String, ?> configs) {

	}

	@Override
	public void close() {

	}

	@Override
	public int partition(String topic, Object key, byte[] keybytes, Object value, byte[] valuebytes, Cluster cluster) {
		List<PartitionInfo> partitions = cluster.availablePartitionsForTopic(topic);
		int partitionSize = partitions.size();
		int s = (int) Math.abs(partitionSize * 0.3);
		int partition = 0;

		if (keybytes == null) {
			throw new InvalidRecordException("Key must be there in message");
		}

		if (key.equals("LOG")) {
			partition = 0;
		} else {
			partition =1;
		}
		return partition;
	}

}
