package com.pacific.consumer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

public class EmployeeSerializer implements Serializer<Employee> {

	private String encoding = "UTF8";

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, Employee employee) {

		byte[] serializedFirstName;
		byte[] serializedLastName;
		byte[] serializedEmpId;

		try {
			if (employee == null) {
				return null;
			} else {
				serializedFirstName = employee.getFirstName().getBytes(encoding);
				serializedLastName = employee.getLastName().getBytes(encoding);
				serializedEmpId = ByteBuffer.allocate(4).putInt(employee.getEmpId()).array();

				ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + serializedFirstName.length + 4
						+ serializedLastName.length + 4 + serializedEmpId.length + 4);

				buffer.putInt(employee.getEmpId());
				buffer.putInt(serializedFirstName.length);
				buffer.put(serializedFirstName);
				buffer.putInt(serializedLastName.length);
				buffer.put(serializedLastName);
				
				return buffer.array();

			}

		} catch (UnsupportedEncodingException e) {
			throw new SerializationException(
					"Error when serializing string to byte[] due to unsupported encoding " + encoding);
		}
	}

}
