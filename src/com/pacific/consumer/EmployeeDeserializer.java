package com.pacific.consumer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class EmployeeDeserializer implements Deserializer<Employee> {

	private String encoding = "UTF8";

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public Employee deserialize(String topic, byte[] employee) {
		try {
			if (employee == null) {
				return null;
			} else {
				ByteBuffer buf = ByteBuffer.wrap(employee);
				int id = buf.getInt();

				int sizeOfFirstName = buf.getInt();
				byte[] firstName = new byte[sizeOfFirstName];
				buf.get(firstName);

				String deserializedName = new String(firstName, encoding);

				int sizeOfLasName = buf.getInt();
				byte[] lastName = new byte[sizeOfLasName];
				buf.get(lastName);

				String deserializedLName = new String(lastName, encoding);

				return new Employee(deserializedName, deserializedLName, id);
			}

		} catch (UnsupportedEncodingException e) {
			throw new SerializationException(
					"Error when deserializing byte[] to string due to unsupported encoding " + encoding);
		}
	}
}
