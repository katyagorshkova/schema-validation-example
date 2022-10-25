package org.jresearch.kafka.examples;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class AvroProducer {

	private final static String BOOTSTRAP_SERVERS = ":9092";

	private final static String TOPIC = "company";

	private final static String CLIENT_ID = "avro-test";

	private final static byte MAGIC_BYTE = 0x0;

	private final static int idSize = 4;

	public static void main(String[] args) throws IOException {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		props.put("schema.registry.url", "http://localhost:8081");

		System.out.println("Sending normal record and registering schema with id 1 ...");
		Company comp = new Company();
		comp.setTradeNumber(12345);
		comp.setRegisteredName("MyCompany");
		final Producer<String, Company> producer = new KafkaProducer<>(props);
		sendData(producer, new ProducerRecord<>(TOPIC, "12345", comp));
		producer.close();

		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

		System.out.println("Sending wrong record with non-existing id ...");
		final Producer<String, byte[]> producer2 = new KafkaProducer<>(props);

		sendData(producer2, new ProducerRecord<>(TOPIC, "11111", createBytes(2)));
		
		System.out.println("Sending wrong record with existing id ...");
		sendData(producer2, new ProducerRecord<>(TOPIC, "22222", createBytes(1)));
		
		producer2.close();
	}

	private static byte[] createBytes(int schemaId) throws IOException {
		try (ByteArrayOutputStream out = new ByteArrayOutputStream();) {

			out.write(MAGIC_BYTE);
			out.write(ByteBuffer.allocate(idSize).putInt(schemaId).array());
			Random rd = new Random();
			byte[] randomMsg = new byte[4];
			rd.nextBytes(randomMsg);
			out.write(randomMsg);
			return out.toByteArray();
		}
	}

	private static void sendData(Producer producer, ProducerRecord record) {
		try {
			RecordMetadata meta = (RecordMetadata) producer.send(record).get();
			System.out.printf("key=%s, value=%s => partition=%d, offset=%d\n", record.key(), record.value(),
					meta.partition(), meta.offset());

		} catch (InterruptedException | ExecutionException e) {
			System.out.printf("Exception %s\n", e.getMessage());
		}

	}
}
