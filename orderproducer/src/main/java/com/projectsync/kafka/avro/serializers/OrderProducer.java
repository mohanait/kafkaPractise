package com.projectsync.kafka.avro.serializers;

import com.projectsync.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class OrderProducer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
		props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		// confluent local services start
		// http://192.168.137.128:8081/schemas
		props.setProperty("schema.registry.url", "http://192.168.137.128:8081");

		KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
		Order order = new Order("Mohan","avro iPhone14",3);
		ProducerRecord<String, Order> record = new ProducerRecord<>("OrderAvroTopic", order.getCustomerName().toString(), order);

		try {
			producer.send(record);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			producer.close();
		}
	}
}
