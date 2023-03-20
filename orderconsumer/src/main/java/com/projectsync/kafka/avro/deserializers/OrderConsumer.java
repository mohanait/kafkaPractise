package com.projectsync.kafka.avro.deserializers;

import com.projectsync.kafka.avro.Order;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class OrderConsumer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
		props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
		props.setProperty("group.id", "OrderGroup");
		props.setProperty("schema.registry.url", "http://192.168.137.128:8081/");
		props.setProperty("specific.avro.reader", "true");

		KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList("OrderAvroTopic"));

		ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(20));
		for (ConsumerRecord<String, Order> record : records) {
			String customerName = record.key();
			Order order = record.value();
			System.out.println("Customer Name: " + customerName);
			System.out.println("Product: " + order.getProduct());
			System.out.println("Quantity: " + order.getQuantity());

		}
		consumer.close();
	}

}