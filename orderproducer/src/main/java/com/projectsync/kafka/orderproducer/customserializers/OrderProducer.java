package com.projectsync.kafka.orderproducer.customserializers;

import com.projectsync.kafka.orderproducer.OrderCallBack;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.projectsync.kafka.orderproducer.customserializers.partitioners.VIPPartitioner;
import java.util.Properties;

@Slf4j
public class OrderProducer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.projectsync.kafka.orderproducer.customserializers.OrderSerializer");
		props.setProperty("partitioner.class", VIPPartitioner.class.getName());

		KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
		Order order = new Order("Mohan", "iPhone", 1);
		// ProducerRecord<String, Order> record = new ProducerRecord<>("OrderCSTopic", order.getCustomerName(), order);
		// kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic OrderCSTopic
		// kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 10 --topic OrderPartitionedTopic
		ProducerRecord<String, Order> record = new ProducerRecord<>("OrderPartitionedTopic", order.getCustomerName(), order);

		try {
			producer.send(record, new OrderCallBack());
		} catch (Exception e) {
			log.error("@@@ Exception occurred: " + e.getStackTrace());
		} finally {
			producer.close();
		}
	}
}
