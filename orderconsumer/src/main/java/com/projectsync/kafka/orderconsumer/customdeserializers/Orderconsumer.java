package com.projectsync.kafka.orderconsumer.customdeserializers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

@Slf4j
public class Orderconsumer {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, OrderDeserializer.class.getName());
		props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "OrderGroup"); // https://www.confluent.io/blog/configuring-apache-kafka-consumer-group-ids/
		//props.setProperty("auto.commit.interval.ms", "2000");
		props.setProperty("auto.commit.offset", "false");

		Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
		KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);

		// below methods are invoked suppose if re-balance happens. It must be written before subscribe method
		class RebalanceHandler implements ConsumerRebalanceListener{

			// commit any offsets that are processed, but not yet committed before re-balance
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				consumer.commitSync(currentOffsets); // commit last record that was processed before re-balance
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			}

		}


		// kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic OrderCSTopic
		// consumer.subscribe(Collections.singletonList("OrderCSTopic"));
		// kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 10 --topic OrderPartitionedTopic
		consumer.subscribe(Collections.singletonList("OrderPartitionedTopic"));

		try {
			while (true) {
				ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(20));
				int count = 0;
				for (ConsumerRecord<String, Order> record: records) {
					String customerName = record.key();
					Order order = record.value();
					System.out.println("@@@ Customer Name: " + customerName);
					System.out.println("@@@ Product: " + order.getProduct());
					System.out.println("@@@ Quantity: " + order.getQuantity());
					System.out.println("@@@ Partition: " + record.partition());
					currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
							new OffsetAndMetadata(record.offset() + 1)); // this map will have latest offset tht is processed. Useful when re-balance

					// offset commit. To minimize risk when re-balance happens
					if (count % 10 == 0) {
						consumer.commitAsync(currentOffsets, new OffsetCommitCallback() {
											@Override
											public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
																   Exception exception) {
												if(exception != null) {
													System.out.println("@@@ Commit failed for offset: " + offsets);
												}
											}
										});
					}
					count++;
				}

				// consumer.commitSync();
				/* // offset commit. To minimize risk when re-balance happens
				consumer.commitAsync(new OffsetCommitCallback() {
					@Override
					public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
						if(exception != null) {
							System.out.println("@@@ Commit failed for offset: " + offsets);
						}
					}
				});*/
			}
		} finally {
			consumer.close();
		}
	}
}