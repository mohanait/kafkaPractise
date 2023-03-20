package com.projectsync.kafka.orderproducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

@Slf4j
public class OrderProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");

		props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		props.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG, "343434334");
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
		props.setProperty(ProducerConfig.RETRIES_CONFIG, "2");
		props.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "400");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1024");
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
		props.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
		props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

		// 1. Fire and forget
		// producer.send(record);
		// log.info("@@@ Message sent successfully");

		// 2. sync call
		// RecordMetadata metadata = producer.send(record).get();
		// log.info("@@@ Partition: " + metadata.partition());
		// log.info("@@@ Offset: " + metadata.offset());
		// log.info("@@@ Message sent successfully");

		// 3. async call. Method will not wait. Whenever there is a response, onCompletion method is called in OrderCallBack class
		try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(props)) {
			ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "Mac Book Pro", 10);
			producer.send(record, new OrderCallBack());
		} catch (Exception e) {
			log.error(new StringBuilder().append("@@@ Exception occurred: ").append(e.getStackTrace()).toString());
		}
	}
}
