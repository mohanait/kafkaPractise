package com.projectsync.kafka.orderproducer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;

@Slf4j
public class OrderCallBack implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {

        System.out.println("@@@ Partition: " + metadata.partition());
        System.out.println("@@@ Offset: " + metadata.offset());
        System.out.println("@@@ Message sent successfully");

        if (exception != null) {
            log.error("@@@ Exception occurred: " + exception.getMessage());
        }
    }
}
