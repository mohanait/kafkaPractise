package com.projectsync.kafka.orderconsumer.customdeserializers;

import java.io.IOException;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class OrderDeserializer implements Deserializer<Order> {

    @Override
    public Order deserialize(String topic, byte[] data) {

        ObjectMapper objectMapper = new ObjectMapper();
        Order order = null;
        try {
            order = objectMapper.readValue(data, Order.class);
        } catch (IOException e) {
            log.error("@@@ Exception occurred: " + e.getStackTrace());
        }
        return order;
    }
}
