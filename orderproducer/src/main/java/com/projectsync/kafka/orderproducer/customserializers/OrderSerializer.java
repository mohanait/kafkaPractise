package com.projectsync.kafka.orderproducer.customserializers;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Slf4j
public class OrderSerializer implements Serializer<Order> {

    @Override
    public byte[] serialize(String topic, Order order) {

        byte[] response = null;
        ObjectMapper objectMapper = new ObjectMapper();

        try {
            response = objectMapper.writeValueAsString(order).getBytes();
        } catch (JsonProcessingException e) {
            log.error("@@@ Exception occurred: " + e.getStackTrace());
        }
        return response;
    }
}