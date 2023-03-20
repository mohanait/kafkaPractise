package com.projectsync.kafka.userConsumer.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import com.projectsync.kafka.userConsumer.dto.User;

@Service
public class UserConsumerService {

    @KafkaListener(topics = { "user-topic" })
    public void consumerUserData(User user) {
        System.out.println("Users Name Is: " + user.getName()+" Fav Genre "+user.getFavGenre());
    }
}