package com.projectsync.kafka.userProducer.controller;

import com.projectsync.kafka.userProducer.dto.User;
import com.projectsync.kafka.userProducer.service.UserProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/userapi")
public class UserController {

    @Autowired
    private UserProducerService service;

    @PostMapping("/publishUserData")
    public void sendUserData(@RequestBody User user) {
        service.sendUserData(user);
    }
}
