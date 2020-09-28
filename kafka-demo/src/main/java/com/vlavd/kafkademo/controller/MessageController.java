package com.vlavd.kafkademo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vlavd.kafkademo.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("message")
public class MessageController {
    @Autowired
    private KafkaTemplate<Long, User> kafkaTemplate;

    @PostMapping
    public void sendMsg(@RequestParam(value = "msgId") Long msgId, @RequestParam(value = "user")String user) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        User msg = mapper.readValue(user, User.class);
        ListenableFuture<SendResult<Long, User>> future = kafkaTemplate.send("msg", msgId, msg);
        future.addCallback(System.out::println, System.err::println);
        kafkaTemplate.flush();
    }
}
