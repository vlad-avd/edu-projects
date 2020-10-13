package com.vlavd.kafkademo.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.vlavd.kafkademo.model.User;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

import static com.google.common.io.Resources.getResource;

@RestController
@RequestMapping("message")
public class MessageController {
    @Autowired
    private KafkaTemplate<Long, GenericRecord> kafkaTemplate;

    @PostMapping
    public void sendMsg(@RequestParam(value = "msgId") Long msgId, @RequestParam(value = "user")String user) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        User msg = mapper.readValue(user, User.class);

        GenericRecord record = new GenericData.Record(new Schema.Parser().parse(Resources.toString(getResource("schema/user.avro"), Charsets.UTF_8)));
        record.put("name", msg.getName());
        record.put("age", msg.getAge());

        kafkaTemplate.setDefaultTopic("msg");
        kafkaTemplate.sendDefault(1L, record);
        kafkaTemplate.flush();
    }
}
