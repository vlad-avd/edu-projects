package com.vlavd.kafkademo;

import com.vlavd.kafkademo.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;

@EnableKafka
@SpringBootApplication
public class KafkaDemoApplication {

	@KafkaListener(topics="msg")
	public void messageListener(ConsumerRecord<Long, User> record){
		System.out.println(record.partition());
		System.out.println(record.key());
		System.out.println(record.value());
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaDemoApplication.class, args);
	}

}
