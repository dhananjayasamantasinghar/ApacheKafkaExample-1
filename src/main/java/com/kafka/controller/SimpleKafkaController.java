package com.kafka.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.consumer.SimpleKafkaConsumer;
import com.kafka.provider.SimpleKafkaProducer;

@RestController
public class SimpleKafkaController {

	@Autowired
	private SimpleKafkaProducer producer;

	@Autowired
	private SimpleKafkaConsumer consumer;

	@GetMapping("/kafka/start")
	public ResponseEntity<String> startKafka() {
		producer.start();
		consumer.start();
		return ResponseEntity.ok("Apache Kafka Started..");
	}
}
