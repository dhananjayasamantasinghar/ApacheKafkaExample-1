package com.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SimpleKafkaProducerApplication {

	@Value("${kafka.bootstrap.servers}")
	private String kafkaBootstrapServers;

	@Value("${zookeeper.groupId}")
	private String zookeeperGroupId;

	@Value("${kafka.topic.thetechcheck}")
	private String theTechCheckTopicName;

	public static void main(String[] args) {
		SpringApplication.run(SimpleKafkaProducerApplication.class, args);
	}

	@Bean
	public KafkaConsumer<String, String> kafkaConsumer() {

		Properties consumerProperties = new Properties();
		consumerProperties.put("bootstrap.servers", kafkaBootstrapServers);
		consumerProperties.put("group.id", zookeeperGroupId);
		consumerProperties.put("zookeeper.session.timeout.ms", "6000");
		consumerProperties.put("zookeeper.sync.time.ms", "2000");
		consumerProperties.put("auto.commit.enable", "false");
		consumerProperties.put("auto.commit.interval.ms", "1000");
		consumerProperties.put("consumer.timeout.ms", "-1");
		consumerProperties.put("max.poll.records", "1");
		consumerProperties.put("value.deserializer", StringDeserializer.class.getName());
		consumerProperties.put("key.deserializer", StringDeserializer.class.getName());

		KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties);
		kafkaConsumer.subscribe(Arrays.asList(theTechCheckTopicName));
		return kafkaConsumer;
	}

	@Bean
	public KafkaProducer<String, String> kafkaProducer() {
		Properties producerProperties = new Properties();
		producerProperties.put("bootstrap.servers", kafkaBootstrapServers);
		producerProperties.put("acks", "all");
		producerProperties.put("retries", 0);
		producerProperties.put("batch.size", 16384);
		producerProperties.put("linger.ms", 1);
		producerProperties.put("buffer.memory", 33554432);
		producerProperties.put("key.serializer", StringSerializer.class.getName());
		producerProperties.put("value.serializer", StringSerializer.class.getName());
		KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
		return producer;
	}

}
