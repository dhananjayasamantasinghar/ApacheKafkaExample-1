package com.kafka.consumer;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SimpleKafkaConsumer {

	private static final Logger logger = Logger.getLogger(SimpleKafkaConsumer.class);

	@Autowired
	private KafkaConsumer<String, String> consumer;

	public void start() {
			logger.info("Starting Kafka consumer");
			runSingleWorker();
	}

	private void runSingleWorker() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				doStartConsumer(record, consumer);
				logger.info("Offset committed to Kafka.");
			}
		}
	}

	private static void doStartConsumer(ConsumerRecord<String, String> record,
			KafkaConsumer<String, String> kafkaConsumer) {
		logTheMessage(record);
		commitToKafka(record, kafkaConsumer);
	}

	private static void logTheMessage(ConsumerRecord<String, String> record) {
		String message = record.value();
		logger.info("Received message: " + message);
		try {
			JSONObject receivedJsonObject = new JSONObject(message);
			logger.info("Index of deserialized JSON object: " + receivedJsonObject.getInt("index"));
		} catch (JSONException e) {
			logger.error(e.getMessage());
		}
	}

	private static void commitToKafka(ConsumerRecord<String, String> record,
			KafkaConsumer<String, String> kafkaConsumer) {
		Map<TopicPartition, OffsetAndMetadata> commitMessage = new HashMap<>();
		commitMessage.put(new TopicPartition(record.topic(), record.partition()),
				new OffsetAndMetadata(record.offset() + 1));
		kafkaConsumer.commitSync(commitMessage);
	}
}
