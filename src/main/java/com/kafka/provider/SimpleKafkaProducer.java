package com.kafka.provider;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SimpleKafkaProducer {

	private static final Logger logger = Logger.getLogger(SimpleKafkaProducer.class);

	@Value("${kafka.topic.thetechcheck}")
	private String topicName;

	@Autowired
	private KafkaProducer<String, String> producer;

	public void start() {

		// Send String message to kafka
		for (int index = 0; index < 10; index++) {
			sendKafkaMessage("The index is now: " + index, producer, topicName);
		}

		// Send JSON message to kafka
		for (int index = 0; index < 10; index++) {
			JSONObject jsonObject = createJsonObject(index);
			sendKafkaMessage(jsonObject.toString(), producer, topicName);
		}
	}

	private JSONObject createJsonObject(int index) {
		JSONObject jsonObject = new JSONObject();
		JSONObject nestedJsonObject = new JSONObject();
		try {
			jsonObject.put("index", index);
			jsonObject.put("message", "The index is now: " + index);
			nestedJsonObject.put("nestedObjectMessage", "This is a nested JSON object with index: " + index);
			jsonObject.put("nestedJsonObject", nestedJsonObject);
		} catch (JSONException e) {
			logger.error(e.getMessage());
		}
		return jsonObject;
	}

	private static void sendKafkaMessage(String payload, KafkaProducer<String, String> producer, String topic) {
		logger.info("Sending Kafka message: " + payload);
		producer.send(new ProducerRecord<>(topic, payload));
	}
}
