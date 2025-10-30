package com.example.mqtt;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import java.io.IOException;

@Service
public class MqttSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(MqttSubscriber.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule()) // Suport pentru Instant, LocalDateTime etc.
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    @Autowired
    private Mqtt3AsyncClient mqttClient;
    @Autowired
    private VehicleDataRepository vehicleDataRepository;

    @PostConstruct
    public void subscribeToTopic() {
        System.out.println("QWERTY: Subscribing to topic: " + Constants.LISTENER_TOPIC_NEW);
        mqttClient.subscribeWith()
            .topicFilter(Constants.LISTENER_TOPIC_NEW)
            .callback(publish -> {
                String message = new String(publish.getPayloadAsBytes());
                logger.info("Received MQTT message from topic {}: {}", Constants.LISTENER_TOPIC_NEW, message);
                handleMessage(message);
            })
            .send()
            .whenComplete((subAck, subscribeThrowable) -> {
                if (subscribeThrowable != null) {
                    logger.error("Error subscribing to topic {}: {}", Constants.LISTENER_TOPIC_NEW, subscribeThrowable.getMessage());
                } else {
                    logger.info("Subscribed to topic: {}", Constants.LISTENER_TOPIC_NEW);
                }
            });
    }

    private void handleMessage(String message) {
        try {
            // Map JSON to our VehicleData entity
            VehicleData data = OBJECT_MAPPER.readValue(message, VehicleData.class);
            // Save the entity to the database
            VehicleData savedData = vehicleDataRepository.save(data);
            logger.info("Saved vehicle data to DB with ID: {}", savedData.getId());
        } catch (IOException e) {
            logger.error("Failed to parse and save message: {}", message, e);
        }
    }
}