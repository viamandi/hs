package com.example.mqtt;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

@Component
@EnableScheduling
public class MqttPublisher {

    private static final Logger logger = LoggerFactory.getLogger(MqttPublisher.class);

    String new_listener_schema = "";
    @Autowired
    private Mqtt3AsyncClient mqttClient;

    @Scheduled(fixedRate = 3000) // Publish every 3 seconds
    public void publishMessage() {
        String message = String.format("""
                {
                  "operation_id": "%s",
                  "vin": "VIN-%s",
                  "lo_degree": 12.34,
                  "lo_direction": "E",
                  "la_degree": 56.78,
                  "la_direction": "N",
                  "lastUpdateTimestamp": "%s"
                }""", UUID.randomUUID(), UUID.randomUUID().toString().substring(0, 8), Instant.now());
        System.out.println("QWERTY: Publishing to topic: " + Constants.LISTENER_TOPIC_NEW + " message: " + message);
        mqttClient.publishWith()
                .topic(Constants.LISTENER_TOPIC_NEW)
                .payload(message.getBytes(StandardCharsets.UTF_8))
                .send()
                .whenComplete((publish, throwable) -> {
                    if (throwable != null) {
                        logger.error("Error publishing message to topic {}: {}", Constants.LISTENER_TOPIC_NEW, throwable.getMessage());
                    } else {
                        logger.info("Published message to topic {}: {}", Constants.LISTENER_TOPIC_NEW, message);
                    }
                    System.out.println("Heart beat " + Constants.LISTENER_TOPIC_NEW + ": " + message);

                });
    }
}