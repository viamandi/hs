package com.example.mqtt;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@EnableScheduling
public class MqttPublisher {

    private static final Logger logger = LoggerFactory.getLogger(MqttPublisher.class);

    String new_listener_schema = "";
    @Autowired
    private Mqtt3AsyncClient mqttClient;

    @Scheduled(fixedRate = 3000) // Publish every 3 seconds
    public void publishMessage() {
        String message = "{\"operation_id\":\"string\", \"vin\":\"string\", \"lo_degree\":\"float\", \"lo_direction\":\"string\", \"la_degree\":\"float\", \"la_direction\":\"string\", \"lastUpdateTimestamp\":\"date\"}";

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