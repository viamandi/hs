package com.example.mqtt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    @Autowired
    private Mqtt3AsyncClient mqttClient;

    @Scheduled(fixedRate = 3000) // Publish every 3 seconds
    public void publishMessage() {
        // Create a VehicleData object to be serialized
        VehicleData data = new VehicleData();
        data.setOperationId(UUID.randomUUID().toString());
        data.setVin("VIN-" + UUID.randomUUID().toString().substring(0, 8));
        data.setLoDegree(12.34f);
        data.setLoDirection("E");
        data.setLaDegree(56.78f);
        data.setLaDirection("N");
        data.setLastUpdateTimestamp(Instant.now());

        try {
            // Serialize the object to a JSON string
            String message = OBJECT_MAPPER.writeValueAsString(data);

            logger.info("Publishing to topic {}: {}", Constants.LISTENER_TOPIC_NEW, message);
            mqttClient.publishWith()
                    .topic(Constants.LISTENER_TOPIC_NEW)
                    .payload(message.getBytes(StandardCharsets.UTF_8))
                    .send()
                    .whenComplete((publish, throwable) -> {
                        if (throwable != null) {
                            logger.error("Error publishing message to topic {}: {}", Constants.LISTENER_TOPIC_NEW, throwable.getMessage());
                        } else {
                            logger.debug("Successfully published message to topic {}", Constants.LISTENER_TOPIC_NEW);
                        }
                    });
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize VehicleData object to JSON", e);
        }
    }
}