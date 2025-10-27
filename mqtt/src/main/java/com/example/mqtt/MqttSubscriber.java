package com.example.mqtt;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@Service
public class MqttSubscriber {

    private static final Logger logger = LoggerFactory.getLogger(MqttSubscriber.class);

    @Autowired
    private Mqtt3AsyncClient mqttClient;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @PostConstruct
    public void subscribeToTopic() {
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

    void handleMessage(String message) {
        // Parse JSON into a list of string-pair objects (key -> string value)
        List<Map.Entry<String, String>> pairs = parseMessageToStringPairs(message);

        // Example: log pairs (or forward them to other services)
        for (Map.Entry<String, String> p : pairs) {
            logger.info("field='{}' value='{}'", p.getKey(), p.getValue());
        }

        // Implement further message handling here
    }

    /**
     * Parse a JSON object string into a list of string pairs (field name -> stringified value).
     * Expected JSON shape:
     * {
     *   "vin":"string",
     *   "lo_degree":float,
     *   "lo_direction":"string",
     *   "la_degree":float,
     *   "la_direction":"string",
     *   "lastUpdateTimestamp":"date"
     * }
     */
    private List<Map.Entry<String, String>> parseMessageToStringPairs(String message) {
        try {
            JsonNode root = OBJECT_MAPPER.readTree(message);
            if (!root.isObject()) {
                logger.warn("Expected JSON object but got: {}", root.getNodeType());
                return Collections.emptyList();
            }

            List<Map.Entry<String, String>> list = new ArrayList<>();
            root.fieldNames().forEachRemaining(name -> {
                JsonNode valueNode = root.get(name);
                String value = valueNode == null || valueNode.isNull() ? null : valueNode.asText();
                list.add(new AbstractMap.SimpleEntry<>(name, value));
            });
            return list;
        } catch (Exception e) {
            logger.error("Failed to parse message as JSON: {}", e.getMessage());
            return Collections.emptyList();
        }
    }
}