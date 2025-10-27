package com.example.mqtt;

import com.hivemq.client.mqtt.mqtt3.Mqtt3AsyncClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

@Configuration
public class MqttConfig {

    @Bean
    public Mqtt3AsyncClient mqttAsyncClient() {
        String clientId = "spring-boot-client-" + UUID.randomUUID();
        
        Mqtt3AsyncClient client = Mqtt3Client.builder()
                .identifier(clientId)
                .serverHost(Constants.BROKER_HOST)
                .serverPort(Constants.BROKER_PORT)
                .automaticReconnectWithDefaultConfig()
                .buildAsync();
        
        // Conectare la broker
        client.connectWith()
                .cleanSession(true)
                .send()
                .whenComplete((connAck, throwable) -> {
                    if (throwable != null) {
                        System.err.println("Eroare la conectare: " + throwable.getMessage());
                    } else {
                        System.out.println("Conectat la HiveMQ broker cu ID: " + clientId);
                    }
                });
        
        return client;
    }
}