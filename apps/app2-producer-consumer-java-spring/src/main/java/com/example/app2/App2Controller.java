package com.example.app2;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class App2Controller {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/producer")
    public String Producer(){
        kafkaTemplate.send("topicA", "{message from app2}");
        return "published";
    }

    @KafkaListener(topics = "A", groupId = "group-id-1")
    public void ConsumingA(String message) {
        System.out.println("Received Message A : " + message);
    }
    @KafkaListener(topics = "B", groupId = "group-id-1")
    public void ConsumingB(String message) {
        System.out.println("Received Message B : " + message);
    }
}

