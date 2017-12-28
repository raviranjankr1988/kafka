package com.ravkumar.kafka.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

public class SpringKafkaConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(SpringKafkaConsumer.class);

    private CountDownLatch latch = new CountDownLatch(1);

    public CountDownLatch getLatch() {
        return latch;
    }

    @KafkaListener(topics = "${kafka.topic.event_topic}")
    public void receive(String payload) {
        System.out.println("received payload= " + payload);
        LOGGER.info("received payload='{}'", payload);
        latch.countDown();
    }
}