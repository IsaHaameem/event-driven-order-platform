package com.orderplatform.orderservice.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    public static final String TOPIC = "order.created.v1";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publish(OrderCreatedEventV1 event) {
        // Keyed by customerId: every event for this customer lands on the same
        // partition, so downstream consumers see one customer's events in order.
        String key = event.customerId().toString();

        CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to publish OrderCreatedEventV1 for order {}: {}",
                        event.orderId(), ex.getMessage(), ex);
            } else {
                log.info("Published OrderCreatedEventV1 for order {} -> partition {} offset {}",
                        event.orderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }
}