package com.orderplatform.orderservice.config;

import com.orderplatform.orderservice.event.OrderEventPublisher;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderCreatedTopic() {
        // 3 partitions even on a single-broker dev cluster: lets us prove different
        // customerIds can land on different partitions, while the SAME customerId
        // always lands on the same one.
        return TopicBuilder.name(OrderEventPublisher.TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }
}