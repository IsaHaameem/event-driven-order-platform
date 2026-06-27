package com.orderplatform.notificationservice.event;

import com.orderplatform.notificationservice.service.NotificationSendingService;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedEventListener {

    private final NotificationSendingService notificationSendingService;

    public OrderCreatedEventListener(NotificationSendingService notificationSendingService) {
        this.notificationSendingService = notificationSendingService;
    }

    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2, maxDelay = 10000),
            retryTopicSuffix = "-notification-retry",
            dltTopicSuffix = "-notification-dlt",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
    )
    @KafkaListener(topics = "order.created.v1")
    public void onOrderCreated(OrderCreatedEventV1 event) {
        notificationSendingService.send(event);
    }

    @DltHandler
    public void onOrderCreatedDlt(OrderCreatedEventV1 event, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        notificationSendingService.recordPermanentFailure(event, topic);
    }
}