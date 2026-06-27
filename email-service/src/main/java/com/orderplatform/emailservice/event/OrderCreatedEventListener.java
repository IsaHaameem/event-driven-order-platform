package com.orderplatform.emailservice.event;

import com.orderplatform.emailservice.service.EmailSendingService;
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

    private final EmailSendingService emailSendingService;

    public OrderCreatedEventListener(EmailSendingService emailSendingService) {
        this.emailSendingService = emailSendingService;
    }

    @RetryableTopic(
            attempts = "4",
            backOff = @BackOff(delay = 1000, multiplier = 2, maxDelay = 10000),
            retryTopicSuffix = "-email-retry",
            dltTopicSuffix = "-email-dlt",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
    )
    @KafkaListener(topics = "order.created.v1")
    public void onOrderCreated(OrderCreatedEventV1 event) {
        emailSendingService.send(event);
    }

    @DltHandler
    public void onOrderCreatedDlt(OrderCreatedEventV1 event, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        emailSendingService.recordPermanentFailure(event, topic);
    }
}