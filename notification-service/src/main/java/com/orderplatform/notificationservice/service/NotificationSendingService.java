package com.orderplatform.notificationservice.service;

import com.orderplatform.notificationservice.domain.NotificationLog;
import com.orderplatform.notificationservice.domain.NotificationStatus;
import com.orderplatform.notificationservice.domain.ProcessedEvent;
import com.orderplatform.notificationservice.event.NotificationDeliveryEventV1;
import com.orderplatform.notificationservice.event.OrderCreatedEventV1;
import com.orderplatform.notificationservice.repository.NotificationLogRepository;
import com.orderplatform.notificationservice.repository.ProcessedEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.security.SecureRandom;
import java.time.Instant;
import java.util.UUID;

@Service
public class NotificationSendingService {

    private static final Logger log = LoggerFactory.getLogger(NotificationSendingService.class);
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final String NOTIFICATION_DELIVERY_TOPIC = "notification.delivery.v1";

    private final NotificationLogRepository notificationLogRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final double simulatedFailureRate;
    private final String alwaysFailForCustomerId;

    public NotificationSendingService(NotificationLogRepository notificationLogRepository,
                                       ProcessedEventRepository processedEventRepository,
                                       KafkaTemplate<String, Object> kafkaTemplate,
                                       @Value("${app.notification.simulated-failure-rate:0.2}") double simulatedFailureRate,
                                       @Value("${app.notification.always-fail-for-customer-id:}") String alwaysFailForCustomerId) {
        this.notificationLogRepository = notificationLogRepository;
        this.processedEventRepository = processedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.simulatedFailureRate = simulatedFailureRate;
        this.alwaysFailForCustomerId = alwaysFailForCustomerId;
    }

    @Transactional
    public void send(OrderCreatedEventV1 event) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("Skipping duplicate delivery of event {} (order {})", event.eventId(), event.orderId());
            return;
        }

        String message = "Your order " + event.orderId() + " has been placed.";

        if (shouldSimulateFailure(event)) {
            log.warn("Simulated push delivery failure for order {} (customer {})", event.orderId(), event.customerId());
            throw new TransientNotificationException("Simulated push provider timeout for customer " + event.customerId());
        }

        NotificationLog notificationLog = new NotificationLog();
        notificationLog.setOrderId(event.orderId());
        notificationLog.setCustomerId(event.customerId());
        notificationLog.setChannel("PUSH");
        notificationLog.setMessage(message);
        notificationLog.setStatus(NotificationStatus.SENT);
        notificationLogRepository.save(notificationLog);

        processedEventRepository.save(new ProcessedEvent(event.eventId(), "OrderCreatedEventV1"));

        publishDeliveryEvent(event, "SENT");

        log.info("Sent push notification for order {} to customer {}", event.orderId(), event.customerId());
    }

    @Transactional
    public void recordPermanentFailure(OrderCreatedEventV1 event, String originTopic) {
        NotificationLog notificationLog = new NotificationLog();
        notificationLog.setOrderId(event.orderId());
        notificationLog.setCustomerId(event.customerId());
        notificationLog.setChannel("PUSH");
        notificationLog.setMessage("Your order " + event.orderId() + " has been placed.");
        notificationLog.setStatus(NotificationStatus.FAILED);
        notificationLogRepository.save(notificationLog);

        publishDeliveryEvent(event, "FAILED");

        log.error("Order {} exhausted all retries (from topic {}) — recorded as FAILED for manual follow-up",
                event.orderId(), originTopic);
    }

    private void publishDeliveryEvent(OrderCreatedEventV1 event, String status) {
        NotificationDeliveryEventV1 deliveryEvent = new NotificationDeliveryEventV1(
                UUID.randomUUID(), Instant.now(), event.orderId(), event.customerId(), status, "PUSH"
        );
        kafkaTemplate.send(NOTIFICATION_DELIVERY_TOPIC, event.orderId().toString(), deliveryEvent);
    }

    private boolean shouldSimulateFailure(OrderCreatedEventV1 event) {
        if (!alwaysFailForCustomerId.isBlank() && alwaysFailForCustomerId.equals(event.customerId().toString())) {
            return true;
        }
        return RANDOM.nextDouble() < simulatedFailureRate;
    }
}