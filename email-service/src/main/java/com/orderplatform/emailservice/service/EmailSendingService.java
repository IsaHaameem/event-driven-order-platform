package com.orderplatform.emailservice.service;

import com.orderplatform.emailservice.domain.EmailLog;
import com.orderplatform.emailservice.domain.EmailStatus;
import com.orderplatform.emailservice.domain.ProcessedEvent;
import com.orderplatform.emailservice.event.EmailDeliveryEventV1;
import com.orderplatform.emailservice.event.OrderCreatedEventV1;
import com.orderplatform.emailservice.repository.EmailLogRepository;
import com.orderplatform.emailservice.repository.ProcessedEventRepository;
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
public class EmailSendingService {

    private static final Logger log = LoggerFactory.getLogger(EmailSendingService.class);
    private static final SecureRandom RANDOM = new SecureRandom();
    private static final String EMAIL_DELIVERY_TOPIC = "email.delivery.v1";

    private final EmailLogRepository emailLogRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final double simulatedFailureRate;
    private final String alwaysFailForCustomerId;

    public EmailSendingService(EmailLogRepository emailLogRepository,
                                ProcessedEventRepository processedEventRepository,
                                KafkaTemplate<String, Object> kafkaTemplate,
                                @Value("${app.email.simulated-failure-rate:0.2}") double simulatedFailureRate,
                                @Value("${app.email.always-fail-for-customer-id:}") String alwaysFailForCustomerId) {
        this.emailLogRepository = emailLogRepository;
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

        String recipient = "customer-" + event.customerId() + "@example.com";
        String subject = "Order confirmation: " + event.orderId();

        if (shouldSimulateFailure(event)) {
            log.warn("Simulated send failure for order {} to {}", event.orderId(), recipient);
            throw new TransientEmailException("Simulated SMTP failure sending to " + recipient);
        }

        EmailLog emailLog = new EmailLog();
        emailLog.setOrderId(event.orderId());
        emailLog.setRecipient(recipient);
        emailLog.setSubject(subject);
        emailLog.setStatus(EmailStatus.SENT);
        emailLogRepository.save(emailLog);

        processedEventRepository.save(new ProcessedEvent(event.eventId(), "OrderCreatedEventV1"));

        publishDeliveryEvent(event, "SENT", recipient);

        log.info("Sent confirmation email for order {} to {}", event.orderId(), recipient);
    }

    @Transactional
    public void recordPermanentFailure(OrderCreatedEventV1 event, String originTopic) {
        String recipient = "customer-" + event.customerId() + "@example.com";

        EmailLog emailLog = new EmailLog();
        emailLog.setOrderId(event.orderId());
        emailLog.setRecipient(recipient);
        emailLog.setSubject("Order confirmation: " + event.orderId());
        emailLog.setStatus(EmailStatus.FAILED);
        emailLogRepository.save(emailLog);

        publishDeliveryEvent(event, "FAILED", recipient);

        log.error("Order {} exhausted all retries (from topic {}) — recorded as FAILED for manual follow-up",
                event.orderId(), originTopic);
    }

    private void publishDeliveryEvent(OrderCreatedEventV1 event, String status, String recipient) {
        EmailDeliveryEventV1 deliveryEvent = new EmailDeliveryEventV1(
                UUID.randomUUID(), Instant.now(), event.orderId(), event.customerId(), status, recipient
        );
        kafkaTemplate.send(EMAIL_DELIVERY_TOPIC, event.orderId().toString(), deliveryEvent);
    }

    private boolean shouldSimulateFailure(OrderCreatedEventV1 event) {
        if (!alwaysFailForCustomerId.isBlank() && alwaysFailForCustomerId.equals(event.customerId().toString())) {
            return true;
        }
        return RANDOM.nextDouble() < simulatedFailureRate;
    }
}