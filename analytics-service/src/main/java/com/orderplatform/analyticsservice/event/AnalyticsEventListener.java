package com.orderplatform.analyticsservice.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderplatform.analyticsservice.service.AnalyticsIngestService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsEventListener {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsEventListener.class);

    private final ObjectMapper objectMapper;
    private final AnalyticsIngestService ingestService;

    public AnalyticsEventListener(ObjectMapper eventObjectMapper, AnalyticsIngestService ingestService) {
        this.objectMapper = eventObjectMapper;
        this.ingestService = ingestService;
    }

    @KafkaListener(topics = "order.created.v1")
    public void onOrderCreated(String payload) {
        try {
            ingestService.recordOrderCreated(objectMapper.readValue(payload, OrderCreatedEventV1.class));
        } catch (Exception e) {
            log.error("Failed to parse OrderCreatedEventV1 payload: {}", payload, e);
        }
    }

    @KafkaListener(topics = "inventory.reserved.v1")
    public void onInventoryReserved(String payload) {
        try {
            ingestService.recordInventoryReserved(objectMapper.readValue(payload, InventoryReservedEventV1.class));
        } catch (Exception e) {
            log.error("Failed to parse InventoryReservedEventV1 payload: {}", payload, e);
        }
    }

    @KafkaListener(topics = "email.delivery.v1")
    public void onEmailDelivery(String payload) {
        try {
            ingestService.recordEmailDelivery(objectMapper.readValue(payload, EmailDeliveryEventV1.class));
        } catch (Exception e) {
            log.error("Failed to parse EmailDeliveryEventV1 payload: {}", payload, e);
        }
    }

    @KafkaListener(topics = "notification.delivery.v1")
    public void onNotificationDelivery(String payload) {
        try {
            ingestService.recordNotificationDelivery(objectMapper.readValue(payload, NotificationDeliveryEventV1.class));
        } catch (Exception e) {
            log.error("Failed to parse NotificationDeliveryEventV1 payload: {}", payload, e);
        }
    }
}