package com.orderplatform.analyticsservice.service;

import com.orderplatform.analyticsservice.domain.AnalyticsEvent;
import com.orderplatform.analyticsservice.event.EmailDeliveryEventV1;
import com.orderplatform.analyticsservice.event.InventoryReservedEventV1;
import com.orderplatform.analyticsservice.event.NotificationDeliveryEventV1;
import com.orderplatform.analyticsservice.event.OrderCreatedEventV1;
import com.orderplatform.analyticsservice.repository.AnalyticsEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AnalyticsIngestService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsIngestService.class);

    private final AnalyticsEventRepository repository;

    public AnalyticsIngestService(AnalyticsEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public void recordOrderCreated(OrderCreatedEventV1 event) {
        if (repository.existsById(event.eventId())) {
            return;
        }
        AnalyticsEvent record = new AnalyticsEvent();
        record.setId(event.eventId());
        record.setEventType("ORDER_CREATED");
        record.setOrderId(event.orderId());
        record.setCustomerId(event.customerId());
        record.setOccurredAt(event.occurredAt());
        repository.save(record);
        log.info("Recorded ORDER_CREATED for order {}", event.orderId());
    }

    @Transactional
    public void recordInventoryReserved(InventoryReservedEventV1 event) {
        if (repository.existsById(event.eventId())) {
            return;
        }
        int totalQuantity = event.items().stream()
                .mapToInt(InventoryReservedEventV1.ReservedItem::quantity)
                .sum();

        AnalyticsEvent record = new AnalyticsEvent();
        record.setId(event.eventId());
        record.setEventType("INVENTORY_RESERVED");
        record.setOrderId(event.orderId());
        record.setQuantity(totalQuantity);
        record.setOccurredAt(event.occurredAt());
        repository.save(record);
        log.info("Recorded INVENTORY_RESERVED for order {} ({} units)", event.orderId(), totalQuantity);
    }

    @Transactional
    public void recordEmailDelivery(EmailDeliveryEventV1 event) {
        if (repository.existsById(event.eventId())) {
            return;
        }
        AnalyticsEvent record = new AnalyticsEvent();
        record.setId(event.eventId());
        record.setEventType("EMAIL_DELIVERY");
        record.setOrderId(event.orderId());
        record.setCustomerId(event.customerId());
        record.setStatus(event.status());
        record.setOccurredAt(event.occurredAt());
        repository.save(record);
        log.info("Recorded EMAIL_DELIVERY ({}) for order {}", event.status(), event.orderId());
    }

    @Transactional
    public void recordNotificationDelivery(NotificationDeliveryEventV1 event) {
        if (repository.existsById(event.eventId())) {
            return;
        }
        AnalyticsEvent record = new AnalyticsEvent();
        record.setId(event.eventId());
        record.setEventType("NOTIFICATION_DELIVERY");
        record.setOrderId(event.orderId());
        record.setCustomerId(event.customerId());
        record.setStatus(event.status());
        record.setOccurredAt(event.occurredAt());
        repository.save(record);
        log.info("Recorded NOTIFICATION_DELIVERY ({}) for order {}", event.status(), event.orderId());
    }
}