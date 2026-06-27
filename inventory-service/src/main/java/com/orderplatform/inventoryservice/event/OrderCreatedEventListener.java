package com.orderplatform.inventoryservice.event;

import com.orderplatform.inventoryservice.service.InventoryReservationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedEventListener {

    private final InventoryReservationService reservationService;

    public OrderCreatedEventListener(InventoryReservationService reservationService) {
        this.reservationService = reservationService;
    }

    @KafkaListener(topics = "order.created.v1")
    public void onOrderCreated(OrderCreatedEventV1 event) {
        reservationService.handle(event);
    }
}