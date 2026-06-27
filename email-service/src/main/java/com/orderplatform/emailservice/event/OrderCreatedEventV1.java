package com.orderplatform.emailservice.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

// Local copy of Order Service's contract — same reasoning as Inventory Service.
public record OrderCreatedEventV1(
        UUID eventId,
        Instant occurredAt,
        UUID orderId,
        UUID customerId,
        BigDecimal totalAmount,
        String currency,
        List<OrderItemPayload> items
) {

    public record OrderItemPayload(
            String productId,
            int quantity,
            BigDecimal unitPrice
    ) {
    }
}