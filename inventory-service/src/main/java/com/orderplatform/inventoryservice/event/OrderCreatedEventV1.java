package com.orderplatform.inventoryservice.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

// Deliberately a LOCAL copy of Order Service's contract, not a shared dependency.
// Same shape, different package — see this step's concept note on why.
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