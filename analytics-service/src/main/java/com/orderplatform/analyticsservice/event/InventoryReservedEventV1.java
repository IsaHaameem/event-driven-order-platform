package com.orderplatform.analyticsservice.event;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record InventoryReservedEventV1(
        UUID eventId,
        Instant occurredAt,
        UUID orderId,
        List<ReservedItem> items
) {

    public record ReservedItem(
            String productId,
            int quantity
    ) {
    }
}