package com.orderplatform.emailservice.event;

import java.time.Instant;
import java.util.UUID;

public record EmailDeliveryEventV1(
        UUID eventId,
        Instant occurredAt,
        UUID orderId,
        UUID customerId,
        String status,      // "SENT" or "FAILED"
        String recipient
) {
}