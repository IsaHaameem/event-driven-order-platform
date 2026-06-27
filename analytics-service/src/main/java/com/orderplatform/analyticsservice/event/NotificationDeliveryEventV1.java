package com.orderplatform.analyticsservice.event;

import java.time.Instant;
import java.util.UUID;

public record NotificationDeliveryEventV1(
        UUID eventId,
        Instant occurredAt,
        UUID orderId,
        UUID customerId,
        String status,
        String channel
) {
}