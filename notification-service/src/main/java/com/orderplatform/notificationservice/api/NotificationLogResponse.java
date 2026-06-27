package com.orderplatform.notificationservice.api;

import com.orderplatform.notificationservice.domain.NotificationStatus;

import java.time.Instant;
import java.util.UUID;

public record NotificationLogResponse(
        UUID orderId,
        UUID customerId,
        String channel,
        String message,
        NotificationStatus status,
        Instant createdAt
) {
}