package com.orderplatform.emailservice.api;

import com.orderplatform.emailservice.domain.EmailStatus;

import java.time.Instant;
import java.util.UUID;

public record EmailLogResponse(
        UUID orderId,
        String recipient,
        String subject,
        EmailStatus status,
        Instant createdAt
) {
}