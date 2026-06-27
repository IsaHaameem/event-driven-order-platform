package com.orderplatform.orderservice.api;

import com.orderplatform.orderservice.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderResponse(
        UUID id,
        UUID customerId,
        OrderStatus status,
        BigDecimal totalAmount,
        String currency,
        Instant createdAt,
        List<OrderItemResponse> items
) {
}