package com.orderplatform.orderservice.api;

import java.math.BigDecimal;

public record OrderItemResponse(
        String productId,
        int quantity,
        BigDecimal unitPrice
) {
}