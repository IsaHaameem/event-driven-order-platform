package com.orderplatform.orderservice.api;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record OrderItemRequest(
        @NotBlank(message = "productId is required")
        String productId,

        @Positive(message = "quantity must be positive")
        int quantity,

        @Positive(message = "unitPrice must be positive")
        BigDecimal unitPrice
) {
}