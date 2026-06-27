package com.orderplatform.inventoryservice.api;

public record ProductStockResponse(
        String productId,
        int quantityAvailable
) {
}