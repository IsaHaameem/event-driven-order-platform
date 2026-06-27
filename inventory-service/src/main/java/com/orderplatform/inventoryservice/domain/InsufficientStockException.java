package com.orderplatform.inventoryservice.domain;

public class InsufficientStockException extends RuntimeException {

    public InsufficientStockException(String productId, int requested, int available) {
        super("Insufficient stock for product " + productId + ": requested " + requested + ", available " + available);
    }
}