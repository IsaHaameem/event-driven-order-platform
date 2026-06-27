package com.orderplatform.inventoryservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@Entity
@Table(name = "products")
@Getter
@Setter
@NoArgsConstructor
public class Product {

    @Id
    @Column(name = "product_id", length = 64)
    private String productId;

    @Column(name = "quantity_available", nullable = false)
    private Integer quantityAvailable;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt = Instant.now();

    public void reserve(int quantity) {
        if (this.quantityAvailable < quantity) {
            throw new InsufficientStockException(this.productId, quantity, this.quantityAvailable);
        }
        this.quantityAvailable -= quantity;
        this.updatedAt = Instant.now();
    }
}