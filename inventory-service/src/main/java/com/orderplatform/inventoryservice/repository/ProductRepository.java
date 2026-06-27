package com.orderplatform.inventoryservice.repository;

import com.orderplatform.inventoryservice.domain.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, String> {
}