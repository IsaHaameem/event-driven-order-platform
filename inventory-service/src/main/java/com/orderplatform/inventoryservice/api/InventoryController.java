package com.orderplatform.inventoryservice.api;

import com.orderplatform.inventoryservice.domain.Product;
import com.orderplatform.inventoryservice.repository.ProductRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.NoSuchElementException;

@RestController
@RequestMapping("/api/inventory")
public class InventoryController {

    private final ProductRepository productRepository;

    public InventoryController(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @GetMapping("/{productId}")
    public ResponseEntity<ProductStockResponse> getStock(@PathVariable String productId) {
        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new NoSuchElementException("Unknown product: " + productId));
        return ResponseEntity.ok(new ProductStockResponse(product.getProductId(), product.getQuantityAvailable()));
    }
}