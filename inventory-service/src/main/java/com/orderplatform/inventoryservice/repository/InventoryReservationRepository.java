package com.orderplatform.inventoryservice.repository;

import com.orderplatform.inventoryservice.domain.InventoryReservation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface InventoryReservationRepository extends JpaRepository<InventoryReservation, UUID> {
}