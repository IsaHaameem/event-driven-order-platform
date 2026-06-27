package com.orderplatform.notificationservice.repository;

import com.orderplatform.notificationservice.domain.NotificationLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface NotificationLogRepository extends JpaRepository<NotificationLog, UUID> {
    List<NotificationLog> findByOrderId(UUID orderId);
}