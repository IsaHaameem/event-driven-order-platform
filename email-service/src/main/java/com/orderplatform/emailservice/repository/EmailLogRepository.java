package com.orderplatform.emailservice.repository;

import com.orderplatform.emailservice.domain.EmailLog;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface EmailLogRepository extends JpaRepository<EmailLog, UUID> {
    List<EmailLog> findByOrderId(UUID orderId);
}