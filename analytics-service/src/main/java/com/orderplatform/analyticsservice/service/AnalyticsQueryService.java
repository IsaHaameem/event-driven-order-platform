package com.orderplatform.analyticsservice.service;

import com.orderplatform.analyticsservice.api.HourlyCount;
import com.orderplatform.analyticsservice.repository.AnalyticsEventRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class AnalyticsQueryService {

    private final AnalyticsEventRepository repository;

    public AnalyticsQueryService(AnalyticsEventRepository repository) {
        this.repository = repository;
    }

    public long totalOrders() {
        return repository.countByEventType("ORDER_CREATED");
    }

    public List<HourlyCount> ordersPerHour() {
        return repository.countByHourNative("ORDER_CREATED").stream()
                // occurred_at is stored as Instant (UTC) -> plain TIMESTAMP column, so the
                // LocalDateTime Hibernate hands back here is already a UTC wall-clock value.
                .map(row -> new HourlyCount(
                        ((LocalDateTime) row[0]).atZone(ZoneOffset.UTC).toInstant(),
                        ((Number) row[1]).longValue()))
                .collect(Collectors.toList());
    }

    public long inventoryReservedEvents() {
        return repository.countByEventType("INVENTORY_RESERVED");
    }

    public long inventoryUnitsReserved() {
        return repository.sumQuantityByEventType("INVENTORY_RESERVED");
    }

    public long notificationsSent() {
        return repository.countByEventTypeAndStatus("NOTIFICATION_DELIVERY", "SENT");
    }

    public double emailSuccessRate() {
        long sent = repository.countByEventTypeAndStatus("EMAIL_DELIVERY", "SENT");
        long failed = repository.countByEventTypeAndStatus("EMAIL_DELIVERY", "FAILED");
        long total = sent + failed;
        return total == 0 ? 0.0 : (double) sent / total;
    }

    public long failedEvents() {
        return repository.countByStatus("FAILED");
    }
}