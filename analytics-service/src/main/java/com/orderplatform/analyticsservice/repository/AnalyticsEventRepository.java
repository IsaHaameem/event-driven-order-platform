package com.orderplatform.analyticsservice.repository;

import com.orderplatform.analyticsservice.domain.AnalyticsEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;
import java.util.UUID;

public interface AnalyticsEventRepository extends JpaRepository<AnalyticsEvent, UUID> {

    long countByEventType(String eventType);

    long countByEventTypeAndStatus(String eventType, String status);

    long countByStatus(String status);

    @Query(value = "SELECT date_trunc('hour', occurred_at) AS hour_bucket, COUNT(*) AS order_count " +
                   "FROM analytics_events WHERE event_type = :eventType " +
                   "GROUP BY hour_bucket ORDER BY hour_bucket",
           nativeQuery = true)
    List<Object[]> countByHourNative(@Param("eventType") String eventType);

    @Query("SELECT COALESCE(SUM(e.quantity), 0) FROM AnalyticsEvent e WHERE e.eventType = :eventType")
    long sumQuantityByEventType(@Param("eventType") String eventType);
}