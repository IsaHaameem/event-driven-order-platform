package com.orderplatform.analyticsservice.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "analytics_events")
@Getter
@Setter
@NoArgsConstructor
public class AnalyticsEvent {

    // The PK IS the originating event's own eventId — doubles as our idempotency
    // guard (existsById before insert), no separate processed_events table needed.
    @Id
    private UUID id;

    @Column(name = "event_type", nullable = false, length = 50)
    private String eventType;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "customer_id")
    private UUID customerId;

    @Column(name = "status", length = 20)
    private String status;

    @Column(name = "quantity")
    private Integer quantity;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt = Instant.now();
}