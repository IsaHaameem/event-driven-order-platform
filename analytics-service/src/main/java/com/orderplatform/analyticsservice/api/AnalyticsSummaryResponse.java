package com.orderplatform.analyticsservice.api;

public record AnalyticsSummaryResponse(
        long totalOrders,
        long inventoryReservedEvents,
        long inventoryUnitsReserved,
        long notificationsSent,
        double emailSuccessRate,
        long failedEvents
) {
}