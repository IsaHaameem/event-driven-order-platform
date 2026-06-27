package com.orderplatform.analyticsservice.api;

import com.orderplatform.analyticsservice.service.AnalyticsQueryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final AnalyticsQueryService queryService;

    public AnalyticsController(AnalyticsQueryService queryService) {
        this.queryService = queryService;
    }

    @GetMapping("/summary")
    public AnalyticsSummaryResponse getSummary() {
        return new AnalyticsSummaryResponse(
                queryService.totalOrders(),
                queryService.inventoryReservedEvents(),
                queryService.inventoryUnitsReserved(),
                queryService.notificationsSent(),
                queryService.emailSuccessRate(),
                queryService.failedEvents()
        );
    }

    @GetMapping("/orders-per-hour")
    public List<HourlyCount> getOrdersPerHour() {
        return queryService.ordersPerHour();
    }
}