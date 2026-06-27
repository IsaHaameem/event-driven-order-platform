package com.orderplatform.analyticsservice.api;

import java.time.Instant;

public record HourlyCount(
        Instant hour,
        long count
) {
}