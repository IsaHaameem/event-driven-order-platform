package com.orderplatform.notificationservice.api;

import com.orderplatform.notificationservice.repository.NotificationLogRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/notifications")
public class NotificationController {

    private final NotificationLogRepository notificationLogRepository;

    public NotificationController(NotificationLogRepository notificationLogRepository) {
        this.notificationLogRepository = notificationLogRepository;
    }

    @GetMapping("/orders/{orderId}")
    public List<NotificationLogResponse> getNotificationsForOrder(@PathVariable UUID orderId) {
        return notificationLogRepository.findByOrderId(orderId).stream()
                .map(n -> new NotificationLogResponse(n.getOrderId(), n.getCustomerId(), n.getChannel(), n.getMessage(), n.getStatus(), n.getCreatedAt()))
                .collect(Collectors.toList());
    }
}