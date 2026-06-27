package com.orderplatform.emailservice.api;

import com.orderplatform.emailservice.repository.EmailLogRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/email")
public class EmailController {

    private final EmailLogRepository emailLogRepository;

    public EmailController(EmailLogRepository emailLogRepository) {
        this.emailLogRepository = emailLogRepository;
    }

    @GetMapping("/orders/{orderId}")
    public List<EmailLogResponse> getEmailsForOrder(@PathVariable UUID orderId) {
        return emailLogRepository.findByOrderId(orderId).stream()
                .map(e -> new EmailLogResponse(e.getOrderId(), e.getRecipient(), e.getSubject(), e.getStatus(), e.getCreatedAt()))
                .collect(Collectors.toList());
    }
}