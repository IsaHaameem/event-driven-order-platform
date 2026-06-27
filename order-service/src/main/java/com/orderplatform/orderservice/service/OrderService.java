package com.orderplatform.orderservice.service;

import com.orderplatform.orderservice.api.CreateOrderRequest;
import com.orderplatform.orderservice.api.OrderItemRequest;
import com.orderplatform.orderservice.api.OrderItemResponse;
import com.orderplatform.orderservice.api.OrderResponse;
import com.orderplatform.orderservice.domain.Order;
import com.orderplatform.orderservice.domain.OrderItem;
import com.orderplatform.orderservice.domain.OrderStatus;
import com.orderplatform.orderservice.event.OrderCreatedEventV1;
import com.orderplatform.orderservice.event.OrderEventPublisher;
import com.orderplatform.orderservice.repository.OrderRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderEventPublisher eventPublisher;

    public OrderService(OrderRepository orderRepository, OrderEventPublisher eventPublisher) {
        this.orderRepository = orderRepository;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        Order order = new Order();
        order.setCustomerId(request.customerId());
        order.setStatus(OrderStatus.CREATED);
        order.setCurrency("INR");

        BigDecimal total = BigDecimal.ZERO;
        for (OrderItemRequest itemRequest : request.items()) {
            OrderItem item = new OrderItem();
            item.setProductId(itemRequest.productId());
            item.setQuantity(itemRequest.quantity());
            item.setUnitPrice(itemRequest.unitPrice());
            order.addItem(item);
            total = total.add(itemRequest.unitPrice().multiply(BigDecimal.valueOf(itemRequest.quantity())));
        }
        order.setTotalAmount(total);

        Order saved = orderRepository.save(order);

        // Known gap, flagged on purpose: publishing after save() inside the same
        // transaction is the "dual write" problem — if the publish fails after the
        // DB commit, the order exists but no event ever goes out. The fix is the
        // Outbox Pattern; we're not implementing it yet, just naming the debt.
        eventPublisher.publish(toEvent(saved));

        return toResponse(saved);
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new NoSuchElementException("Order not found: " + orderId));
        return toResponse(order);
    }

    private OrderCreatedEventV1 toEvent(Order order) {
        List<OrderCreatedEventV1.OrderItemPayload> items = order.getItems().stream()
                .map(i -> new OrderCreatedEventV1.OrderItemPayload(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
                .collect(Collectors.toList());

        return new OrderCreatedEventV1(
                UUID.randomUUID(),
                Instant.now(),
                order.getId(),
                order.getCustomerId(),
                order.getTotalAmount(),
                order.getCurrency(),
                items
        );
    }

    private OrderResponse toResponse(Order order) {
        List<OrderItemResponse> items = order.getItems().stream()
                .map(i -> new OrderItemResponse(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
                .collect(Collectors.toList());

        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getStatus(),
                order.getTotalAmount(),
                order.getCurrency(),
                order.getCreatedAt(),
                items
        );
    }
}