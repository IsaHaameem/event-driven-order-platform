package com.orderplatform.inventoryservice.service;

import com.orderplatform.inventoryservice.domain.InventoryReservation;
import com.orderplatform.inventoryservice.domain.Product;
import com.orderplatform.inventoryservice.domain.ProcessedEvent;
import com.orderplatform.inventoryservice.event.InventoryReservedEventV1;
import com.orderplatform.inventoryservice.event.OrderCreatedEventV1;
import com.orderplatform.inventoryservice.repository.InventoryReservationRepository;
import com.orderplatform.inventoryservice.repository.ProcessedEventRepository;
import com.orderplatform.inventoryservice.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class InventoryReservationService {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservationService.class);
    private static final String INVENTORY_RESERVED_TOPIC = "inventory.reserved.v1";

    private final ProductRepository productRepository;
    private final InventoryReservationRepository reservationRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryReservationService(ProductRepository productRepository,
                                        InventoryReservationRepository reservationRepository,
                                        ProcessedEventRepository processedEventRepository,
                                        KafkaTemplate<String, Object> kafkaTemplate) {
        this.productRepository = productRepository;
        this.reservationRepository = reservationRepository;
        this.processedEventRepository = processedEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Transactional
    public void handle(OrderCreatedEventV1 event) {
        // Idempotency gate: if this eventId is already recorded, this is a Kafka
        // redelivery of an order we already reserved stock for — stop here.
        // (Check-then-act has a theoretical race under concurrent redelivery within
        // the same poll; the fully bulletproof version relies on the processed_events
        // PK violation itself as the mutex. Not needed at single-partition-consumer
        // scale here, but worth knowing the gap exists.)
        if (processedEventRepository.existsById(event.eventId())) {
            log.info("Skipping duplicate delivery of event {} (order {})", event.eventId(), event.orderId());
            return;
        }

        for (OrderCreatedEventV1.OrderItemPayload item : event.items()) {
            Product product = productRepository.findById(item.productId())
                    .orElseThrow(() -> new NoSuchElementException("Unknown product: " + item.productId()));

            // Insufficient stock is a DETERMINISTIC failure — retrying the same
            // message won't fix it. Spring Kafka's default error handler will retry
            // a few times then log and move past it. The real fix is a Dead Letter
            // Topic plus a compensating event (Saga) — not built yet, flagged here.
            product.reserve(item.quantity());
            productRepository.save(product);

            InventoryReservation reservation = new InventoryReservation();
            reservation.setOrderId(event.orderId());
            reservation.setProductId(item.productId());
            reservation.setQuantity(item.quantity());
            reservationRepository.save(reservation);
        }

        processedEventRepository.save(new ProcessedEvent(event.eventId(), "OrderCreatedEventV1"));

        publishReservedEvent(event);
    }

    private void publishReservedEvent(OrderCreatedEventV1 event) {
        List<InventoryReservedEventV1.ReservedItem> items = event.items().stream()
                .map(i -> new InventoryReservedEventV1.ReservedItem(i.productId(), i.quantity()))
                .collect(Collectors.toList());

        InventoryReservedEventV1 reserved = new InventoryReservedEventV1(
                UUID.randomUUID(),
                Instant.now(),
                event.orderId(),
                items
        );

        kafkaTemplate.send(INVENTORY_RESERVED_TOPIC, event.orderId().toString(), reserved);
    }
}