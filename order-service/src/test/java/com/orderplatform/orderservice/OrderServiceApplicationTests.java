package com.orderplatform.orderservice;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

// Needs the real Postgres + Kafka network up (we haven't added Testcontainers yet),
// so this only passes inside `docker compose up --build`, not a bare `mvn test`.
@SpringBootTest
class OrderServiceApplicationTests {

    @Test
    void contextLoads() {
    }
}