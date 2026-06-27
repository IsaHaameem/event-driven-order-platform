CREATE TABLE products (
    product_id          VARCHAR(64) PRIMARY KEY,
    quantity_available  INTEGER NOT NULL CHECK (quantity_available >= 0),
    updated_at           TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE inventory_reservations (
    id          UUID PRIMARY KEY,
    order_id    UUID NOT NULL,
    product_id  VARCHAR(64) NOT NULL REFERENCES products (product_id),
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    reserved_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_inventory_reservations_order_id ON inventory_reservations (order_id);

CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    event_type   VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Seed stock for the SKUs already used in Order Service manual testing
INSERT INTO products (product_id, quantity_available) VALUES
    ('SKU-001', 500),
    ('SKU-002', 500);