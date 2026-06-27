CREATE TABLE orders (
    id              UUID PRIMARY KEY,
    customer_id     UUID NOT NULL,
    status          VARCHAR(20) NOT NULL,
    total_amount    NUMERIC(12, 2) NOT NULL,
    currency        VARCHAR(3) NOT NULL DEFAULT 'INR',
    created_at      TIMESTAMP NOT NULL DEFAULT now(),
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_customer_id ON orders (customer_id);

CREATE TABLE order_items (
    id              UUID PRIMARY KEY,
    order_id        UUID NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    product_id      VARCHAR(64) NOT NULL,
    quantity        INTEGER NOT NULL CHECK (quantity > 0),
    unit_price      NUMERIC(12, 2) NOT NULL CHECK (unit_price > 0)
);

CREATE INDEX idx_order_items_order_id ON order_items (order_id);