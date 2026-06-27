CREATE TABLE notification_log (
    id          UUID PRIMARY KEY,
    order_id    UUID NOT NULL,
    customer_id UUID NOT NULL,
    channel     VARCHAR(20) NOT NULL,
    message     VARCHAR(500) NOT NULL,
    status      VARCHAR(20) NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX idx_notification_log_order_id ON notification_log (order_id);

CREATE TABLE processed_events (
    event_id     UUID PRIMARY KEY,
    event_type   VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT now()
);